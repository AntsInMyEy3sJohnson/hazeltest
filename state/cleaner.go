package state

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/logging"
	"hazeltest/status"
	"strings"
	"time"
)

type (
	hzClientHandler interface {
		getClient() *hazelcast.Client
		hazelcastwrapper.HzClientInitializer
		hazelcastwrapper.HzClientCloser
	}
	defaultHzClientHandler struct {
		hzClient *hazelcast.Client
	}
)

type (
	cleanerBuilder interface {
		build(ch hzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (cleaner, string, error)
	}
	cleaner interface {
		clean(ctx context.Context) (int, error)
	}
	lastCleanedInfoHandler interface {
		check(syncMapName, payloadDataStructureName, hzService string) (bool, error)
		update(syncMapName, payloadDataStructureName, hzService string) error
	}
	cleanedTracker interface {
		addCleanedDataStructure(name string, cleaned int)
	}
	cleanerConfig struct {
		enabled   bool
		usePrefix bool
		prefix    string
	}
	cleanerConfigBuilder struct {
		keyPath string
		a       client.ConfigPropertyAssigner
	}
	mapCleanerBuilder struct {
		cfb cleanerConfigBuilder
	}
	mapCleaner struct {
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		c         *cleanerConfig
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hzClientHandler
		cih       lastCleanedInfoHandler
		t         cleanedTracker
	}
	queueCleanerBuilder struct {
		cfb cleanerConfigBuilder
	}
	queueCleaner struct {
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		c         *cleanerConfig
		qs        hazelcastwrapper.QueueStore
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hzClientHandler
		cih       lastCleanedInfoHandler
		t         cleanedTracker
	}
	defaultLastCleanedInfoHandler struct {
		ms  hazelcastwrapper.MapStore
		ctx context.Context
	}
	cleanedDataStructureTracker struct {
		g *status.Gatherer
	}
)

const (
	mapCleanerBasePath            = "stateCleaner.maps"
	queueCleanerBasePath          = "stateCleaner.queues"
	hzInternalDataStructurePrefix = "__"
	hzMapService                  = "hz:impl:mapService"
	hzQueueService                = "hz:impl:queueService"
	mapCleanersSyncMapName        = hzInternalDataStructurePrefix + "ht.mapCleaners"
	queueCleanersSyncMapName      = hzInternalDataStructurePrefix + "ht.queueCleaners"
	cleanAgainThresholdMs         = 600_000
)

var (
	builders []cleanerBuilder
	lp       *logging.LogProvider
)

func init() {
	register(newMapCleanerBuilder())
	register(newQueueCleanerBuilder())
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func newMapCleanerBuilder() *mapCleanerBuilder {

	return &mapCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: mapCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func newQueueCleanerBuilder() *queueCleanerBuilder {

	return &queueCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: queueCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func register(cb cleanerBuilder) {
	builders = append(builders, cb)
}

func (ch *defaultHzClientHandler) InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) {
	ch.hzClient = hazelcastwrapper.NewHzClientHelper().AssembleHazelcastClient(ctx, clientName, hzCluster, hzMembers)
}

func (ch *defaultHzClientHandler) Shutdown(ctx context.Context) error {
	return ch.hzClient.Shutdown(ctx)
}

func (ch *defaultHzClientHandler) getClient() *hazelcast.Client {
	return ch.hzClient
}

func (t *cleanedDataStructureTracker) addCleanedDataStructure(name string, cleaned int) {

	t.g.Updates <- status.Update{Key: name, Value: cleaned}

}

func (b *mapCleanerBuilder) build(ch hzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (cleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), hzMapService, log.ErrorLevel)
		return nil, hzMapService, err
	}

	clientName := "mapCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.getClient()}
	cih := &defaultLastCleanedInfoHandler{
		ms:  ms,
		ctx: ctx,
	}

	t := &cleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.g.AssembleStatusCopy)

	return &mapCleaner{
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.getClient()},
		ch:        ch,
		cih:       cih,
		t:         t,
	}, hzMapService, nil

}

func identifyCandidateDataStructures(ois hazelcastwrapper.ObjectInfoStore, ctx context.Context, hzService string) ([]hazelcastwrapper.ObjectInfo, error) {

	infos, err := ois.GetDistributedObjectsInfo(ctx)

	var result []hazelcastwrapper.ObjectInfo
	if err != nil {
		return result, err
	}

	for _, v := range infos {
		if !strings.HasPrefix(v.GetName(), hzInternalDataStructurePrefix) && v.GetServiceName() == hzService {
			result = append(result, v)
		}
	}

	return result, nil

}

func (cih *defaultLastCleanedInfoHandler) check(syncMapName, payloadDataStructureName, hzService string) (bool, error) {

	syncMap, err := cih.ms.GetMap(cih.ctx, syncMapName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to retrieve sync map '%s': %v", syncMapName, err), hzService, log.ErrorLevel)
		return false, err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved sync map '%s'", syncMapName), hzService, log.DebugLevel)
	lockSucceeded, err := syncMap.TryLock(cih.ctx, payloadDataStructureName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to acquire lock on sync map '%s' for payload data structure '%s': %v", syncMapName, payloadDataStructureName, err), hzService, log.ErrorLevel)
		return false, err
	}

	if !lockSucceeded {
		return false, fmt.Errorf("unable to acquire lock on '%s' for map %s", mapCleanersSyncMapName, payloadDataStructureName)
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully acquired lock on sync map '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.DebugLevel)
	v, err := syncMap.Get(cih.ctx, payloadDataStructureName)
	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon retrieving last updated info from sync map for '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.ErrorLevel)
		return false, err
	}

	// Value will be nil if key (name of payload map) was not present in sync map
	if v == nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that payload data structure '%s' was never cleaned before", payloadDataStructureName), hzService, log.DebugLevel)
		return true, nil
	}

	var lastCleanedAt int64
	if lc, ok := v.(int64); !ok {
		msg := fmt.Sprintf("unable to treat retrieved value '%v' for payload data structure '%s' as int64 timestamp", v, payloadDataStructureName)
		lp.LogStateCleanerEvent(msg, hzService, log.ErrorLevel)
		return false, errors.New(msg)
	} else {
		lastCleanedAt = lc
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved last updated info from sync map '%s' for payload data structure '%s'; last updated at %d", syncMapName, payloadDataStructureName, lastCleanedAt), hzService, log.DebugLevel)
	if time.Since(time.Unix(lastCleanedAt, 0)) < time.Millisecond*cleanAgainThresholdMs {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is less than configured threshold of '%d' milliseconds for payload data structure '%s'-- negative cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.DebugLevel)
		return false, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is greater than or equal to configured threshold of '%d' milliseconds for payload data structure '%s'-- positive cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.DebugLevel)
	return true, nil

}

func (cih *defaultLastCleanedInfoHandler) update(syncMapName, payloadMapName, hzService string) error {

	mapHoldingLock, err := cih.ms.GetMap(cih.ctx, syncMapName)
	defer func() {
		if mapHoldingLock == nil {
			return
		}
		if err := mapHoldingLock.Unlock(cih.ctx, payloadMapName); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to release lock on '%s' for key '%s' due to error: %v", mapCleanersSyncMapName, payloadMapName, err), hzService, log.ErrorLevel)
		}
	}()

	if err != nil {
		return err
	}

	cleanedAt := time.Now().UnixNano()
	ttlAndMaxIdle := time.Millisecond * cleanAgainThresholdMs
	return mapHoldingLock.SetWithTTLAndMaxIdle(cih.ctx, payloadMapName, cleanedAt, ttlAndMaxIdle, ttlAndMaxIdle)

}

func (c *mapCleaner) retrieveAndClean(ctx context.Context, name string) (int, error) {
	m, err := c.ms.GetMap(ctx, name)
	if err != nil {
		return 0, err
	}
	size, err := m.Size(ctx)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}
	if err := m.EvictAll(ctx); err != nil {
		return 0, err
	}
	c.t.addCleanedDataStructure(name, size)
	return size, nil
}

func (c *mapCleaner) clean(ctx context.Context) (int, error) {

	defer func() {
		_ = c.ch.Shutdown(ctx)
	}()

	if !c.c.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("map cleaner '%s' not enabled; won't run", c.name), hzMapService, log.InfoLevel)
		return 0, nil
	}

	numCleaned, err := runGenericClean(
		c.ois,
		ctx,
		c.cih,
		hzMapService,
		mapCleanersSyncMapName,
		c.c,
		c.retrieveAndClean,
	)

	return numCleaned, err

}

func (b *queueCleanerBuilder) build(ch hzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (cleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), hzQueueService, log.ErrorLevel)
		return nil, hzQueueService, err
	}

	clientName := "queueCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.getClient()}
	cih := &defaultLastCleanedInfoHandler{
		ms:  ms,
		ctx: ctx,
	}

	t := &cleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.g.AssembleStatusCopy)

	return &queueCleaner{
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		qs:        &hazelcastwrapper.DefaultQueueStore{Client: ch.getClient()},
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.getClient()},
		ch:        ch,
		cih:       cih,
		t:         t,
	}, hzQueueService, nil

}

func runGenericClean(
	ois hazelcastwrapper.ObjectInfoStore,
	ctx context.Context,
	cih lastCleanedInfoHandler,
	hzService, syncMapName string,
	c *cleanerConfig,
	retrieveAndClean func(ctx context.Context, name string) (int, error),
) (int, error) {

	candidateDataStructures, err := identifyCandidateDataStructures(ois, ctx, hzService)

	if err != nil {
		return 0, err
	}

	if len(candidateDataStructures) > 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("identified %d data structure candidate/-s to be considered for state cleaning", len(candidateDataStructures)), hzService, log.TraceLevel)
	} else {
		lp.LogStateCleanerEvent("no data structure candidates for state cleaning identified in target hazelcast cluster", hzService, log.TraceLevel)
		return 0, nil
	}

	var filteredDataStructures []hazelcastwrapper.ObjectInfo
	if c.usePrefix {
		lp.LogStateCleanerEvent(fmt.Sprintf("applying prefix '%s' to %d data structure candidate/-s identified for cleaning", c.prefix, len(candidateDataStructures)), hzService, log.TraceLevel)
		for _, v := range candidateDataStructures {
			if strings.HasPrefix(v.GetName(), c.prefix) {
				filteredDataStructures = append(filteredDataStructures, v)
			}
		}
	} else {
		filteredDataStructures = candidateDataStructures
	}

	numCleanedDataStructures := 0
	for _, v := range filteredDataStructures {
		shouldClean, err := cih.check(syncMapName, v.GetName(), hzService)
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to determine whether '%s' should be cleaned due to error: %v", v.GetName(), err), hzService, log.ErrorLevel)
			continue
		}
		if !shouldClean {
			lp.LogStateCleanerEvent(fmt.Sprintf("clean not required for '%s'", v.GetName()), hzService, log.InfoLevel)
			continue
		}
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that '%s' should be cleaned of state, commencing...", v.GetName()), hzService, log.InfoLevel)
		if numCleanedElements, err := retrieveAndClean(ctx, v.GetName()); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean data structure '%s' due to error upon retrieval of proxy object from Hazelcast cluster: %v", v, err), hzService, log.ErrorLevel)
			return numCleanedDataStructures, err
		} else if numCleanedElements > 0 {
			lp.LogStateCleanerEvent(fmt.Sprintf("data structure '%s' successfully cleaned", v.GetName()), hzService, log.InfoLevel)
		} else {
			lp.LogStateCleanerEvent(fmt.Sprintf("no state to clean in data structure '%s'", v.GetName()), hzService, log.InfoLevel)
		}
		numCleanedDataStructures++
		if err := cih.update(syncMapName, v.GetName(), hzService); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to update last cleaned info for '%s' due to error: %v", v.GetName(), err), hzService, log.ErrorLevel)
		} else {
			lp.LogStateCleanerEvent(fmt.Sprintf("successfully updated last cleaned info for '%s'", v.GetName()), hzService, log.InfoLevel)
		}
	}

	return numCleanedDataStructures, nil

}

func (c *queueCleaner) retrieveAndClean(ctx context.Context, name string) (int, error) {

	q, err := c.qs.GetQueue(ctx, name)
	if err != nil {
		return 0, err
	}
	size, err := q.Size(ctx)
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}
	if err := q.Clear(ctx); err != nil {
		return 0, err
	}
	c.t.addCleanedDataStructure(name, size)
	return size, nil

}

func (c *queueCleaner) clean(ctx context.Context) (int, error) {

	defer func() {
		_ = c.ch.Shutdown(ctx)
	}()

	if !c.c.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("queue cleaner '%s' not enabled; won't run", c.name), hzQueueService, log.InfoLevel)
		return 0, nil
	}

	numCleaned, err := runGenericClean(
		c.ois,
		ctx,
		c.cih,
		hzQueueService,
		queueCleanersSyncMapName,
		c.c,
		c.retrieveAndClean,
	)

	return numCleaned, err

}

func RunCleaners(hzCluster string, hzMembers []string) error {

	for _, b := range builders {

		g := status.NewGatherer()
		go g.Listen()

		ctx, cancel := context.WithCancel(context.Background())
		err := func() error {
			defer func() {
				cancel()
				g.StopListen()
			}()

			c, hzService, err := b.build(&defaultHzClientHandler{}, ctx, g, hzCluster, hzMembers)
			if err != nil {
				lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder for hazelcast due to error: %v", err), hzService, log.ErrorLevel)
				return err
			}

			if numCleanedDataStructures, err := c.clean(ctx); err != nil {
				if numCleanedDataStructures > 0 {
					lp.LogStateCleanerEvent(fmt.Sprintf("%d data structure/-s were cleaned before encountering error: %v", numCleanedDataStructures, err), hzService, log.ErrorLevel)
				} else {
					lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean data structures: %v", err), hzService, log.ErrorLevel)
				}
				return err
			} else {
				if numCleanedDataStructures > 0 {
					lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned state in %d data structure/-s", numCleanedDataStructures), hzService, log.InfoLevel)
				} else {
					lp.LogStateCleanerEvent("execution of cleaner was successful; however, zero data structures were cleaned", hzService, log.InfoLevel)
				}
			}

			return nil
		}()

		<-ctx.Done()

		if err != nil {
			return err
		}

	}

	return nil

}

func (b cleanerConfigBuilder) populateConfig() (*cleanerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var usePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.enabled", client.ValidateBool, func(a any) {
			usePrefix = a.(bool)
		})
	})

	var prefix string
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.prefix", client.ValidateString, func(a any) {
			prefix = a.(string)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &cleanerConfig{
		enabled:   enabled,
		usePrefix: usePrefix,
		prefix:    prefix,
	}, nil

}
