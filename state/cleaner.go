package state

import (
	"context"
	"errors"
	"fmt"
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
	BatchCleaner interface {
		Clean() (int, error)
	}
	BatchCleanerBuilder interface {
		Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error)
	}
	DefaultBatchMapCleanerBuilder struct {
		cfb cleanerConfigBuilder
	}
	DefaultBatchMapCleaner struct {
		ctx       context.Context
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		c         *cleanerConfig
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hazelcastwrapper.HzClientHandler
		cih       lastCleanedInfoHandler
		t         cleanedTracker
	}
	DefaultBatchQueueCleanerBuilder struct {
		cfb cleanerConfigBuilder
	}
	DefaultBatchQueueCleaner struct {
		ctx       context.Context
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		c         *cleanerConfig
		qs        hazelcastwrapper.QueueStore
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hazelcastwrapper.HzClientHandler
		cih       lastCleanedInfoHandler
		t         cleanedTracker
	}
)

type (
	SingleCleaner interface {
		Clean(name string) (int, error)
	}
	// SingleMapCleanerBuilder is an interface for encapsulating the capability of assembling map cleaners
	// implementing the SingleCleaner interface. An interesting detail is perhaps that the Build methods for
	// single-data-structure cleaners ask for very few values encapsulating knowledge about the target Hazelcast cluster
	// as well as capability to access it when compared to the batch cleaner's Build methods.
	// That is because a batch cleaner is much more standalone in terms of its functionality -- basically, a batch
	// cleaner registers and initializes itself (including a Hazelcast client), and then investigates the
	// target Hazelcast cluster for data structures to be cleaned all on its own. This requires the batch cleaner
	// assembled by the builder to have more knowledge about the target Hazelcast cluster (in order to successfully
	// initialize a working Hazelcast client) as well as substantially more capabilities to assert the current state
	// of the cluster in terms of the data structures it holds. Cleaners for single-data-structure cleaning, on the
	// other hand, are told which data structure to clean. Hence, all the work in connecting to the target Hazelcast
	// cluster and figuring out which data structures there are has already been performed by the caller. Therefore, the
	// caller must have the knowledge and capability for doing so, and might as well pass both into the single-cleaner
	// build method. (Aligning the signature of the single-cleaner's Build methods more to those of the batch cleaners
	// would be possible, but would lead to inefficiency. For example, why would a single cleaner initialize a
	// new Hazelcast client if the caller already has one?)
	SingleMapCleanerBuilder interface {
		Build(ctx context.Context, ms hazelcastwrapper.MapStore, t cleanedTracker, cih lastCleanedInfoHandler) (SingleCleaner, string)
	}
	// SingleQueueCleanerBuilder is an interface for encapsulating the capability of assembling queue cleaners
	// implementing the SingleCleaner interface. Concerning why the Build method asks for so little knowledge about
	// the target Hazelcast cluster and capabilities for accessing it, the same thoughts as on the
	// SingleMapCleanerBuilder interface apply.
	SingleQueueCleanerBuilder interface {
		Build(ctx context.Context, qs hazelcastwrapper.QueueStore, ms hazelcastwrapper.MapStore, t cleanedTracker, cih lastCleanedInfoHandler) SingleCleaner
	}
	DefaultSingleMapCleanerBuilder struct{}
	DefaultSingleMapCleaner        struct {
		ctx context.Context
		ms  hazelcastwrapper.MapStore
		cih lastCleanedInfoHandler
		t   cleanedTracker
	}
	DefaultSingleQueueCleanerBuilder struct{}
	DefaultSingleQueueCleaner        struct {
		ctx context.Context
		ms  hazelcastwrapper.MapStore
		qs  hazelcastwrapper.QueueStore
		cih lastCleanedInfoHandler
		t   cleanedTracker
	}
)

type (
	lastCleanedInfoHandler interface {
		// check asserts whether the given payload data structure in the target Hazelcast cluster is susceptible
		// to cleaning. In doing so, it acquires a lock on the given sync map (a map that, for each kind of Cleaner
		// implementation, tracks when payload data structures were last cleaned). The lock is not released
		// by this method in order to make sure a caller can invoke check and then update without another caller
		// overwriting the last cleaned info in the sync map in the meantime.
		// To signal to the caller that a lock was acquired and must potentially be released, the method returns
		// a mapLockInfo value -- if non-nil, then there is a lock to be released. See also mapLockInfo.
		// (One might reason that acquiring a lock in method invoked by a caller and then having the caller release the
		// lock is a nice source for bugs -- and it probably is. The use case of the Cleaner, however, dictates this:
		// First, the Cleaner must check whether a given payload data structure is susceptible to cleaning, then
		// it must clean the data structure, and make sure the last cleaned info is updated. To make sure no other
		// actor updates or reads the last cleaned info record during this process, the lock must be held for the
		// process' entire duration. Because check itself cannot know (and should not know) what the caller does
		// with the should clean result, it also cannot know whether it is already time to safely release the lock.
		// Therefore, this task must be handed to the caller -- and it can also not be in the responsibility of the
		// update method, because if a payload data structure was not susceptible to cleaning, then there is nothing to
		// update, hence the caller will not invoke update, but the lock must be released nonetheless. Hence, acquiring
		// and releasing the lock is split between invoker and invoked method.)
		check(syncMapName, payloadDataStructureName, hzService string) (mapLockInfo, bool, error)
		update(lockInfo mapLockInfo) error
	}
	cleanedTracker interface {
		add(name string, cleaned int)
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
	// mapLockInfo exists to signal to the caller of the check method on an implementation of
	// lastCleanedInfoHandler that the method acquired a lock on the map represented by the map
	// proxy object contained in the return value (mapLockInfo.m) for a specific key (mapLockInfo.keyName),
	// so the caller knows it should try to release the lock on the map for the key.
	mapLockInfo struct {
		m            hazelcastwrapper.Map
		mapName, key string
	}
	defaultLastCleanedInfoHandler struct {
		ctx context.Context
		ms  hazelcastwrapper.MapStore
	}
	CleanedDataStructureTracker struct {
		G *status.Gatherer
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
	// TODO Read this value from config
	cleanAgainThresholdMs = 600_000
)

var (
	builders         []BatchCleanerBuilder
	lp               *logging.LogProvider
	emptyMapLockInfo = mapLockInfo{}
)

func init() {
	register(newMapCleanerBuilder())
	register(newQueueCleanerBuilder())
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func newMapCleanerBuilder() *DefaultBatchMapCleanerBuilder {

	return &DefaultBatchMapCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: mapCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func newQueueCleanerBuilder() *DefaultBatchQueueCleanerBuilder {

	return &DefaultBatchQueueCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: queueCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func register(cb BatchCleanerBuilder) {
	builders = append(builders, cb)
}

func (t *CleanedDataStructureTracker) add(name string, cleaned int) {

	t.G.Updates <- status.Update{Key: name, Value: cleaned}

}

func (b *DefaultBatchMapCleanerBuilder) Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), hzMapService, log.ErrorLevel)
		return nil, hzMapService, err
	}

	clientName := "mapCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.GetClient()}
	cih := &defaultLastCleanedInfoHandler{
		ctx: ctx,
		ms:  ms,
	}

	t := &CleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.G.AssembleStatusCopy)

	return &DefaultBatchMapCleaner{
		ctx:       ctx,
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.GetClient()},
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

func (cih *defaultLastCleanedInfoHandler) check(syncMapName, payloadDataStructureName, hzService string) (mapLockInfo, bool, error) {

	syncMap, err := cih.ms.GetMap(cih.ctx, syncMapName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to retrieve sync map '%s': %v", syncMapName, err), hzService, log.ErrorLevel)
		return emptyMapLockInfo, false, err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved sync map '%s'", syncMapName), hzService, log.DebugLevel)
	lockSucceeded, err := syncMap.TryLock(cih.ctx, payloadDataStructureName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to acquire lock on sync map '%s' for payload data structure '%s': %v", syncMapName, payloadDataStructureName, err), hzService, log.ErrorLevel)
		return emptyMapLockInfo, false, err
	}

	if !lockSucceeded {
		return emptyMapLockInfo, false, fmt.Errorf("unable to acquire lock on sync map '%s' for payload data structure key '%s'", syncMapName, payloadDataStructureName)
	}

	lockInfo := mapLockInfo{
		m:       syncMap,
		mapName: syncMapName,
		key:     payloadDataStructureName,
	}
	lp.LogStateCleanerEvent(fmt.Sprintf("successfully acquired lock on sync map '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.DebugLevel)
	v, err := syncMap.Get(cih.ctx, payloadDataStructureName)
	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon retrieving last updated info from sync map for '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.ErrorLevel)
		return lockInfo, false, err
	}

	// Value will be nil if key (name of payload map) was not present in sync map
	if v == nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that payload data structure '%s' was never cleaned before", payloadDataStructureName), hzService, log.DebugLevel)
		return lockInfo, true, nil
	}

	var lastCleanedAt int64
	if lc, ok := v.(int64); !ok {
		msg := fmt.Sprintf("unable to treat retrieved value '%v' for payload data structure '%s' as int64 timestamp", v, payloadDataStructureName)
		lp.LogStateCleanerEvent(msg, hzService, log.ErrorLevel)
		return lockInfo, false, errors.New(msg)
	} else {
		lastCleanedAt = lc
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved last updated info from sync map '%s' for payload data structure '%s'; last updated at %d", syncMapName, payloadDataStructureName, lastCleanedAt), hzService, log.DebugLevel)
	if time.Since(time.Unix(lastCleanedAt, 0)) < time.Millisecond*cleanAgainThresholdMs {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is less than configured threshold of '%d' milliseconds for payload data structure '%s'-- negative cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.DebugLevel)
		return lockInfo, false, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is greater than or equal to configured threshold of '%d' milliseconds for payload data structure '%s'-- positive cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.DebugLevel)
	return lockInfo, true, nil

}

func (cih *defaultLastCleanedInfoHandler) update(lockInfo mapLockInfo) error {

	mapHoldingLock := lockInfo.m
	payloadDataStructureName := lockInfo.key

	cleanedAt := time.Now().UnixNano()
	ttlAndMaxIdle := time.Millisecond * cleanAgainThresholdMs
	return mapHoldingLock.SetWithTTLAndMaxIdle(cih.ctx, payloadDataStructureName, cleanedAt, ttlAndMaxIdle, ttlAndMaxIdle)

}

func (c *DefaultBatchMapCleaner) Clean() (int, error) {

	defer func() {
		_ = c.ch.Shutdown(c.ctx)
	}()

	if !c.c.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("map cleaner '%s' not enabled; won't run", c.name), hzMapService, log.InfoLevel)
		return 0, nil
	}

	b := DefaultSingleMapCleanerBuilder{}
	sc, _ := b.Build(c.ctx, c.ms, c.t, c.cih)

	return runGenericBatchClean(
		c.ctx,
		c.ois,
		hzMapService,
		c.c.usePrefix,
		c.c.prefix,
		sc,
	)

}

func (b *DefaultBatchQueueCleanerBuilder) Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g *status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), hzQueueService, log.ErrorLevel)
		return nil, hzQueueService, err
	}

	clientName := "queueCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.GetClient()}
	cih := &defaultLastCleanedInfoHandler{
		ms:  ms,
		ctx: ctx,
	}

	t := &CleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.G.AssembleStatusCopy)

	return &DefaultBatchQueueCleaner{
		ctx:       ctx,
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		qs:        &hazelcastwrapper.DefaultQueueStore{Client: ch.GetClient()},
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.GetClient()},
		ch:        ch,
		cih:       cih,
		t:         t,
	}, hzQueueService, nil

}

// TODO Implement test
func releaseLock(ctx context.Context, lockInfo mapLockInfo, hzService string) error {

	if lockInfo == emptyMapLockInfo {
		return nil
	}

	if err := lockInfo.m.Unlock(ctx, lockInfo.key); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to release lock on sync map '%s' for key '%s' due to error: %v", lockInfo.mapName, lockInfo.key, err), hzService, log.ErrorLevel)
		return err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully released lock on sync map '%s' for key '%s'", lockInfo.mapName, lockInfo.key), hzService, log.InfoLevel)
	return nil

}

func (b *DefaultSingleMapCleanerBuilder) Build(ctx context.Context, ms hazelcastwrapper.MapStore, t cleanedTracker, cih lastCleanedInfoHandler) (SingleCleaner, string) {

	return &DefaultSingleMapCleaner{
		ctx: ctx,
		ms:  ms,
		cih: cih,
		t:   t,
	}, hzMapService

}

func runGenericSingleClean(
	ctx context.Context,
	cih lastCleanedInfoHandler,
	t cleanedTracker,
	syncMapName, payloadDataStructureName, hzService string,
	retrieveAndCleanFunc func(payloadDataStructureName string) (int, error),
) (int, error) {

	lockInfo, shouldClean, err := cih.check(syncMapName, payloadDataStructureName, hzService)

	// This defer ensures that the lock on the sync map for the given payload map is always released
	// no matter the susceptibility of the payload map for cleaning.
	defer func() {
		if err := releaseLock(ctx, lockInfo, hzService); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to release lock on '%s' for key '%s' due to error: %v", syncMapName, payloadDataStructureName, err), hzService, log.ErrorLevel)
		}
	}()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to determine whether '%s' should be cleaned due to error: %v", payloadDataStructureName, err), hzService, log.ErrorLevel)
		return 0, err
	}

	if !shouldClean {
		lp.LogStateCleanerEvent(fmt.Sprintf("clean not required for '%s'", payloadDataStructureName), hzService, log.InfoLevel)
		return 0, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("determined that '%s' should be cleaned of state, commencing...", payloadDataStructureName), hzService, log.InfoLevel)
	numItemsCleaned, err := retrieveAndCleanFunc(payloadDataStructureName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadDataStructureName, err), hzService, log.ErrorLevel)
		return 0, err
	}

	t.add(payloadDataStructureName, numItemsCleaned)
	lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned '%s', which held %d items", payloadDataStructureName, numItemsCleaned), hzService, log.InfoLevel)

	if err := cih.update(lockInfo); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attemot to update last cleaned info for '%s': %v", payloadDataStructureName, err), hzService, log.ErrorLevel)
		return numItemsCleaned, err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("last cleaned info successfully updated for '%s'", payloadDataStructureName), hzService, log.InfoLevel)
	return numItemsCleaned, nil

}

func (c *DefaultSingleMapCleaner) retrieveAndClean(payloadMapName string) (int, error) {

	mapToClean, err := c.ms.GetMap(c.ctx, payloadMapName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean '%s' due to error upon retrieval of proxy object from Hazelcast cluster: %v", payloadMapName, err), hzMapService, log.ErrorLevel)
		return 0, err
	}

	if mapToClean == nil {
		msg := fmt.Sprintf("cannot clean '%s' because map retrieved from target Hazelcast cluster was nil", payloadMapName)
		lp.LogStateCleanerEvent(msg, hzMapService, log.ErrorLevel)
		return 0, errors.New(msg)
	}

	size, err := mapToClean.Size(c.ctx)
	if err != nil {
		msg := fmt.Sprintf("unable to clean '%s' because retrieval of current size returned with error: %v", payloadMapName, err)
		lp.LogStateCleanerEvent(msg, hzMapService, log.ErrorLevel)
		return 0, err
	}

	if size == 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("payload map '%s' does not currently hold any items -- skipping", payloadMapName), hzMapService, log.DebugLevel)
		return 0, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("payload map '%s' currently holds %d elements -- proceeding to clean", payloadMapName, size), hzMapService, log.DebugLevel)

	if err := mapToClean.EvictAll(c.ctx); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadMapName, err), hzMapService, log.ErrorLevel)
		return 0, err
	}
	return size, nil

}

func (c *DefaultSingleMapCleaner) Clean(name string) (int, error) {

	return runGenericSingleClean(
		c.ctx,
		c.cih,
		c.t,
		mapCleanersSyncMapName,
		name,
		hzMapService,
		c.retrieveAndClean,
	)

}

// TODO Implement tests for this function
func runGenericBatchClean(
	ctx context.Context,
	ois hazelcastwrapper.ObjectInfoStore,
	hzService string,
	usePrefix bool,
	prefix string,
	sc SingleCleaner,
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
	if usePrefix {
		lp.LogStateCleanerEvent(fmt.Sprintf("applying prefix '%s' to %d data structure candidate/-s identified for cleaning", prefix, len(candidateDataStructures)), hzService, log.TraceLevel)
		for _, v := range candidateDataStructures {
			if strings.HasPrefix(v.GetName(), prefix) {
				filteredDataStructures = append(filteredDataStructures, v)
			}
		}
	} else {
		filteredDataStructures = candidateDataStructures
	}

	numCleanedDataStructures := 0
	for _, v := range filteredDataStructures {
		if numItemsCleaned, err := sc.Clean(v.GetName()); numItemsCleaned > 0 {
			numCleanedDataStructures++
			if err != nil {
				lp.LogStateCleanerEvent(fmt.Sprintf("%d elements have been cleaned from payload data structure '%s', but an error occurred during cleaning: %v", numItemsCleaned, v.GetName(), err), hzService, log.ErrorLevel)
				return numCleanedDataStructures, err
			} else {
				lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned %d elements from payload data structure '%s'; cleaned %d data structure/-s so far", numItemsCleaned, v.GetName(), numCleanedDataStructures), hzService, log.InfoLevel)
			}
		} else {
			if err != nil {
				lp.LogStateCleanerEvent(fmt.Sprintf("unable to clean '%s' due to error: %v", v.GetName(), err), hzService, log.ErrorLevel)
				return numCleanedDataStructures, err
			} else {
				lp.LogStateCleanerEvent(fmt.Sprintf("invocation of clean was successful on payload data structure '%s'; however, zero items were cleaned", v.GetName()), hzService, log.InfoLevel)
			}
		}
	}

	return numCleanedDataStructures, nil

}

func (b *DefaultSingleQueueCleanerBuilder) Build(ctx context.Context, qs hazelcastwrapper.QueueStore, ms hazelcastwrapper.MapStore, t cleanedTracker, cih lastCleanedInfoHandler) (SingleCleaner, string) {

	return &DefaultSingleQueueCleaner{
		ctx: ctx,
		qs:  qs,
		ms:  ms,
		cih: cih,
		t:   t,
	}, hzQueueService

}

func (c *DefaultSingleQueueCleaner) retrieveAndClean(payloadQueueName string) (int, error) {

	queueToClean, err := c.qs.GetQueue(c.ctx, payloadQueueName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean '%s' due to error upon retrieval of proxy object from Hazelcast cluster: %v", payloadQueueName, err), hzQueueService, log.ErrorLevel)
		return 0, err
	}

	if queueToClean == nil {
		msg := fmt.Sprintf("cannot clean '%s' because queue retrieved from target Hazelcast cluster was nil", payloadQueueName)
		lp.LogStateCleanerEvent(msg, hzQueueService, log.ErrorLevel)
		return 0, errors.New(msg)
	}

	size, err := queueToClean.Size(c.ctx)
	if err != nil {
		msg := fmt.Sprintf("unable to clean '%s' because size check failed with error: %v", payloadQueueName, err)
		lp.LogStateCleanerEvent(msg, hzQueueService, log.ErrorLevel)
		return 0, err
	}

	if size == 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("payload queue '%s' does not currently hold any items -- skipping", payloadQueueName), hzQueueService, log.DebugLevel)
		return 0, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("payload queue '%s' currently holds %d elements -- proceeding to clean", payloadQueueName, size), hzQueueService, log.DebugLevel)

	if err := queueToClean.Clear(c.ctx); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadQueueName, err), hzQueueService, log.ErrorLevel)
		return 0, err
	}

	return size, nil

}

func (c *DefaultSingleQueueCleaner) Clean(name string) (int, error) {

	return runGenericSingleClean(
		c.ctx,
		c.cih,
		c.t,
		queueCleanersSyncMapName,
		name,
		hzQueueService,
		c.retrieveAndClean,
	)

}

func (c *DefaultBatchQueueCleaner) Clean() (int, error) {

	defer func() {
		_ = c.ch.Shutdown(c.ctx)
	}()

	if !c.c.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("queue cleaner '%s' not enabled; won't run", c.name), hzQueueService, log.InfoLevel)
		return 0, nil
	}

	b := DefaultSingleQueueCleanerBuilder{}
	sc, _ := b.Build(c.ctx, c.qs, c.ms, c.t, c.cih)
	numCleaned, err := runGenericBatchClean(
		c.ctx,
		c.ois,
		hzQueueService,
		c.c.usePrefix,
		c.c.prefix,
		sc,
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

			c, hzService, err := b.Build(&hazelcastwrapper.DefaultHzClientHandler{}, ctx, g, hzCluster, hzMembers)
			if err != nil {
				lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder for hazelcast due to error: %v", err), hzService, log.ErrorLevel)
				return err
			}

			if numCleanedDataStructures, err := c.Clean(); err != nil {
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
