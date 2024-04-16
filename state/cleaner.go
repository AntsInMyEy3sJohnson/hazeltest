package state

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"strings"
)

type (
	hzMap interface {
		EvictAll(ctx context.Context) error
	}
	hzMapStore interface {
		GetMap(ctx context.Context, name string) (hzMap, error)
	}
	hzQueueStore      interface{}
	hzObjectInfoStore interface {
		GetDistributedObjectsInfo(ctx context.Context) ([]hzObjectInfo, error)
	}
	hzClientHandler interface {
		getClient() *hazelcast.Client
		client.HzClientInitializer
		client.HzClientCloser
	}
	hzObjectInfo interface {
		getName() string
		getServiceName() string
	}
	defaultHzMapStore struct {
		hzClient *hazelcast.Client
	}
	defaultHzQueueStore struct {
		hzClient *hazelcast.Client
	}
	defaultHzObjectInfoStore struct {
		hzClient *hazelcast.Client
	}
	defaultHzClientHandler struct {
		hzClient *hazelcast.Client
	}
	simpleObjectInfo struct {
		name, serviceName string
	}
)

type (
	cleanerBuilder interface {
		build(ch hzClientHandler, ctx context.Context, hzCluster string, hzMembers []string) (cleaner, error)
	}
	cleaner interface {
		clean(ctx context.Context) (int, error)
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
		ms        hzMapStore
		ois       hzObjectInfoStore
		ch        hzClientHandler
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
		qs        hzQueueStore
		ois       hzObjectInfoStore
		ch        hzClientHandler
	}
)

const (
	baseKeyPath                   = "stateCleaner"
	hzInternalDataStructurePrefix = "__"
	hzMapService                  = "hz:impl:mapService"
	hzQueueService                = "hz:impl:queueService"
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
			keyPath: baseKeyPath + ".maps",
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func newQueueCleanerBuilder() *queueCleanerBuilder {

	return &queueCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: baseKeyPath + ".queues",
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func register(cb cleanerBuilder) {
	builders = append(builders, cb)
}

func (i simpleObjectInfo) getName() string {

	return i.name

}

func (i simpleObjectInfo) getServiceName() string {

	return i.serviceName

}

func (ms *defaultHzMapStore) GetMap(ctx context.Context, name string) (hzMap, error) {

	return ms.hzClient.GetMap(ctx, name)

}

func (ois *defaultHzObjectInfoStore) GetDistributedObjectsInfo(ctx context.Context) ([]hzObjectInfo, error) {

	infos, err := ois.hzClient.GetDistributedObjectsInfo(ctx)

	if err != nil {
		return nil, err
	}

	var result []hzObjectInfo

	for _, v := range infos {
		i := &simpleObjectInfo{
			name:        v.Name,
			serviceName: v.ServiceName,
		}
		result = append(result, i)
	}

	return result, nil

}

func (ch *defaultHzClientHandler) InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) {
	ch.hzClient = client.NewHzClientHelper().AssembleHazelcastClient(ctx, clientName, hzCluster, hzMembers)
}

func (ch *defaultHzClientHandler) Shutdown(ctx context.Context) error {
	return ch.hzClient.Shutdown(ctx)
}

func (ch *defaultHzClientHandler) getClient() *hazelcast.Client {
	return ch.hzClient
}

func (b *mapCleanerBuilder) build(ch hzClientHandler, ctx context.Context, hzCluster string, hzMembers []string) (cleaner, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), log.ErrorLevel)
		return nil, err
	}

	clientName := "mapCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	return &mapCleaner{
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		ms:        &defaultHzMapStore{ch.getClient()},
		ois:       &defaultHzObjectInfoStore{ch.getClient()},
		ch:        ch,
	}, nil

}

func identifyCandidateDataStructuresFromObjectInfo(info []hzObjectInfo, hzService string) []hzObjectInfo {

	var result []hzObjectInfo

	for _, v := range info {
		if !strings.HasPrefix(v.getName(), hzInternalDataStructurePrefix) && v.getServiceName() == hzService {
			result = append(result, v)
		}
	}

	return result

}

func (c *mapCleaner) clean(ctx context.Context) (int, error) {

	defer func() {
		_ = c.ch.Shutdown(ctx)
	}()

	if !c.c.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("map cleaner '%s' not enabled; won't run", c.name), log.InfoLevel)
		return 0, nil
	}

	infoList, err := c.ois.GetDistributedObjectsInfo(ctx)

	if err != nil {
		return 0, err
	}

	candidateMaps := identifyCandidateDataStructuresFromObjectInfo(infoList, hzMapService)
	if len(candidateMaps) > 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("identified %d map candidate/-s for cleaning", len(candidateMaps)), log.TraceLevel)
	} else {
		lp.LogStateCleanerEvent("no map candidates for cleaning identified in target hazelcast cluster", log.TraceLevel)
		return 0, nil
	}

	var filteredMaps []hzObjectInfo
	if c.c.usePrefix {
		lp.LogStateCleanerEvent(fmt.Sprintf("applying prefix '%s' to %d map candidate/-s identified for cleaning", c.c.prefix, len(candidateMaps)), log.TraceLevel)
		for _, v := range candidateMaps {
			if strings.HasPrefix(v.getName(), c.c.prefix) {
				filteredMaps = append(filteredMaps, v)
			}
		}
	} else {
		filteredMaps = candidateMaps
	}

	// TODO Send names of cleaned data structures to status gatherer
	var cleanedMaps []string
	for _, v := range filteredMaps {
		hzMap, err := c.ms.GetMap(ctx, v.getName())
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean map '%s' due to error upon retrieval of map object from Hazelcast cluster: %v", v, err), log.ErrorLevel)
			return len(cleanedMaps), err
		}
		if err := hzMap.EvictAll(ctx); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean map '%s': %v", v, err), log.ErrorLevel)
			return len(cleanedMaps), err
		}
		lp.LogStateCleanerEvent(fmt.Sprintf("map '%s' successfully cleaned", v), log.TraceLevel)
		cleanedMaps = append(cleanedMaps, v.getName())
	}

	return len(cleanedMaps), nil

}

func (b *queueCleanerBuilder) build(ch hzClientHandler, ctx context.Context, hzCluster string, hzMembers []string) (cleaner, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), log.ErrorLevel)
		return nil, err
	}

	clientName := "queueCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	return &queueCleaner{
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		qs:        &defaultHzQueueStore{hzClient: ch.getClient()},
		ois:       &defaultHzObjectInfoStore{hzClient: ch.getClient()},
		ch:        ch,
	}, nil

}

func (c *queueCleaner) clean(_ context.Context) (int, error) {

	return 0, nil

}

func RunCleaners(hzCluster string, hzMembers []string) error {

	for _, b := range builders {

		ctx, cancel := context.WithCancel(context.Background())
		err := func() error {
			defer cancel()

			c, err := b.build(&defaultHzClientHandler{}, ctx, hzCluster, hzMembers)
			if err != nil {
				lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder due to error: %v", err), log.ErrorLevel)
				return err
			}

			if numCleanedDataStructures, err := c.clean(ctx); err != nil {
				if numCleanedDataStructures > 0 {
					lp.LogStateCleanerEvent(fmt.Sprintf("%d data structure/-s were cleaned before encountering error: %v", numCleanedDataStructures, err), log.ErrorLevel)
				} else {
					lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean data structures: %v", err), log.ErrorLevel)
				}
				return err
			} else {
				if numCleanedDataStructures > 0 {
					lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned state in %d data structures", numCleanedDataStructures), log.InfoLevel)
				} else {
					lp.LogStateCleanerEvent("execution of cleaner was successful; however, zero data structures were cleaned", log.InfoLevel)
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
