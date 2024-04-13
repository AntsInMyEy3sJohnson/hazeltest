package state

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"strings"
)

type (
	cleanerBuilder interface {
		build(hzCluster string, hzMembers []string) (cleaner, error)
	}
	cleaner interface {
		clean() error
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
		mapStore  hzMapStore
	}
	hzMap interface {
		EvictAll(ctx context.Context) error
	}
	hzMapStore interface {
		client.HzClientInitializer
		GetMap(ctx context.Context, name string) (hzMap, error)
		GetDistributedObjectsInfo(ctx context.Context) ([]types.DistributedObjectInfo, error)
		client.HzClientCloser
	}
	defaultHzMapStore struct {
		hzClient *hazelcast.Client
	}
)

const (
	baseKeyPath         = "stateCleaner"
	hzInternalMapPrefix = "__"
	hzMapService        = "hz:impl:mapService"
	hzQueueService      = "hz:impl:queueService"
)

var (
	builders []cleanerBuilder
	lp       *logging.LogProvider
)

func init() {
	register(newMapCleanerBuilder())
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

func register(cb cleanerBuilder) {
	builders = append(builders, cb)
}

func (ms *defaultHzMapStore) InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) {
	ms.hzClient = client.NewHzClientHelper().AssembleHazelcastClient(ctx, clientName, hzCluster, hzMembers)
}

func (ms *defaultHzMapStore) Shutdown(ctx context.Context) error {
	return ms.hzClient.Shutdown(ctx)
}

func (ms *defaultHzMapStore) GetMap(ctx context.Context, name string) (hzMap, error) {
	return ms.hzClient.GetMap(ctx, name)
}

func (ms *defaultHzMapStore) GetDistributedObjectsInfo(ctx context.Context) ([]types.DistributedObjectInfo, error) {
	return ms.hzClient.GetDistributedObjectsInfo(ctx)
}

func (b *mapCleanerBuilder) build(hzCluster string, hzMembers []string) (cleaner, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), log.ErrorLevel)
		return nil, err
	}

	return &mapCleaner{
		name:      "mapStateCleaner",
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		mapStore:  &defaultHzMapStore{},
	}, nil

}

func (c *mapCleaner) clean() error {

	ctx := context.TODO()
	defer func() {
		_ = c.mapStore.Shutdown(ctx)
	}()

	c.mapStore.InitHazelcastClient(ctx, c.name, c.hzCluster, c.hzMembers)

	infoList, err := c.mapStore.GetDistributedObjectsInfo(ctx)

	if err != nil {
		return err
	}

	var mapNames []string
	for _, distributedObjectInfo := range infoList {
		objectName := distributedObjectInfo.Name
		if distributedObjectInfo.ServiceName == hzMapService && !strings.HasPrefix(objectName, hzInternalMapPrefix) {
			lp.LogStateCleanerEvent(fmt.Sprintf("identified the following map to evict: %s", distributedObjectInfo.Name), log.TraceLevel)
			mapNames = append(mapNames, distributedObjectInfo.Name)
		}
	}

	var cleanedMaps []string
	for _, mapName := range mapNames {
		hzMap, err := c.mapStore.GetMap(ctx, mapName)
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean map '%s' due to error upon retrieval of map object from Hazelcast cluster: %v", mapName, err), log.ErrorLevel)
			return err
		}
		if err := hzMap.EvictAll(ctx); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to evict map '%s': %v", mapName, err), log.ErrorLevel)
			return err
		}
		lp.LogStateCleanerEvent(fmt.Sprintf("map '%s' successfully evicted", mapName), log.TraceLevel)
		cleanedMaps = append(cleanedMaps, mapName)
	}

	if len(cleanedMaps) > 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("successfully evicted %d maps", len(cleanedMaps)), log.InfoLevel)
	}

	return nil

}

func RunCleaners(hzCluster string, hzMembers []string) error {

	for _, b := range builders {

		c, err := b.build(hzCluster, hzMembers)
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder due to error: %v", err), log.ErrorLevel)
			return err
		}

		if err := c.clean(); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean state: %v", err), log.ErrorLevel)
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
