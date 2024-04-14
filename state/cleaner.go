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
		clean(ctx context.Context) error
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

	clientName := "mapStateCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	return &mapCleaner{
		name:      "mapStateCleaner",
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		c:         config,
		ms:        &defaultHzMapStore{ch.getClient()},
		ois:       &defaultHzObjectInfoStore{ch.getClient()},
		ch:        ch,
	}, nil

}

func (c *mapCleaner) clean(ctx context.Context) error {

	defer func() {
		_ = c.ch.Shutdown(ctx)
	}()

	infoList, err := c.ois.GetDistributedObjectsInfo(ctx)

	if err != nil {
		return err
	}

	var mapNames []string
	for _, distributedObjectInfo := range infoList {
		objectName := distributedObjectInfo.getName()
		if distributedObjectInfo.getServiceName() == hzMapService && !strings.HasPrefix(objectName, hzInternalMapPrefix) {
			lp.LogStateCleanerEvent(fmt.Sprintf("identified the following map to evict: %s", objectName), log.TraceLevel)
			mapNames = append(mapNames, objectName)
		}
	}

	var cleanedMaps []string
	for _, mapName := range mapNames {
		hzMap, err := c.ms.GetMap(ctx, mapName)
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

		ctx := context.TODO()

		c, err := b.build(&defaultHzClientHandler{}, ctx, hzCluster, hzMembers)
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder due to error: %v", err), log.ErrorLevel)
			return err
		}

		if err := c.clean(ctx); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean state: %v", err), log.ErrorLevel)
			return err
		}

		ctx.Done()
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
