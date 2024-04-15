package state

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"strings"
	"sync"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		dummyConfig map[string]any
	}
	testCleanerBuilder struct {
		behavior         *testCleanerBehavior
		buildInvocations int
	}
	testCleanerBehavior struct {
		throwErrorUponBuild, throwErrorUponClean bool
	}
	testCleaner struct {
		behavior *testCleanerBehavior
	}
	cleanerWatcher struct {
		m                sync.Mutex
		cleanInvocations int
	}
	testHzClientHandler struct {
		hzClient *hazelcast.Client
	}
	testHzMapStore struct {
		maps              map[string]*testHzMap
		getMapInvocations int
	}
	testHzObjectInfoStore struct {
		objectInfos                         []simpleObjectInfo
		getDistributedObjectInfoInvocations int
	}
	testHzMap struct {
		data                map[string]any
		evictAllInvocations int
	}
)

const (
	checkMark          = "\u2713"
	ballotX            = "\u2717"
	mapCleanerBasePath = "stateCleaner.maps"
	hzCluster          = "awesome-hz-cluster"
	mapCleanerName     = "awesome-map-cleaner"
)

var (
	hzMembers         = []string{"awesome-hz-member:5701", "another-awesome-hz-member:5701"}
	cw                = cleanerWatcher{}
	cleanerBuildError = errors.New("something went terribly wrong when attempting to build the cleaner")
	cleanerCleanError = errors.New("something went terribly wrong when attempting to clean state")
)

func (m *testHzMap) EvictAll(_ context.Context) error {

	m.evictAllInvocations++

	clear(m.data)

	return nil

}

func (ms *testHzMapStore) GetMap(_ context.Context, name string) (hzMap, error) {

	ms.getMapInvocations++

	return ms.maps[name], nil

}

func (ois *testHzObjectInfoStore) GetDistributedObjectsInfo(_ context.Context) ([]hzObjectInfo, error) {

	ois.getDistributedObjectInfoInvocations++

	var hzObjectInfos []hzObjectInfo
	for _, v := range ois.objectInfos {
		hzObjectInfos = append(hzObjectInfos, v)
	}

	return hzObjectInfos, nil

}

func (ch *testHzClientHandler) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// no-op
}

func (ch *testHzClientHandler) Shutdown(_ context.Context) error {
	return nil
}

func (ch *testHzClientHandler) getClient() *hazelcast.Client {
	return nil
}

func (cw *cleanerWatcher) reset() {
	cw.m = sync.Mutex{}

	cw.cleanInvocations = 0
}

func (c *testCleaner) clean(_ context.Context) error {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanInvocations++

	if c.behavior.throwErrorUponClean {
		return cleanerCleanError
	}

	return nil

}

func (b *testCleanerBuilder) build(_ hzClientHandler, _ context.Context, _ string, _ []string) (cleaner, error) {

	b.buildInvocations++

	if b.behavior.throwErrorUponBuild {
		return nil, cleanerBuildError
	}

	return &testCleaner{behavior: b.behavior}, nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	} else {
		return fmt.Errorf("test error: unable to find value in dummy config for given key path '%s'", keyPath)
	}

	return nil

}

func newMapObjectInfoFromName(objectInfoName string) *simpleObjectInfo {
	return &simpleObjectInfo{
		name:        objectInfoName,
		serviceName: hzMapService,
	}
}

func newQueueObjectInfoFromName(objectInfoName string) *simpleObjectInfo {
	return &simpleObjectInfo{
		name:        objectInfoName,
		serviceName: hzQueueService,
	}
}

func populateDummyObjectsWithMapObjectInfoFromName(objectInfos *[]simpleObjectInfo, testMaps map[string]*testHzMap, nameOfNewMapObject string) {

	*objectInfos = append(*objectInfos, *newMapObjectInfoFromName(nameOfNewMapObject))
	testMaps[nameOfNewMapObject] = &testHzMap{data: make(map[string]any)}

}

func resolveObjectKindForNameFromObjectInfoList(name string, objectInfos []simpleObjectInfo) string {

	for _, v := range objectInfos {
		if v.getName() == name {
			return v.getServiceName()
		}
	}

	return ""

}

func TestMapCleanerClean(t *testing.T) {

	t.Log("given a map cleaner build method")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues")
		{
			numMapObjects := 9
			var objectInfos []simpleObjectInfo
			testMaps := make(map[string]*testHzMap)
			prefixToConsider := "ht_"

			for i := 0; i < numMapObjects; i++ {
				populateDummyObjectsWithMapObjectInfoFromName(&objectInfos, testMaps, fmt.Sprintf("%sload-%d", prefixToConsider, i))
				populateDummyObjectsWithMapObjectInfoFromName(&objectInfos, testMaps, fmt.Sprintf("gimli_load-%d", i))
			}

			// Add object representing queue, so we can verify that no attempt was made to retrieve it
			// The name of this object matches the given predicate, so method under test must use service name to establish
			// object in question represents queue
			queueObjectName := fmt.Sprintf("%sload-42", prefixToConsider)
			objectInfos = append(objectInfos, *newQueueObjectInfoFromName(queueObjectName))
			testMaps[queueObjectName] = &testHzMap{data: make(map[string]any)}

			// Add Hazelcast-internal map
			hzInternalMapName := "__sql.catalog"
			objectInfos = append(objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
			testMaps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

			ms := &testHzMapStore{
				maps: testMaps,
			}
			ois := &testHzObjectInfoStore{
				objectInfos: objectInfos,
			}
			mc := mapCleaner{
				name:      mapCleanerName,
				hzCluster: hzCluster,
				hzMembers: hzMembers,
				keyPath:   mapCleanerBasePath,
				c: &cleanerConfig{
					enabled:   true,
					usePrefix: true,
					prefix:    prefixToConsider,
				},
				ms:  ms,
				ois: ois,
				ch:  &testHzClientHandler{},
			}

			ctx := context.TODO()
			err := mc.clean(ctx)

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdistributed objects info must have been queried once"
			if ois.getDistributedObjectInfoInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tget map must have been invoked only on maps whose prefix matches configuration"
			if ms.getMapInvocations == numMapObjects {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocations)
			}

			invokedOnceMsg := "\t\tevict all must have been invoked on all maps whose prefix matches configuration"
			notInvokedMsg := "\t\tevict all must not have been invoked on data structure that is either not a map or whose name does not correspond to given prefix"
			for k, v := range testMaps {
				if strings.HasPrefix(k, prefixToConsider) && resolveObjectKindForNameFromObjectInfoList(k, objectInfos) == hzMapService {
					if v.evictAllInvocations == 1 {
						t.Log(invokedOnceMsg, checkMark, k)
					} else {
						t.Fatal(invokedOnceMsg, ballotX, k, v.evictAllInvocations)
					}
				} else {
					if v.evictAllInvocations == 0 {
						t.Log(notInvokedMsg, checkMark, k)
					} else {
						t.Fatal(notInvokedMsg, ballotX, k, v.evictAllInvocations)
					}
				}

			}

		}
	}

}

func TestMapCleanerBuilderBuild(t *testing.T) {

	t.Log("given a method to build a map cleaner builder")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig()}

			tch := &testHzClientHandler{}
			c, err := b.build(tch, context.TODO(), hzCluster, hzMembers)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tmap cleaner built must carry map state cleaner key path"
			mc := c.(*mapCleaner)

			if mc.keyPath == mapCleanerBasePath {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mc.keyPath)
			}

			msg = "\t\tmap cleaner built must carry state cleaner config"
			if mc.c != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap cleaner built must carry hazelcast map store"
			if mc.ms != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap cleaner built must carry hazelcast object info store"
			if mc.ois != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap cleaner built must carry hazelcast client handler"
			if mc.ch != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestRunCleaners(t *testing.T) {

	t.Log("given a function to invoke registered state cleaner builders")
	{
		t.Log("\twhen at least one state cleaner builder has registered")
		{
			t.Log("\t\twhen both build and clean invocations are successful")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tbuilder's build method must have been invoked once"
					if b.buildInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, b.buildInvocations)
					}

					msg = "\t\t\tclean method must have been invoked once"
					if cw.cleanInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, cw.cleanInvocations)
					}
				})
			}
			t.Log("\t\twhen build invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						throwErrorUponBuild: true,
					}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during build must be returned"
					if errors.Is(err, cleanerBuildError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}
			t.Log("\t\twhen clean invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						throwErrorUponClean: true,
					}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during clean must be returned"
					if errors.Is(err, cleanerCleanError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}

		}
	}

}

func runTestCaseAndResetState(testFunc func()) {

	defer cw.reset()
	testFunc()

}

func assembleTestConfig() map[string]any {

	return map[string]any{
		mapCleanerBasePath + ".enabled":        true,
		mapCleanerBasePath + ".prefix.enabled": true,
		mapCleanerBasePath + ".prefix.prefix":  "ht_",
	}

}
