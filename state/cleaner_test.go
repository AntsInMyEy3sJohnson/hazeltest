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
		hzClient            *hazelcast.Client
		shutdownInvocations int
	}
	testHzMapStore struct {
		maps                  map[string]*testHzMap
		getMapInvocations     int
		returnErrorUponGetMap bool
	}
	testHzObjectInfoStore struct {
		objectInfos                         []hzObjectInfo
		getDistributedObjectInfoInvocations int
		returnErrorUponGetObjectInfos       bool
	}
	testHzMap struct {
		data                    map[string]any
		evictAllInvocations     int
		returnErrorUponEvictAll bool
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
	hzMembers                     = []string{"awesome-hz-member:5701", "another-awesome-hz-member:5701"}
	cw                            = cleanerWatcher{}
	cleanerBuildError             = errors.New("something went terribly wrong when attempting to build the cleaner")
	cleanerCleanError             = errors.New("something went terribly wrong when attempting to clean state")
	getDistributedObjectInfoError = errors.New("something somewhere went terribly wrong upon retrieval of distributed object info")
	getMapError                   = errors.New("something somewhere went terribly wrong when attempting to get a map from the target hazelcast cluster")
	evictAllError                 = errors.New("something somewhere went terribly wrong upon attempt to perform evict all")
)

func (m *testHzMap) EvictAll(_ context.Context) error {

	m.evictAllInvocations++

	if m.returnErrorUponEvictAll {
		return evictAllError
	}

	clear(m.data)

	return nil

}

func (ms *testHzMapStore) GetMap(_ context.Context, name string) (hzMap, error) {

	ms.getMapInvocations++

	if ms.returnErrorUponGetMap {
		return nil, getMapError
	}

	return ms.maps[name], nil

}

func (ois *testHzObjectInfoStore) GetDistributedObjectsInfo(_ context.Context) ([]hzObjectInfo, error) {

	ois.getDistributedObjectInfoInvocations++

	if ois.returnErrorUponGetObjectInfos {
		return nil, getDistributedObjectInfoError
	}

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
	ch.shutdownInvocations++
	return nil
}

func (ch *testHzClientHandler) getClient() *hazelcast.Client {
	return nil
}

func (cw *cleanerWatcher) reset() {
	cw.m = sync.Mutex{}

	cw.cleanInvocations = 0
}

func (c *testCleaner) clean(_ context.Context) (int, error) {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanInvocations++

	if c.behavior.throwErrorUponClean {
		return 0, cleanerCleanError
	}

	return cw.cleanInvocations, nil

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

func populateDummyMapStore(numMapObjects int, objectNamePrefixes []string) *testHzMapStore {

	testMaps := make(map[string]*testHzMap)

	for i := 0; i < numMapObjects; i++ {
		for _, v := range objectNamePrefixes {
			testMaps[fmt.Sprintf("%sload-%d", v, i)] = &testHzMap{data: make(map[string]any)}
		}
	}

	return &testHzMapStore{maps: testMaps}

}

func populateDummyObjectInfos(numObjects int, objectNamePrefixes []string) *testHzObjectInfoStore {

	var objectInfos []hzObjectInfo
	for i := 0; i < numObjects; i++ {
		for _, v := range objectNamePrefixes {
			objectInfos = append(objectInfos, *newMapObjectInfoFromName(fmt.Sprintf("%sload-%d", v, i)))
		}
	}

	return &testHzObjectInfoStore{objectInfos: objectInfos}

}

func resolveObjectKindForNameFromObjectInfoList(name string, objectInfos []hzObjectInfo) string {

	for _, v := range objectInfos {
		if v.getName() == name {
			return v.getServiceName()
		}
	}

	return ""

}

func assembleMapCleaner(c *cleanerConfig, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler) *mapCleaner {

	return &mapCleaner{
		name:      mapCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   mapCleanerBasePath,
		c:         c,
		ms:        ms,
		ois:       ois,
		ch:        ch,
	}

}

func TestIdentifyCandidateDataStructuresFromObjectInfo(t *testing.T) {

	t.Log("given a function to identify possible candidates for state cleaning from an object info list")
	{
		valid := &simpleObjectInfo{
			name:        "ht_load-1",
			serviceName: hzMapService,
		}
		invalidBecauseSystemInternal := &simpleObjectInfo{
			name:        "__sql.catalog",
			serviceName: hzMapService,
		}
		invalidBecauseRepresentsQueue := &simpleObjectInfo{
			name:        "ht_load-2",
			serviceName: hzQueueService,
		}

		t.Log("\twhen object info retrieval does not yield error")
		{
			t.Log("\t\twhen object info list contains information on both valid candidates and elements not viable as candidates")
			{
				objectInfos := []hzObjectInfo{valid, invalidBecauseSystemInternal, invalidBecauseRepresentsQueue}
				ois := &testHzObjectInfoStore{
					objectInfos: objectInfos,
				}

				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), hzMapService)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tonly valid candidate must be returned"
				if len(candidates) == 1 && candidates[0] == valid {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen object info list only contains information on elements that are not viable candidates")
			{
				ois := &testHzObjectInfoStore{
					objectInfos: []hzObjectInfo{invalidBecauseSystemInternal, invalidBecauseRepresentsQueue},
				}
				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), hzMapService)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\treturned list of candidates must be empty"
				if len(candidates) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen object info list is empty")
			{
				ois := &testHzObjectInfoStore{
					objectInfos: make([]hzObjectInfo, 0),
				}
				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), hzMapService)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\treturned list of candidates must be empty, too"
				if len(candidates) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

		}
		t.Log("\twhen object info retrieval yields error")
		{
			ois := &testHzObjectInfoStore{
				returnErrorUponGetObjectInfos: true,
			}

			candidates, err := identifyCandidateDataStructures(ois, context.TODO(), hzMapService)

			msg := "\t\t\terror must be returned"
			if errors.Is(err, getDistributedObjectInfoError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\t\tlist of identified candidates must be empty"
			if len(candidates) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(candidates))
			}

		}
	}

}

func TestMapCleanerClean(t *testing.T) {

	t.Log("given a map cleaner build method")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues, and all retrieval operations are successful")
		{
			t.Log("\t\twhen prefix usage has been enabled")
			{
				numMapObjects := 9
				prefixToConsider := "ht_"
				prefixes := []string{prefixToConsider, "gimli_"}

				dummyMapStore := populateDummyMapStore(numMapObjects, prefixes)
				dummyObjectInfoStore := populateDummyObjectInfos(numMapObjects, prefixes)

				// Add object representing queue, so we can verify that no attempt was made to retrieve it
				// The name of this object matches the given predicate, so method under test must use service name to establish
				// object in question represents queue
				queueObjectName := fmt.Sprintf("%sload-42", prefixToConsider)
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newQueueObjectInfoFromName(queueObjectName))
				dummyMapStore.maps[queueObjectName] = &testHzMap{data: make(map[string]any)}

				// Add Hazelcast-internal map
				hzInternalMapName := "__sql.catalog"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				dummyMapStore.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled:   true,
					usePrefix: true,
					prefix:    prefixToConsider,
				}
				ch := &testHzClientHandler{}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch)

				ctx := context.TODO()
				numCleaned, err := mc.clean(ctx)

				msg := "\t\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tdistributed objects info must have been queried once"
				if dummyObjectInfoStore.getDistributedObjectInfoInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tget map must have been invoked only on maps whose prefix matches configuration"
				if dummyMapStore.getMapInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocations)
				}

				invokedOnceMsg := "\t\t\tevict all must have been invoked on all maps whose prefix matches configuration"
				notInvokedMsg := "\t\t\tevict all must not have been invoked on data structure that is either not a map or whose name does not correspond to given prefix"
				for k, v := range dummyMapStore.maps {
					if strings.HasPrefix(k, prefixToConsider) && resolveObjectKindForNameFromObjectInfoList(k, dummyObjectInfoStore.objectInfos) == hzMapService {
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

				msg = fmt.Sprintf("\t\tcleaner must report %d cleaned data structures", numMapObjects)
				if numCleaned == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numCleaned)
				}

				msg = "\t\t\thazelcast client must have been closed"
				if ch.shutdownInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ch.shutdownInvocations)
				}

			}
			t.Log("\t\twhen prefix usage has been disabled")
			{
				numMapObjects := 9
				prefixes := []string{"ht_", "gimli_"}

				dummyMapStore := populateDummyMapStore(numMapObjects, prefixes)
				dummyObjectInfoStore := populateDummyObjectInfos(numMapObjects, prefixes)

				// Add Hazelcast-internal map to make sure cleaner does not consider such maps
				// even when prefix usage has been disabled
				hzInternalMapName := "__sql.catalog"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				dummyMapStore.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch)

				numCleaned, err := mc.clean(context.TODO())

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tget all must have been invoked on all maps that are not hazelcast-internal maps"
				expectedCleaned := numMapObjects * len(prefixes)
				if dummyMapStore.getMapInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocations)
				}

				invokedMsg := "\t\t\tevict all must have been invoked on all maps that are not hazelcast-internal maps"
				notInvokedMsg := "\t\t\tevict all must not have been invoked on hazelcast-internal maps"
				for k, v := range dummyMapStore.maps {
					if !strings.HasPrefix(k, hzInternalMapName) {
						if v.evictAllInvocations == 1 {
							t.Log(invokedMsg, checkMark, k)
						} else {
							t.Fatal(invokedMsg, ballotX, k)
						}
					} else {
						if v.evictAllInvocations == 0 {
							t.Log(notInvokedMsg, checkMark, k)
						} else {
							t.Fatal(notInvokedMsg, ballotX, k)
						}
					}
				}

				msg = fmt.Sprintf("\t\t\tcleaner must report %d cleaned data structures", expectedCleaned)
				if numCleaned == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numCleaned)
				}

			}

		}
		t.Log("\twhen target hazelcast cluster does not contain any maps")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			ms := &testHzMapStore{
				maps:              make(map[string]*testHzMap),
				getMapInvocations: 0,
			}
			ois := &testHzObjectInfoStore{
				objectInfos:                         make([]hzObjectInfo, 0),
				getDistributedObjectInfoInvocations: 0,
			}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{})

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tno get map operations must have been performed"
			if ms.getMapInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocations)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}
		}
		t.Log("\twhen retrieval of object info fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			ois := &testHzObjectInfoStore{
				returnErrorUponGetObjectInfos: true,
			}
			mc := assembleMapCleaner(c, &testHzMapStore{}, ois, &testHzClientHandler{})

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, getDistributedObjectInfoError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}
		}
		t.Log("\twhen retrieval of object info succeeds, but get map operation fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}

			ms := populateDummyMapStore(numMapObjects, prefixes)
			ms.returnErrorUponGetMap = true

			ois := populateDummyObjectInfos(numMapObjects, prefixes)

			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{})

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, getMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tthere must have been only one get map invocation"
			if ms.getMapInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocations)
			}

			msg = "\t\tthere must have been no evict all invocations on any map"
			for k, v := range ms.maps {
				if v.evictAllInvocations == 0 {
					t.Log(msg, checkMark, k)
				} else {
					t.Fatal(msg, ballotX, k)
				}
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}
		}
		t.Log("\twhen info retrieval and get map operations succeed, but evict all fails")
		{
			numMapObjects := 9
			prefixes := []string{"ht_"}

			ms := populateDummyMapStore(numMapObjects, prefixes)
			ois := populateDummyObjectInfos(numMapObjects, prefixes)

			erroneousEvictAllMapName := "ht_load-0"
			ms.maps[erroneousEvictAllMapName].returnErrorUponEvictAll = true

			c := &cleanerConfig{
				enabled: true,
			}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{})

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, evictAllError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tthere must have been only one get map invocation"
			if ms.getMapInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			invokedOnceMsg := "\t\tthere must have been only one evict all invocation"
			notInvokedMsg := "\t\tno evict all must have been performed"
			for k, v := range ms.maps {
				if k == erroneousEvictAllMapName {
					if v.evictAllInvocations == 1 {
						t.Log(invokedOnceMsg, checkMark, k)
					} else {
						t.Fatal(invokedOnceMsg, ballotX, k)
					}
				} else {
					if v.evictAllInvocations == 0 {
						t.Log(notInvokedMsg, checkMark, k)
					} else {
						t.Fatal(notInvokedMsg, ballotX, k)
					}
				}
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

		}
		t.Log("\twhen cleaner has not been enabled")
		{
			c := &cleanerConfig{
				enabled: false,
			}
			ms := &testHzMapStore{}
			ois := &testHzObjectInfoStore{}
			ch := &testHzClientHandler{}

			mc := assembleMapCleaner(c, ms, ois, ch)

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tno retrieval of object infos must have been attempted"
			if ois.getDistributedObjectInfoInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ois.getDistributedObjectInfoInvocations)
			}

			msg = "\t\tno map retrieval must have been attempted"
			if ms.getMapInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocations)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
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
