package state

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"strings"
	"sync"
	"testing"
	"time"
)

type (
	testConfigPropertyAssigner struct {
		testConfig                       map[string]any
		returnErrorUponAssignConfigValue bool
	}
	testCleanerBuilder struct {
		behavior         *testCleanerBehavior
		gathererPassedIn *status.Gatherer
		buildInvocations int
	}
	testCleanerBehavior struct {
		numItemsCleanedReturnValue                 int
		returnCleanErrorAfterNoInvocations         int
		returnErrorUponBuild, returnErrorUponClean bool
	}
	testBatchCleaner struct {
		behavior *testCleanerBehavior
	}
	testSingleCleaner struct {
		behavior *testCleanerBehavior
		cfg      *cleanerConfig
	}
	cleanerWatcher struct {
		m                      sync.Mutex
		cleanAllInvocations    int
		cleanSingleInvocations int
	}
	testHzClientHandler struct {
		hzClient            *hazelcast.Client
		shutdownInvocations int
		hzClusterName       string
		hzClusterMembers    []string
	}
	testHzMapStore struct {
		maps                                                                                     map[string]*testHzMap
		getMapInvocationsPayloadMap, getMapInvocationsMapsSyncMap, getMapInvocationsQueueSyncMap int
		returnErrorUponGetPayloadMap, returnErrorUponGetSyncMap                                  bool
	}
	testHzQueueStore struct {
		queues                  map[string]*testHzQueue
		getQueueInvocations     int
		returnErrorUponGetQueue bool
	}
	testHzObjectInfoStore struct {
		objectInfos                         []hazelcastwrapper.ObjectInfo
		getDistributedObjectInfoInvocations int
		returnErrorUponGetObjectInfos       bool
	}
	testHzMap struct {
		data                                map[string]any
		evictAllInvocations                 int
		sizeInvocations                     int
		tryLockInvocations                  int
		unlockInvocations                   int
		getInvocations                      int
		setInvocations                      int
		setWithTTLAndMaxIdleInvocations     int
		returnErrorUponEvictAll             bool
		returnErrorUponSize                 bool
		returnErrorUponTryLock              bool
		returnErrorUponUnlock               bool
		returnErrorUponGet                  bool
		returnErrorUponSet                  bool
		returnErrorUponSetWithTTLAndMaxIdle bool
		tryLockReturnValue                  bool
	}
	testHzQueue struct {
		data                 chan string
		clearInvocations     int
		sizeInvocations      int
		returnErrorUponClear bool
		returnErrorUponSize  bool
	}
	testLastCleanedInfoHandler struct {
		syncMap                                                     hazelcastwrapper.Map
		checkInvocations, updateInvocations                         int
		shouldCleanIndividualMap                                    map[string]bool
		shouldCleanAll, returnErrorUponCheck, returnErrorUponUpdate bool
	}
	testCleanedTracker struct {
		numAddInvocations int
	}
)

func (ch *testHzClientHandler) GetClusterName() string {
	return ch.hzClusterName
}

func (ch *testHzClientHandler) GetClusterMembers() []string {
	return ch.hzClusterMembers
}

func (qs *testHzQueueStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (qs *testHzQueueStore) Shutdown(_ context.Context) error {
	return nil
}

func (ms *testHzMapStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (ms *testHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (q *testHzQueue) Put(_ context.Context, _ any) error {
	return nil
}

func (q *testHzQueue) Poll(_ context.Context) (any, error) {
	return nil, nil
}

func (q *testHzQueue) RemainingCapacity(_ context.Context) (int, error) {
	return 0, nil
}

func (q *testHzQueue) Destroy(_ context.Context) error {
	return nil
}

func (m *testHzMap) ContainsKey(_ context.Context, _ any) (bool, error) {
	return false, nil
}

func (m *testHzMap) Remove(_ context.Context, _ any) (any, error) {
	return false, nil
}

func (m *testHzMap) Destroy(_ context.Context) error {
	return nil
}

func (m *testHzMap) RemoveAll(_ context.Context, _ predicate.Predicate) error {
	return nil
}

const (
	checkMark        = "\u2713"
	ballotX          = "\u2717"
	hzCluster        = "awesome-hz-cluster"
	mapCleanerName   = "awesome-map-cleaner"
	queueCleanerName = "awesome-queue-cleaner"
)

var (
	hzMembers      = []string{"awesome-hz-member:5701", "another-awesome-hz-member:5701"}
	cw             = cleanerWatcher{}
	cleanerKeyPath = "stateCleaners.test"
	testConfig     = map[string]any{
		cleanerKeyPath + ".enabled":                         true,
		cleanerKeyPath + ".prefix.enabled":                  true,
		cleanerKeyPath + ".prefix.prefix":                   "awesome_prefix_",
		cleanerKeyPath + ".cleanAgainThreshold.enabled":     true,
		cleanerKeyPath + ".cleanAgainThreshold.thresholdMs": 30_000,
		cleanerKeyPath + ".errorBehavior":                   "ignore",
	}
	assignConfigPropertyError     = errors.New("something somewhere went terribly wrong during config property assignment")
	cleanerBuildError             = errors.New("something went terribly wrong when attempting to build the cleaner")
	batchCleanerCleanError        = errors.New("something went terribly wrong when attempting to clean state in all data structures")
	singleCleanerCleanError       = errors.New("something somewhere went terribly wrong when attempting to clean state in single data structure")
	getDistributedObjectInfoError = errors.New("something somewhere went terribly wrong upon retrieval of distributed object info")
	getPayloadMapError            = errors.New("something somewhere went terribly wrong when attempting to get a payload map from the target hazelcast cluster")
	getSyncMapError               = errors.New("something somewhere went terribly wrong when attempting to get a sync map from the target hazelcast cluster")
	mapEvictAllError              = errors.New("something somewhere went terribly wrong upon attempt to perform evict all")
	mapSizeError                  = errors.New("something somewhere went terribly wrong upon attempt to query the map's size")
	getQueueError                 = errors.New("something somewhere went terribly wrong when attempting to get a queue from the target hazelcast cluster")
	queueClearError               = errors.New("something somewhere went terribly wrong upon attempt to perform clear operation on queue")
	queueSizeError                = errors.New("something somewhere went terribly wrong upon attempt to query the queue's size")
	lastCleanedInfoCheckError     = errors.New("something somewhere went terribly wrong upon attempt to check last cleaned info")
	lastCleanedInfoUpdateError    = errors.New("something somewhere went terribly wrong upon attempt to update last cleaned info")
	tryLockError                  = errors.New("something somewhere went terribly wrong upon attempt to acquire a lock")
	getOnMapError                 = errors.New("something somewhere went terribly wrong upon attempt to perform get on a map")
	emptyTestCleanerBehavior      = &testCleanerBehavior{}
)

func (m *testHzMap) EvictAll(_ context.Context) error {

	m.evictAllInvocations++

	if m.returnErrorUponEvictAll {
		return mapEvictAllError
	}

	clear(m.data)

	return nil

}

func (m *testHzMap) Size(_ context.Context) (int, error) {

	m.sizeInvocations++

	if m.returnErrorUponSize {
		return 0, mapSizeError
	}

	return len(m.data), nil

}

func (m *testHzMap) TryLock(_ context.Context, _ any) (bool, error) {

	m.tryLockInvocations++
	if m.returnErrorUponTryLock {
		return false, tryLockError
	}

	return m.tryLockReturnValue, nil

}

func (m *testHzMap) Unlock(_ context.Context, _ any) error {

	m.unlockInvocations++
	if m.returnErrorUponUnlock {
		return errors.New("test error upon Unlock")
	}

	return nil

}

func (m *testHzMap) Get(_ context.Context, payloadDataStructureName any) (any, error) {

	m.getInvocations++
	if m.returnErrorUponGet {
		return nil, getOnMapError
	}

	return m.data[payloadDataStructureName.(string)], nil

}

func (m *testHzMap) Set(_ context.Context, _, _ any) error {

	m.setInvocations++
	if m.returnErrorUponSet {
		return errors.New("test error upon Set")
	}

	return nil

}

func (m *testHzMap) SetWithTTLAndMaxIdle(_ context.Context, _, _ any, _ time.Duration, _ time.Duration) error {

	m.setWithTTLAndMaxIdleInvocations++
	if m.returnErrorUponSetWithTTLAndMaxIdle {
		return errors.New("test error upon SetWithTTLAndMaxIdle")
	}

	return nil

}

func (ms *testHzMapStore) GetMap(_ context.Context, name string) (hazelcastwrapper.Map, error) {

	if name == mapCleanersSyncMapName {
		ms.getMapInvocationsMapsSyncMap++
	} else if name == queueCleanersSyncMapName {
		ms.getMapInvocationsQueueSyncMap++
	} else {
		ms.getMapInvocationsPayloadMap++
	}

	if (name == mapCleanersSyncMapName || name == queueCleanersSyncMapName) && ms.returnErrorUponGetSyncMap {
		return nil, getSyncMapError
	}

	if !(name == mapCleanersSyncMapName || name == queueCleanersSyncMapName) && ms.returnErrorUponGetPayloadMap {
		return nil, getPayloadMapError
	}

	if v, exists := ms.maps[name]; exists {
		return v, nil
	}

	return nil, nil

}

func (q *testHzQueue) Clear(_ context.Context) error {

	q.clearInvocations++

	if q.returnErrorUponClear {
		return queueClearError
	}

	for {
		select {
		case <-q.data:
			// Receive item from channel and discard it
		default:
			// Channel empty, no more events to receive
			return nil
		}
	}

}

func (q *testHzQueue) Size(_ context.Context) (int, error) {

	q.sizeInvocations++

	if q.returnErrorUponSize {
		return 0, queueSizeError
	}

	return len(q.data), nil

}

func (qs *testHzQueueStore) GetQueue(_ context.Context, name string) (hazelcastwrapper.Queue, error) {

	qs.getQueueInvocations++

	if qs.returnErrorUponGetQueue {
		return nil, getQueueError
	}

	if v, exists := qs.queues[name]; exists {
		return v, nil
	}

	return nil, nil

}

func (ois *testHzObjectInfoStore) GetDistributedObjectsInfo(_ context.Context) ([]hazelcastwrapper.ObjectInfo, error) {

	ois.getDistributedObjectInfoInvocations++

	if ois.returnErrorUponGetObjectInfos {
		return nil, getDistributedObjectInfoError
	}

	var hzObjectInfos []hazelcastwrapper.ObjectInfo
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

func (ch *testHzClientHandler) GetClient() *hazelcast.Client {
	return nil
}

func (cw *cleanerWatcher) reset() {
	cw.m = sync.Mutex{}

	cw.cleanSingleInvocations = 0
	cw.cleanAllInvocations = 0
}

func (c *testBatchCleaner) Clean() (int, error) {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanAllInvocations++

	if c.behavior.returnErrorUponClean {
		return 0, batchCleanerCleanError
	}

	return cw.cleanAllInvocations, nil

}

func (c *testSingleCleaner) Clean(_ string) (int, error) {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanSingleInvocations++

	if c.behavior.returnErrorUponClean && (c.behavior.returnCleanErrorAfterNoInvocations == 0 || cw.cleanSingleInvocations == c.behavior.returnCleanErrorAfterNoInvocations+1) {
		return c.behavior.numItemsCleanedReturnValue, singleCleanerCleanError
	}

	return c.behavior.numItemsCleanedReturnValue, nil

}

func (b *testCleanerBuilder) Build(_ hazelcastwrapper.HzClientHandler, _ context.Context, g *status.Gatherer, _ string, _ []string) (BatchCleaner, string, error) {

	b.buildInvocations++
	b.gathererPassedIn = g

	if b.behavior.returnErrorUponBuild {
		return nil, HzMapService, cleanerBuildError
	}

	return &testBatchCleaner{behavior: b.behavior}, HzMapService, nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnErrorUponAssignConfigValue {
		return assignConfigPropertyError
	}

	if value, ok := a.testConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	} else {
		return fmt.Errorf("test error: unable to find value in test config for given key path '%s'", keyPath)
	}

	return nil

}

func (cih *testLastCleanedInfoHandler) check(syncMapName, payloadDataStructureName, _ string) (mapLockInfo, bool, error) {

	cih.checkInvocations++

	if cih.returnErrorUponCheck {
		return emptyMapLockInfo, false, lastCleanedInfoCheckError
	}

	return mapLockInfo{
		m:       cih.syncMap,
		mapName: syncMapName,
		key:     payloadDataStructureName,
	}, cih.shouldCleanAll || cih.shouldCleanIndividualMap[payloadDataStructureName], nil

}

func (cih *testLastCleanedInfoHandler) update(_ mapLockInfo) error {

	cih.updateInvocations++

	if cih.returnErrorUponUpdate {
		return lastCleanedInfoUpdateError
	}

	return nil

}

func (t *testCleanedTracker) add(_ string, _ int) {

	t.numAddInvocations++

}

func TestValidateErrorDuringCleanBehavior(t *testing.T) {

	t.Log("given a value to configure pre-run clean error behavior")
	{
		keyPath := "super.awesome.key.path"
		t.Log("\twhen value is empty string")
		{
			err := ValidateErrorDuringCleanBehavior(keyPath, "")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen value is string representing unknown behavior")
		{
			err := ValidateErrorDuringCleanBehavior(keyPath, "do a backflip")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen string representing valid error behavior is provided")
		{
			for _, v := range []string{string(Ignore), string(Fail)} {

				err := ValidateErrorDuringCleanBehavior(keyPath, v)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}

			}
		}
	}

}

func TestReleaseLock(t *testing.T) {

	t.Log("given a lock info value representing a key in a sync map to release lock for")
	{
		t.Log("\twhen given lock info is empty lock info value")
		{
			err := releaseLock(context.TODO(), emptyMapLockInfo, HzMapService)

			msg := "\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

		}
		t.Log("\twhen unlock yields error")
		{
			li := mapLockInfo{
				m: &testHzMap{
					returnErrorUponUnlock: true,
				},
				mapName: "awesome-map",
				key:     "awesome-key",
			}
			err := releaseLock(context.TODO(), li, HzMapService)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen unlock is successful")
		{
			li := mapLockInfo{
				m:       &testHzMap{},
				mapName: "awesome-map",
				key:     "awesome-key",
			}

			err := releaseLock(context.TODO(), li, HzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
	}

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given configuration to populate state cleaners from")
	{
		t.Log("\twhen assignment operation yields error due to invalid configuration property")
		{
			b := &cleanerConfigBuilder{
				keyPath: cleanerKeyPath,
				a: &testConfigPropertyAssigner{
					returnErrorUponAssignConfigValue: true,
				},
			}

			cfg, err := b.populateConfig()

			msg := "\t\terror must be returned"
			if errors.Is(err, assignConfigPropertyError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig must be nil"
			if cfg == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cfg)
			}
		}

		t.Log("\twhen all assignment operations are successful")
		{
			b := &cleanerConfigBuilder{
				keyPath: cleanerKeyPath,
				a: &testConfigPropertyAssigner{
					testConfig: testConfig,
				},
			}

			cfg, err := b.populateConfig()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			if ok, detail := configValuesAsExpected(cfg, testConfig); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}
	}

}

func TestDefaultLastCleanedInfoHandler_Check(t *testing.T) {

	t.Log("given a map store containing sync map for map cleaners that needs to be checked for last cleaned info on payload map")
	{
		t.Log("\twhen get map on sync map yields error")
		{

			ms := populateTestMapStore(1, []string{"ht_"}, 1)
			ms.returnErrorUponGetSyncMap = true
			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_aragorn-0", HzMapService)

			msg := "\t\tcorrect error must be returned"
			if errors.Is(err, getSyncMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlock info must be empty"
			if lockInfo == emptyMapLockInfo {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tshould check result must be negative"

			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen try lock fails on sync map for key associated with payload data structure name")
		{
			ms := populateTestMapStore(1, []string{"ht_"}, 1)
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.returnErrorUponTryLock = true

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_gimli-0", HzMapService)

			msg := "\t\tcorrect error must be returned"
			if errors.Is(err, tryLockError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlock info must be empty"
			if lockInfo == emptyMapLockInfo {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tshould check result must be negative"

			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of get map invocations on map store for map cleaners sync map must be one"
			if ms.getMapInvocationsMapsSyncMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

			msg = "\t\tnumber of try lock invocations on map cleaners sync map must be one, too"
			if mapCleanersSyncMap.tryLockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.tryLockInvocations)
			}
		}

		t.Log("\twhen try lock operation yields negative result")
		{

			ms := populateTestMapStore(1, []string{"blubbedi_"}, 1)
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = false

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_legolas-0", HzMapService)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be empty"
			if lockInfo == emptyMapLockInfo {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tshould check result must be negative"
			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of get map invocations on map store for map cleaners sync map must be one"
			if ms.getMapInvocationsMapsSyncMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

			msg = "\t\tnumber of try lock invocations on map cleaners sync map must be one, too"
			if mapCleanersSyncMap.tryLockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.tryLockInvocations)
			}

		}

		t.Log("\twhen usage of cleaning threshold has been disabled")
		{
			ms := populateTestMapStore(0, []string{}, 0)
			syncMap := ms.maps[mapCleanersSyncMapName]
			syncMap.tryLockReturnValue = true

			cih := &DefaultLastCleanedInfoHandler{
				Ctx: context.TODO(),
				Ms:  ms,
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: false,
					CleanAgainThresholdMs:  60_000,
				},
			}

			payloadDataStructureName := "awesome-map-name"
			lockInfo, shouldClean, err := cih.check(mapCleanersSyncMapName, payloadDataStructureName, HzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tshould clean result must indicate data structure is susceptible to cleaning"
			if shouldClean {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must contain sync map value"
			if lockInfo.m == syncMap {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must contain correct sync map name"
			if lockInfo.mapName == mapCleanersSyncMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo.mapName)
			}

			msg = "\t\tkey contained in lock info must be equal to payload data structure name"
			if lockInfo.key == payloadDataStructureName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of get map invocations on map store for map cleaners sync map must be one"
			if ms.getMapInvocationsMapsSyncMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

			msg = "\t\tthere must have been no invocation on sync map for key corresponding to payload data structure"
			if ms.getMapInvocationsPayloadMap == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

		}

		t.Log("\twhen get on sync map for key-value pair related to payload data structure yields error")
		{
			prefix := "waldo_"
			ms := populateTestMapStore(1, []string{prefix}, 1)

			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.returnErrorUponGet = true

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
				},
			}

			payloadMapName := prefix + "load-0"
			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, HzMapService)

			msg := "\t\terror must be returned"
			if errors.Is(err, getOnMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.key == payloadMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo)
			}

			msg = "\t\tshould check result must be negative"
			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of get map invocations on map store for map cleaners sync map must be one"
			if ms.getMapInvocationsMapsSyncMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

			msg = "\t\tnumber of try lock invocations on map cleaners sync map must be one, too"
			if mapCleanersSyncMap.tryLockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.tryLockInvocations)
			}

			msg = "\t\tnumber of get invocations on sync map must be one, too"
			if mapCleanersSyncMap.getInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.getInvocations)
			}

		}

		t.Log("\twhen payload map hasn't been cleaned before")
		{
			ms := populateTestMapStore(1, []string{"ht_"}, 1)
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
				},
			}

			payloadMapName := "ht_aragorn-0"
			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, HzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.key == payloadMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo)
			}

			msg = "\t\tshould check result must be positive"
			if shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of get map invocations on map store for map cleaners sync map must be one"
			if ms.getMapInvocationsMapsSyncMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
			}

			msg = "\t\tnumber of try lock invocations on map cleaners sync map must be one, too"
			if mapCleanersSyncMap.tryLockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.tryLockInvocations)
			}

			msg = "\t\tnumber of get calls on map cleaners sync map must be one, too"
			if mapCleanersSyncMap.getInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mapCleanersSyncMap.getInvocations)
			}
		}

		t.Log("\twhen payload map has been cleaned before, but last cleaned state does not represent valid timestamp")
		{
			prefix := "ht_"
			ms := populateTestMapStore(1, []string{prefix}, 1)

			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap.data[payloadMapName] = "clearly not a valid timestamp"

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
				},
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, HzMapService)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.key == payloadMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo)
			}

			msg = "\t\tshould check result must be negative"
			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen payload map has been cleaned before and last cleaned timestamp is within clean interval")
		{
			prefix := "ht_"
			ms := populateTestMapStore(1, []string{prefix}, 1)

			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.data[payloadMapName] = time.Now().UnixNano()

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
					CleanAgainThresholdMs:  30_000,
				},
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, HzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.key == payloadMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo)
			}

			msg = "\t\tshould check result must be negative"
			if !shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen payload map has been cleaned before and last cleaned timestamp is not within clean interval")
		{
			prefix := "ht_"
			ms := populateTestMapStore(1, []string{prefix}, 1)

			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.data[payloadMapName] = int64(0)

			cih := &DefaultLastCleanedInfoHandler{
				Ms:  ms,
				Ctx: context.TODO(),
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
					CleanAgainThresholdMs:  1,
				},
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, HzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.key == payloadMapName {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, lockInfo)
			}

			msg = "\t\tshould check result must be positive"
			if shouldCheck {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestDefaultLastCleanedInfoHandler_Update(t *testing.T) {

	t.Log("given a map store containing sync map for map cleaners that needs to be updated with new last cleaned info")
	{
		t.Log("\twhen unlock operation yields error")
		{
			func() {

				defer func() {
					msg := "\t\tno panic must have been caused"
					if r := recover(); r == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, r)
					}
				}()

				ms := populateTestMapStore(1, []string{}, 1)
				mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
				mapCleanersSyncMap.returnErrorUponUnlock = true

				cih := &DefaultLastCleanedInfoHandler{
					Ms:  ms,
					Ctx: context.TODO(),
					Cfg: &LastCleanedInfoHandlerConfig{
						CleanAgainThresholdMs: 30_000,
					},
				}

				lockInfo := mapLockInfo{
					m:       mapCleanersSyncMap,
					mapName: mapCleanersSyncMapName,
					key:     "ht_load-0",
				}
				err := cih.update(lockInfo)

				msg := "\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

			}()
		}
	}

}

func TestCleanedDataStructureTracker_add(t *testing.T) {

	t.Log("given a status update about a cleaned data structure to be added to the cleaned data structure tracker")
	{
		t.Log("\twhen status gatherer has been correctly populated")
		{
			g := status.NewGatherer()
			go g.Listen()

			tracker := &CleanedDataStructureTracker{g}

			name := "awesome-map"
			size := 9
			tracker.add(name, size)

			g.StopListen()

			waitForStatusGatheringDone(g)

			msg := "\t\tinformation about cleaned data structure must have been added to status gatherer"

			statusCopy := g.AssembleStatusCopy()
			if statusCopy[name].(int) == size {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestIdentifyCandidateDataStructures(t *testing.T) {

	t.Log("given information about data structures stored in hazelcast that need to be checked for whether they are susceptible to getting cleaned")
	{
		valid := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "ht_load-1",
			ServiceName: HzMapService,
		}
		invalidBecauseSystemInternal := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "__sql.catalog",
			ServiceName: HzMapService,
		}
		invalidBecauseRepresentsQueue := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "ht_load-2",
			ServiceName: HzQueueService,
		}

		t.Log("\twhen object info retrieval does not yield error")
		{
			t.Log("\t\twhen object info list contains information on both valid candidates and elements not viable as candidates")
			{
				objectInfos := []hazelcastwrapper.ObjectInfo{valid, invalidBecauseSystemInternal, invalidBecauseRepresentsQueue}
				ois := &testHzObjectInfoStore{
					objectInfos: objectInfos,
				}

				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), HzMapService)

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
					objectInfos: []hazelcastwrapper.ObjectInfo{invalidBecauseSystemInternal, invalidBecauseRepresentsQueue},
				}
				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), HzMapService)

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
					objectInfos: make([]hazelcastwrapper.ObjectInfo, 0),
				}
				candidates, err := identifyCandidateDataStructures(ois, context.TODO(), HzMapService)

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

			candidates, err := identifyCandidateDataStructures(ois, context.TODO(), HzMapService)

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

func TestDefaultSingleQueueCleaner_retrieveAndClean(t *testing.T) {

	t.Log("given a queue in a target hazelcast cluster to be retrieved and cleaned")
	{
		t.Log("\twhen retrieval of queue yields error")
		{
			prefix := "ht_"
			baseName := "tweets"
			qs := populateTestQueueStore(1, []string{prefix}, baseName, 0)
			qs.returnErrorUponGetQueue = true

			qc := &DefaultSingleQueueCleaner{
				ctx: context.TODO(),
				ms:  nil,
				qs:  qs,
				cih: nil,
			}

			numCleanedItems, err := qc.retrieveAndClean(prefix + baseName + "-0")

			msg := "\t\terror must be returned"
			if errors.Is(err, getQueueError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}

			msg = "\t\tget queue must have been invoked once"
			if qs.getQueueInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.getQueueInvocations)
			}

		}
		t.Log("\twhen retrieval of queue is successful, but queue is nil")
		{
			prefix := "ht_"
			qs := populateTestQueueStore(1, []string{prefix}, "load", 0)

			qc := &DefaultSingleQueueCleaner{
				ctx: context.TODO(),
				ms:  nil,
				qs:  qs,
				cih: nil,
			}

			numCleanedItems, err := qc.retrieveAndClean("blubbo")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}
		}
		t.Log("\twhen retrieval of non-nil queue is successful, but size check fails")
		{
			prefix := "ht_"
			baseName := "load"
			qs := populateTestQueueStore(1, []string{prefix}, baseName, 0)

			payloadQueueName := prefix + baseName + "-0"
			payloadQueue := qs.queues[payloadQueueName]
			payloadQueue.returnErrorUponSize = true

			qc := &DefaultSingleQueueCleaner{
				ctx: context.TODO(),
				qs:  qs,
			}

			numCleanedItems, err := qc.retrieveAndClean(payloadQueueName)

			msg := "\t\terror must be returned"
			if errors.Is(err, queueSizeError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}

		}
		t.Log("\twhen retrieval of non-nil queue is successful, but queue clear operation yields error")
		{
			prefix := "ht_"
			baseName := "load"
			qs := populateTestQueueStore(1, []string{prefix}, baseName, 1)

			payloadQueueName := prefix + baseName + "-0"
			payloadQueue := qs.queues[payloadQueueName]
			payloadQueue.returnErrorUponClear = true

			qc := &DefaultSingleQueueCleaner{
				ctx: context.TODO(),
				qs:  qs,
			}

			numCleanedItems, err := qc.retrieveAndClean(payloadQueueName)

			msg := "\t\terror must be returned"

			if errors.Is(err, queueClearError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}

			msg = "\t\tclear on payload queue must have been attempted once"
			if payloadQueue.clearInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, payloadQueue.clearInvocations)
			}
		}
		t.Log("\twhen retrieval of non-nil queue is successful and queue clear operation does not yield error")
		{
			prefix := "ht_"
			baseName := "load"
			numItemsInQueues := 9
			qs := populateTestQueueStore(1, []string{prefix}, baseName, numItemsInQueues)

			qc := &DefaultSingleQueueCleaner{
				ctx: context.TODO(),
				ms:  nil,
				qs:  qs,
				cih: nil,
			}

			payloadQueueName := prefix + baseName + "-0"
			numCleanedItems, err := qc.retrieveAndClean(payloadQueueName)

			payloadQueue := qs.queues[payloadQueueName]

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be equal to number of items previously in queue"
			if numCleanedItems == numItemsInQueues {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}

			msg = "\t\tclear on payload queue must have been performed once"
			if payloadQueue.clearInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, payloadQueue.clearInvocations)
			}
		}
	}

}

func TestDefaultSingleQueueCleaner_Clean(t *testing.T) {

	t.Log("given a specific queue to clean in a target hazelcast cluster")
	{
		t.Log("\twhen single queue cleaner was properly initialized")
		{
			t.Log("\t\twhen operation prior to invoking clean function on data structure yields error")
			{
				b := DefaultSingleQueueCleanerBuilder{}

				ms := populateTestMapStore(0, []string{}, 0)
				syncMap := ms.maps[queueCleanersSyncMapName]

				// This is the default empty value for a bool, but it's specified here explicitly
				// to let the reader know what the error in question is.
				// (The last cleaned info handler will return an error in case acquiring a lock on the sync map
				// was unsuccessful.)
				syncMap.tryLockReturnValue = false

				cih := &DefaultLastCleanedInfoHandler{
					Ms: ms,
				}
				qc, _ := b.Build(context.TODO(), populateTestQueueStore(0, []string{}, "", 0), ms, &testCleanedTracker{}, cih)

				numItemsCleaned, err := qc.Clean("something")

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treported number of cleaned items must be zero"
				if numItemsCleaned == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numItemsCleaned)
				}
			}

			t.Log("\t\twhen all operations performed in scope of cleaning are successful")
			{
				b := DefaultSingleQueueCleanerBuilder{}

				ms := populateTestMapStore(0, []string{}, 0)
				syncMap := ms.maps[queueCleanersSyncMapName]
				syncMap.tryLockReturnValue = true

				prefix := "ht_"
				baseName := "tweets"
				numQueueObjects := 1
				tr := &testCleanedTracker{}
				cih := &DefaultLastCleanedInfoHandler{
					Ms: ms,
					Cfg: &LastCleanedInfoHandlerConfig{
						UseCleanAgainThreshold: true,
						CleanAgainThresholdMs:  30_000,
					},
				}
				numItemsInQueues := 9
				qc, _ := b.Build(context.TODO(), populateTestQueueStore(numQueueObjects, []string{prefix}, baseName, numItemsInQueues), ms, tr, cih)

				numItemsCleaned, err := qc.Clean(prefix + baseName + "-0")

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treported number of items cleaned from queue must be equal to number of items previously held by queue"
				if numItemsCleaned == numItemsInQueues {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treported number of cleaned data structures must be equal to number of queues in queue store susceptible to cleaning"
				if tr.numAddInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}
		}
	}

}

func TestDefaultBatchQueueCleaner_Clean(t *testing.T) {

	t.Log("given a target hazelcast cluster potentially containing queues susceptible to getting cleaned")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues, and all retrieval operations are successful")
		{
			t.Log("\t\twhen prefix usage has been enabled")
			{
				numQueueObjects := 9
				prefixToConsider := "ht_"
				prefixes := []string{prefixToConsider, "aragorn_"}

				baseName := "load"
				testQueueStore := populateTestQueueStore(numQueueObjects, prefixes, baseName, 1)
				testObjectInfoStore := populateTestObjectInfos(numQueueObjects, prefixes, HzQueueService)

				// Add object representing map, so we can verify that no attempt was made to retrieve it
				// The name of this object matches the given predicate, so method under test must use service name to establish
				// object in question represents map
				mapObjectName := fmt.Sprintf("%s%s-42", prefixToConsider, baseName)
				testObjectInfoStore.objectInfos = append(testObjectInfoStore.objectInfos, *newMapObjectInfoFromName(mapObjectName))
				testQueueStore.queues[mapObjectName] = &testHzQueue{}

				// Add Hazelcast-internal map
				hzInternalQueueName := "__awesome.internal.queue"
				testObjectInfoStore.objectInfos = append(testObjectInfoStore.objectInfos, *newQueueObjectInfoFromName(hzInternalQueueName))
				testQueueStore.queues[hzInternalQueueName] = &testHzQueue{}

				c := &cleanerConfig{
					enabled:   true,
					usePrefix: true,
					prefix:    prefixToConsider,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}

				ms := &testHzMapStore{maps: map[string]*testHzMap{
					queueCleanersSyncMapName: {data: make(map[string]any)},
				}}

				ctx := context.TODO()

				// Default last cleaned info handler used in place of test variant for this "happy-path" test
				// in order to increase test integration level by verifying number and kind of invocations performed
				// on the test map store.
				cih := &DefaultLastCleanedInfoHandler{
					Ms:  ms,
					Ctx: ctx,
					Cfg: &LastCleanedInfoHandlerConfig{
						UseCleanAgainThreshold: true,
						CleanAgainThresholdMs:  30_000,
					},
				}
				queueCleanersSyncMap := ms.maps[queueCleanersSyncMapName]
				queueCleanersSyncMap.tryLockReturnValue = true

				qc := assembleBatchQueueCleaner(c, testQueueStore, ms, testObjectInfoStore, ch, cih, tracker)

				numCleaned, err := qc.Clean()

				msg := "\t\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tdistributed objects info must have been queried once"
				if testObjectInfoStore.getDistributedObjectInfoInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tget queue must have been invoked only on queues whose prefix matches configuration"
				if testQueueStore.getQueueInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, testQueueStore.getQueueInvocations)
				}

				msg = "\t\t\tnumber of get map invocations on queue cleaners sync map must be equal to number of payload queues whose name matches given prefix"
				if ms.getMapInvocationsQueueSyncMap == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.getMapInvocationsQueueSyncMap)
				}

				msg = "\t\t\tthere must be no get map invocations on map cleaners sync map"
				if ms.getMapInvocationsMapsSyncMap == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.getMapInvocationsMapsSyncMap)
				}

				sizeInvokedOnceMsg := "\t\t\tsize must have been invoked on all data structures whose prefix matches configuration"
				sizeNotInvokedMsg := "\t\t\tsize must not have been invoked on data structure that is either not a queue or whose name does not correspond to given prefix"

				clearInvokedOnceMsg := "\t\t\tclear must have been invoked on all data structures whose prefix matches configuration"
				clearNotInvokedMsg := "\t\t\tclear must not have been invoked on data structure that is either not a queue or whose name does not correspond to given prefix"

				for k, v := range testQueueStore.queues {
					if strings.HasPrefix(k, prefixToConsider) && resolveObjectKindForNameFromObjectInfoList(k, testObjectInfoStore.objectInfos) == HzQueueService {
						if v.sizeInvocations == 1 {
							t.Log(sizeInvokedOnceMsg, checkMark, k)
						} else {
							t.Fatal(sizeInvokedOnceMsg, ballotX, k, v.sizeInvocations)
						}
						if v.clearInvocations == 1 {
							t.Log(clearInvokedOnceMsg, checkMark, k)
						} else {
							t.Fatal(clearInvokedOnceMsg, ballotX, k, v.clearInvocations)
						}
					} else {
						if v.sizeInvocations == 0 {
							t.Log(sizeNotInvokedMsg, checkMark, k)
						} else {
							t.Fatal(sizeNotInvokedMsg, ballotX, k, v.sizeInvocations)
						}
						if v.clearInvocations == 0 {
							t.Log(clearNotInvokedMsg, checkMark, k)
						} else {
							t.Fatal(clearNotInvokedMsg, ballotX, k, v.clearInvocations)
						}
					}
				}

				msg = "\t\t\tnumber of try lock invocations on queue cleaners sync map must be equal to number of payload queues whose name matches prefix"
				if queueCleanersSyncMap.tryLockInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, queueCleanersSyncMap.tryLockInvocations)
				}

				msg = "\t\t\tnumber of unlock invocations on queue cleaners sync map must be equal to number of payload queues whose name matches prefix"
				if queueCleanersSyncMap.unlockInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, queueCleanersSyncMap.unlockInvocations)
				}

				msg = "\t\t\tnumber of get invocations on queue cleaners sync map must be equal to number of payload queues whose name matches prefix"
				if queueCleanersSyncMap.getInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, queueCleanersSyncMap.getInvocations)
				}

				msg = "\t\t\tnumber of set with ttl and max idle time invocations on queue cleaners sync map must be equal to number of payload queues whose name matches prefix"
				if queueCleanersSyncMap.setWithTTLAndMaxIdleInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, queueCleanersSyncMap.setWithTTLAndMaxIdleInvocations)
				}

				msg = fmt.Sprintf("\t\t\tcleaner must report %d cleaned data structures", numQueueObjects)
				if numCleaned == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numCleaned)
				}

				msg = fmt.Sprintf("\t\t\tcleaned data structure tracker must have been invoked %d times", numQueueObjects)
				if tracker.numAddInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numAddInvocations)
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
				numQueueObjects := 9
				prefixes := []string{"ht_", "gimli_"}

				baseName := "load"
				qs := populateTestQueueStore(numQueueObjects, prefixes, baseName, 1)
				ms := populateTestMapStore(0, prefixes, 0)
				queuesSyncMap := ms.maps[queueCleanersSyncMapName]
				queuesSyncMap.tryLockReturnValue = true

				ois := populateTestObjectInfos(numQueueObjects, prefixes, HzQueueService)

				// Add Hazelcast-internal map to make sure cleaner does not consider such maps
				// even when prefix usage has been disabled
				hzInternalQueueName := "__awesome.internal.queue"
				ois.objectInfos = append(ois.objectInfos, *newQueueObjectInfoFromName(hzInternalQueueName))
				qs.queues[hzInternalQueueName] = &testHzQueue{}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}

				cih := &testLastCleanedInfoHandler{
					syncMap:        queuesSyncMap,
					shouldCleanAll: true,
				}
				qc := assembleBatchQueueCleaner(c, qs, ms, ois, ch, cih, tracker)

				numCleaned, err := qc.Clean()

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tget all must have been invoked on all maps that are not hazelcast-internal maps"
				expectedCleaned := numQueueObjects * len(prefixes)
				if qs.getQueueInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, qs.getQueueInvocations)
				}

				invokedMsg := "\t\t\tclear must have been invoked on all queues that are not hazelcast-internal queues"
				notInvokedMsg := "\t\t\tclear must not have been invoked on hazelcast-internal queues"
				for k, v := range qs.queues {
					if !strings.HasPrefix(k, hzInternalQueueName) {
						if v.clearInvocations == 1 {
							t.Log(invokedMsg, checkMark, k)
						} else {
							t.Fatal(invokedMsg, ballotX, k)
						}
					} else {
						if v.clearInvocations == 0 {
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

				msg = fmt.Sprintf("\t\t\ttracker must have been invoked %d times", expectedCleaned)
				if tracker.numAddInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numAddInvocations)
				}

			}

		}
		t.Log("\twhen target hazelcast cluster does not contain any queues")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			qs := &testHzQueueStore{queues: make(map[string]*testHzQueue)}
			ois := &testHzObjectInfoStore{
				objectInfos:                         make([]hazelcastwrapper.ObjectInfo, 0),
				getDistributedObjectInfoInvocations: 0,
			}

			tracker := &testCleanedTracker{}

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			qc := assembleBatchQueueCleaner(c, qs, &testHzMapStore{}, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.Clean()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tno get queue operations must have been performed"
			if qs.getQueueInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.getQueueInvocations)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
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
			tracker := &testCleanedTracker{}

			ms := &testHzMapStore{maps: map[string]*testHzMap{
				queueCleanersSyncMapName: {data: make(map[string]any)},
			}}
			qc := assembleBatchQueueCleaner(c, &testHzQueueStore{}, ms, ois, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, tracker)

			numCleaned, err := qc.Clean()

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

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
		t.Log("\twhen retrieval of object info succeeds, but get queue operation fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numPayloadQueueObjects := 9
			prefixes := []string{"ht_"}

			qs := populateTestQueueStore(numPayloadQueueObjects, prefixes, "load", 1)
			qs.returnErrorUponGetQueue = true

			ois := populateTestObjectInfos(numPayloadQueueObjects, prefixes, HzQueueService)

			tracker := &testCleanedTracker{}

			ms := populateTestMapStore(0, prefixes, 0)
			queuesSyncMap := ms.maps[queueCleanersSyncMapName]
			queuesSyncMap.tryLockReturnValue = true

			cih := &testLastCleanedInfoHandler{
				syncMap:        queuesSyncMap,
				shouldCleanAll: true,
			}

			qc := assembleBatchQueueCleaner(c, qs, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.Clean()

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, getQueueError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tthere must have been only one get queue invocation"
			if qs.getQueueInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.getQueueInvocations)
			}

			msg = "\t\tthere must have been no clear invocations on any queue"
			for k, v := range qs.queues {
				if v.clearInvocations == 0 {
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

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
		t.Log("\twhen info retrieval and get queue operations succeed, but clear operation fails")
		{
			numQueueObjects := 9
			prefixes := []string{"ht_"}

			baseName := "load"
			qs := populateTestQueueStore(numQueueObjects, prefixes, baseName, 1)
			ois := populateTestObjectInfos(numQueueObjects, prefixes, HzQueueService)

			erroneousClearQueueName := prefixes[0] + baseName + "-0"
			qs.queues[erroneousClearQueueName].returnErrorUponClear = true

			c := &cleanerConfig{
				enabled: true,
			}

			tracker := &testCleanedTracker{}

			ms := populateTestMapStore(0, prefixes, 0)
			queuesSyncMap := ms.maps[queueCleanersSyncMapName]
			queuesSyncMap.tryLockReturnValue = true

			cih := &testLastCleanedInfoHandler{
				syncMap:        queuesSyncMap,
				shouldCleanAll: true,
			}

			qc := assembleBatchQueueCleaner(c, qs, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.Clean()

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, queueClearError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tthere must have been only one get queue invocation"
			if qs.getQueueInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.getQueueInvocations)
			}

			invokedOnceMsg := "\t\tthere must have been only one invocation of clear"
			notInvokedMsg := "\t\tno clear invocation must have been performed"
			for k, v := range qs.queues {
				if k == erroneousClearQueueName {
					if v.clearInvocations == 1 {
						t.Log(invokedOnceMsg, checkMark, k)
					} else {
						t.Fatal(invokedOnceMsg, ballotX, k)
					}
				} else {
					if v.clearInvocations == 0 {
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

			msg = "\t\tcleaned tracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}

		}
		t.Log("\twhen cleaner has not been enabled")
		{
			c := &cleanerConfig{
				enabled: false,
			}
			qs := &testHzQueueStore{}
			ms := &testHzMapStore{maps: map[string]*testHzMap{
				queueCleanersSyncMapName: {data: make(map[string]any)},
			}}
			ois := &testHzObjectInfoStore{}
			ch := &testHzClientHandler{}

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			qc := assembleBatchQueueCleaner(c, qs, ms, ois, ch, cih, tracker)

			numCleaned, err := qc.Clean()

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

			msg = "\t\tno queue retrieval must have been attempted"
			if qs.getQueueInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.getQueueInvocations)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
	}

}

func TestRunGenericSingleClean(t *testing.T) {

	t.Log("given a specific payload data structure in a target Hazelcast cluster")
	{
		t.Log("\twhen should clean check yields error")
		{
			ms := populateTestMapStore(1, []string{}, 1)
			cih := &testLastCleanedInfoHandler{
				returnErrorUponCheck: true,
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			payloadMapName := "ht_darthvader"
			numItemsCleaned, err := runGenericSingleClean(mc.ctx, mc.cih, mc.t, mapCleanersSyncMapName, payloadMapName, HzMapService, mc.retrieveAndClean)

			msg := "\t\tcorrect error must be returned"
			if errors.Is(err, lastCleanedInfoCheckError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tlast cleaned info check must have been invoked once"
			if cih.checkInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.checkInvocations)
			}

			msg = "\t\tlast cleaned info update must not have been invoked"
			if cih.updateInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be zero"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}
		}

		t.Log("\twhen should clean check is successful and returns negative result")
		{
			mapPrefix := "ht_"
			ms := populateTestMapStore(1, []string{mapPrefix}, 1)

			payloadMapName := mapPrefix + "load-0"

			b := DefaultSingleMapCleanerBuilder{}
			syncMap := ms.maps[mapCleanersSyncMapName]
			syncMap.data[payloadMapName] = time.Now().UnixNano()
			syncMap.tryLockReturnValue = true

			// Use builder this time to check proper lock and unlock behavior on sync map based
			// on DefaultLastCleanedInfoHandler embedded in built map cleaner
			tr := &testCleanedTracker{}
			cih := &DefaultLastCleanedInfoHandler{
				Ms: ms,
				Cfg: &LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: true,
					CleanAgainThresholdMs:  30_000,
				},
			}
			mc, _ := b.Build(context.TODO(), ms, tr, cih)
			dmc := mc.(*DefaultSingleMapCleaner)

			numItemsCleaned, err := runGenericSingleClean(dmc.ctx, dmc.cih, tr, mapCleanersSyncMapName, payloadMapName, HzMapService, dmc.retrieveAndClean)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\ttry lock must have been invoked once on map cleaners sync map"

			if syncMap.tryLockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.tryLockInvocations)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be zero"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}

		}

		t.Log("\twhen should clean check is successful and returns positive result, but get map on payload map yields error")
		{

			mapPrefix := "ht_"
			ms := populateTestMapStore(1, []string{mapPrefix}, 1)
			ms.returnErrorUponGetPayloadMap = true

			payloadMapName := mapPrefix + "load-0"
			cih := &testLastCleanedInfoHandler{
				syncMap: ms.maps[mapCleanersSyncMapName],
				shouldCleanIndividualMap: map[string]bool{
					payloadMapName: true,
				},
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := runGenericSingleClean(mc.ctx, mc.cih, mc.t, mapCleanersSyncMapName, payloadMapName, HzMapService, mc.retrieveAndClean)

			msg := "\t\terror must be returned"
			if errors.Is(err, getPayloadMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			syncMap := ms.maps[mapCleanersSyncMapName]

			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\tno last cleaned info update must have been performed"
			if cih.updateInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be zero"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}

		}

		t.Log("\twhen should clean is successful and returns positive result, and get map for payload map is successful, but size check returns error")
		{
			mapPrefix := "ht_"
			ms := populateTestMapStore(1, []string{mapPrefix}, 1)

			payloadMapName := mapPrefix + "load-0"
			payloadMap := ms.maps[payloadMapName]
			payloadMap.returnErrorUponSize = true

			syncMap := ms.maps[mapCleanersSyncMapName]
			cih := &testLastCleanedInfoHandler{
				syncMap: syncMap,
				shouldCleanIndividualMap: map[string]bool{
					payloadMapName: true,
				},
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := mc.Clean(payloadMapName)

			msg := "\t\terror must be returned"
			if errors.Is(err, mapSizeError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of items cleaned must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tno last cleaned info update must have been performed"
			if cih.updateInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be zero"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

		}

		t.Log("\twhen should clean is successful and returns positive result, and get map for payload map is successful, too, but evict all on payload map yields error")
		{
			mapPrefix := "ht_"
			ms := populateTestMapStore(1, []string{mapPrefix}, 1)

			payloadMapName := mapPrefix + "load-0"
			payloadMap := ms.maps[payloadMapName]
			payloadMap.returnErrorUponEvictAll = true

			cih := &testLastCleanedInfoHandler{
				syncMap: ms.maps[mapCleanersSyncMapName],
				shouldCleanIndividualMap: map[string]bool{
					payloadMapName: true,
				},
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := runGenericSingleClean(mc.ctx, mc.cih, mc.t, mapCleanersSyncMapName, payloadMapName, HzMapService, mc.retrieveAndClean)

			msg := "\t\terror must be returned"
			if errors.Is(err, mapEvictAllError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tevict all must have been invoked once on payload map"
			if payloadMap.evictAllInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, payloadMap.evictAllInvocations)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			syncMap := ms.maps[mapCleanersSyncMapName]

			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\tno last cleaned info update must have been performed"
			if cih.updateInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be zero"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}

		}

		t.Log("\twhen should clean is successful and returns positive result, and both get map and query size operation on payload map are successful, but map contains zero items")
		{
			mapPrefix := "ht_"
			ms := populateTestMapStore(1, []string{mapPrefix}, 0)

			syncMap := ms.maps[mapCleanersSyncMapName]
			cih := &testLastCleanedInfoHandler{
				syncMap:        syncMap,
				shouldCleanAll: true,
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := mc.Clean(mapPrefix + "load-0")

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numItemsCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tdata structure must not have been added to cleaned data structure tracker"
			if tr.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\tlast cleaned info must have been updated anyway"
			if cih.updateInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

		}

		t.Log("\twhen should clean is successful and returns positive result, and get map for payload map, size check, and evict all on payload map are successful, but last cleaned info update fails")
		{
			mapPrefix := "ht_"
			numItemsInPayloadMaps := 6
			ms := populateTestMapStore(1, []string{mapPrefix}, numItemsInPayloadMaps)

			payloadMapName := mapPrefix + "load-0"
			cih := &testLastCleanedInfoHandler{
				syncMap: ms.maps[mapCleanersSyncMapName],
				shouldCleanIndividualMap: map[string]bool{
					payloadMapName: true,
				},
				returnErrorUponUpdate: true,
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := runGenericSingleClean(mc.ctx, mc.cih, mc.t, mapCleanersSyncMapName, payloadMapName, HzMapService, mc.retrieveAndClean)

			msg := "\t\terror must be returned"
			if errors.Is(err, lastCleanedInfoUpdateError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be equal to number of items previously held by payload map"
			if numItemsCleaned == numItemsInPayloadMaps {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			syncMap := ms.maps[mapCleanersSyncMapName]

			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\tlast cleaned info update must have been invoked once"
			if cih.updateInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be one"
			if tr.numAddInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}
		}

		t.Log("\twhen should clean is successful and returns positive result, and get map for payload map, size check, and evict all on payload map are successful, and last cleaned info update is successful, too")
		{
			mapPrefix := "ht_"
			numItemsInPayloadMaps := 6
			ms := populateTestMapStore(1, []string{mapPrefix}, numItemsInPayloadMaps)

			payloadMapName := mapPrefix + "load-0"
			cih := &testLastCleanedInfoHandler{
				syncMap: ms.maps[mapCleanersSyncMapName],
				shouldCleanIndividualMap: map[string]bool{
					payloadMapName: true,
				},
			}
			tr := &testCleanedTracker{}
			mc := DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: cih,
				t:   tr,
			}

			numItemsCleaned, err := runGenericSingleClean(mc.ctx, mc.cih, mc.t, mapCleanersSyncMapName, payloadMapName, HzMapService, mc.retrieveAndClean)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be equal to number of items previously held by payload map"
			if numItemsCleaned == numItemsInPayloadMaps {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numItemsCleaned)
			}

			msg = "\t\tunlock must have been invoked once on map cleaners sync map"
			syncMap := ms.maps[mapCleanersSyncMapName]

			if syncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, syncMap.unlockInvocations)
			}

			msg = "\t\tlast cleaned info update must have been invoked once"
			if cih.updateInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cih.updateInvocations)
			}

			msg = "\t\treported number of cleaned data structures must be one"
			if tr.numAddInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tr.numAddInvocations)
			}
		}

		t.Log("\twhen attempt to release lock yields error")
		{
			func() {
				defer func() {
					msg := "\t\tno panic must have occurred"
					if r := recover(); r == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, r)
					}
				}()

				mapPrefix := "ht_"
				numItemsInPayloadMaps := 3
				ms := populateTestMapStore(1, []string{mapPrefix}, numItemsInPayloadMaps)
				syncMap := ms.maps[mapCleanersSyncMapName]
				syncMap.tryLockReturnValue = true
				syncMap.returnErrorUponUnlock = true

				builder := DefaultSingleMapCleanerBuilder{}
				cih := &DefaultLastCleanedInfoHandler{
					Ms:  ms,
					Cfg: &LastCleanedInfoHandlerConfig{},
				}
				mc, _ := builder.Build(context.TODO(), ms, &testCleanedTracker{}, cih)

				dmc := mc.(*DefaultSingleMapCleaner)

				numCleanedItems, err := runGenericSingleClean(dmc.ctx, dmc.cih, dmc.t, mapCleanersSyncMapName, mapPrefix+"load-0", HzMapService, dmc.retrieveAndClean)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\tnumber of cleaned items must be reported correctly anyway"
				if numCleanedItems == numItemsInPayloadMaps {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numCleanedItems)
				}
			}()

		}
	}

}

func TestRunGenericBatchClean(t *testing.T) {

	t.Log("given at least one payload data structure to be cleaned in a target Hazelcast cluster")
	{
		t.Log("\twhen identifying target data structures yields error")
		{
			runTestCaseAndResetState(func() {
				ois := populateTestObjectInfos(0, []string{}, HzMapService)
				ois.returnErrorUponGetObjectInfos = true

				sc := &testSingleCleaner{
					cfg: &cleanerConfig{},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\terror must be returned"
				if errors.Is(err, getDistributedObjectInfoError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\treported number of cleaned data structures must be zero"
				if numMapsCleaned == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}

				msg = "\t\tsingle data structure cleaner must not have been invoked"
				if cw.cleanSingleInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
				}
			})
		}

		t.Log("\twhen zero target data structures were identified")
		{
			runTestCaseAndResetState(func() {
				ois := populateTestObjectInfos(0, []string{}, HzMapService)

				sc := &testSingleCleaner{
					cfg: &cleanerConfig{},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\treported number of cleaned data structures must be zero"
				if numMapsCleaned == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}

				msg = "\t\tsingle data structure cleaner must not have been invoked"
				if cw.cleanSingleInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
				}
			})
		}

		t.Log("\twhen prefix usage was disabled")
		{
			runTestCaseAndResetState(func() {
				numObjectsPerPrefix := 9
				prefixes := []string{"ht_", "somethingcompletelydifferent_"}
				ois := populateTestObjectInfos(numObjectsPerPrefix, prefixes, HzMapService)

				sc := &testSingleCleaner{
					behavior: &testCleanerBehavior{
						numItemsCleanedReturnValue: 1,
					},
					cfg: &cleanerConfig{},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\treported number of maps cleaned must be equal to number of objects in object info store"
				if numMapsCleaned == numObjectsPerPrefix*len(prefixes) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}
			})
		}

		t.Log("\twhen prefix usage was enabled")
		{
			runTestCaseAndResetState(func() {
				numObjectsPerPrefix := 21
				prefixToConsider := "ht_"

				ois := populateTestObjectInfos(numObjectsPerPrefix, []string{prefixToConsider, "somethingcompletelydifferent_"}, HzMapService)

				sc := &testSingleCleaner{
					behavior: &testCleanerBehavior{
						numItemsCleanedReturnValue: 1,
					},
					cfg: &cleanerConfig{
						usePrefix: true,
						prefix:    prefixToConsider,
					},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\treported number of maps cleaned must be equal to number of maps having the given prefix"
				if numMapsCleaned == numObjectsPerPrefix {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}
			})
		}

		t.Log("\twhen invocation of clean for individual data structure yields error and error behavior advises error to be ignored")
		{
			t.Log("\t\twhen single cleaner reports zero items have been removed")
			{
				runTestCaseAndResetState(func() {
					numObjectsPerPrefix := 12
					prefixes := []string{"ht_"}
					ois := populateTestObjectInfos(numObjectsPerPrefix, prefixes, HzMapService)

					sc := &testSingleCleaner{
						behavior: &testCleanerBehavior{
							returnErrorUponClean: true,
						},
						cfg: &cleanerConfig{
							errorBehavior: Ignore,
						},
					}

					numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\treported number of maps cleaned must be zero"
					if numMapsCleaned == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, numMapsCleaned)
					}

					msg = "\t\t\tsingle cleaner clean must have been invoked for all data structures"
					if cw.cleanSingleInvocations == numObjectsPerPrefix*len(prefixes) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
					}
				})
			}
			t.Log("\t\twhen single cleaner reports number of cleaned items is greater than zero despite error")
			{
				runTestCaseAndResetState(func() {
					numObjectsPerPrefix := 12
					prefixes := []string{"ht_"}
					ois := populateTestObjectInfos(numObjectsPerPrefix, prefixes, HzMapService)

					sc := &testSingleCleaner{
						behavior: &testCleanerBehavior{
							numItemsCleanedReturnValue: 9,
							returnErrorUponClean:       true,
						},
						cfg: &cleanerConfig{
							errorBehavior: Ignore,
						},
					}

					numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\treported number of maps cleaned must be equal to number of map entries contained in object info store"
					if numMapsCleaned == numObjectsPerPrefix*len(prefixes) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, numMapsCleaned)
					}

					msg = "\t\t\tsingle cleaner clean must have been invoked for all data structures"
					if cw.cleanSingleInvocations == numObjectsPerPrefix*len(prefixes) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
					}
				})
			}
		}

		t.Log("\twhen invocation of clean for individual data structure yields error and error behavior advises error not to be ignored")
		{
			runTestCaseAndResetState(func() {
				numObjectsPerPrefix := 9
				ois := populateTestObjectInfos(numObjectsPerPrefix, []string{"ht_"}, HzMapService)

				cleanErrorAfterNoInvocations := 3
				sc := &testSingleCleaner{
					behavior: &testCleanerBehavior{
						numItemsCleanedReturnValue:         1,
						returnCleanErrorAfterNoInvocations: 3,
						returnErrorUponClean:               true,
					},
					cfg: &cleanerConfig{
						errorBehavior: Fail,
					},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\terror must be returned"
				if errors.Is(err, singleCleanerCleanError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				// All invocations, including the erroneous one, will return 1, signalling that one item has been cleaned
				// from the target data structure. Hence, even in the face of an error, the function under test
				// will count the data structure whose clean invocation yielded an error as a cleaned data structure,
				// as one item was reported to be cleaned.
				msg = "\t\treported number of maps cleaned must be equal to single clean operations successful prior to error plus one"
				if numMapsCleaned == cleanErrorAfterNoInvocations+1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}

				msg = "\t\tsingle cleaner clean must have been invoked one time more often than number of successful clean operations"
				if cw.cleanSingleInvocations == cleanErrorAfterNoInvocations+1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
				}
			})

		}
		t.Log("\twhen invocation of clean for individual data structure succeeds, but data data structure held no items")
		{
			runTestCaseAndResetState(func() {
				numObjectsPerPrefix := 99
				prefixes := []string{"ht_"}
				ois := populateTestObjectInfos(numObjectsPerPrefix, prefixes, HzMapService)

				sc := &testSingleCleaner{
					behavior: &testCleanerBehavior{
						// This is the empty value found on emptyTestCleanerBehavior -- explicitly provided here
						// anyway to convey why we expect zero maps reported to be cleaned
						numItemsCleanedReturnValue: 0,
					},
					cfg: &cleanerConfig{},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\treported number of maps cleaned must be zero"
				if numMapsCleaned == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}

				msg = "\t\tsingle cleaner clean must have been invoked for all data structures anyway"
				if cw.cleanSingleInvocations == numObjectsPerPrefix*len(prefixes) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
				}
			})
		}

		t.Log("\twhen invocation of clean for individual data structure succeeds and data structure held at least one item")
		{
			runTestCaseAndResetState(func() {
				numObjectsPerPrefix := 12
				prefixes := []string{"ht_"}
				ois := populateTestObjectInfos(numObjectsPerPrefix, prefixes, HzMapService)

				sc := &testSingleCleaner{
					behavior: &testCleanerBehavior{
						numItemsCleanedReturnValue: 1,
					},
					cfg: &cleanerConfig{},
				}

				numMapsCleaned, err := runGenericBatchClean(context.TODO(), ois, HzMapService, sc.cfg, sc)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\treported number of maps cleaned must be equal to number of map entries contained in object info store"
				if numMapsCleaned == numObjectsPerPrefix*len(prefixes) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numMapsCleaned)
				}

				msg = "\t\tsingle cleaner clean must have been invoked for all data structures"
				if cw.cleanSingleInvocations == numObjectsPerPrefix*len(prefixes) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cw.cleanSingleInvocations)
				}
			})
		}

	}

}

func TestDefaultSingleMapCleaner_retrieveAndClean(t *testing.T) {

	t.Log("given a map in a target Hazelcast cluster to be retrieved and cleaned")
	{
		t.Log("\twhen get payload map is unsuccessful")
		{
			prefix := "ht_"
			ms := populateTestMapStore(1, []string{prefix}, 1)
			ms.returnErrorUponGetPayloadMap = true

			mc := &DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: nil,
			}

			numCleanedItems, err := mc.retrieveAndClean(prefix + "load-0")

			msg := "\t\terror must be returned"
			if errors.Is(err, getPayloadMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tget map must have been invoked once"
			if ms.getMapInvocationsPayloadMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsPayloadMap)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}
		}

		t.Log("\twhen get payload map itself is successful, but map is nil")
		{
			ms := populateTestMapStore(0, []string{}, 0)

			mc := &DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: nil,
			}

			numCleanedItems, err := mc.retrieveAndClean("blubbi")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}
		}

		t.Log("\twhen get payload map is successful and retrieved map is non-nil, but evict yields error")
		{

			prefix := "ht_"
			ms := populateTestMapStore(1, []string{prefix}, 1)

			payloadMapName := prefix + "load-0"
			payloadMap := ms.maps[payloadMapName]
			payloadMap.returnErrorUponEvictAll = true

			mc := &DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: nil,
			}

			numCleanedItems, err := mc.retrieveAndClean(payloadMapName)

			msg := "\t\terror must be returned"
			if errors.Is(err, mapEvictAllError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tevict all on payload map must have been attempted once"
			if payloadMap.evictAllInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be zero"
			if numCleanedItems == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}
		}

		t.Log("\twhen get payload map is successful, retrieved map is non-nil, and evict does not yield error")
		{

			prefix := "ht_"
			numItemsInPayloadMaps := 9
			ms := populateTestMapStore(1, []string{prefix}, numItemsInPayloadMaps)

			mc := &DefaultSingleMapCleaner{
				ctx: context.TODO(),
				ms:  ms,
				cih: nil,
			}

			payloadMapName := prefix + "load-0"
			numCleanedItems, err := mc.retrieveAndClean(payloadMapName)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			payloadMap := ms.maps[payloadMapName]

			msg = "\t\tevict all on payload map must have been attempted once"
			if payloadMap.evictAllInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of cleaned items must be equal to number of items map previously held"
			if numCleanedItems == numItemsInPayloadMaps {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleanedItems)
			}

		}

	}
}

func TestDefaultSingleMapCleaner_Clean(t *testing.T) {

	t.Log("given a specific map to clean in a target Hazelcast cluster")
	{
		t.Log("\twhen single map cleaner was properly initialized")
		{
			t.Log("\t\twhen operation prior to invoking clean function on data structure yields error")
			{
				builder := DefaultSingleMapCleanerBuilder{}

				prefix := "ht_"
				ms := populateTestMapStore(1, []string{prefix}, 1)
				syncMap := ms.maps[mapCleanersSyncMapName]

				// This is the default empty value for a bool anyway, but it's specified here explicitly
				// to let the reader know what the error in question is.
				// (The last cleaned info handler will return an error in case acquiring a lock on the sync map
				// was unsuccessful.)
				syncMap.tryLockReturnValue = false

				cih := &DefaultLastCleanedInfoHandler{
					Ms: ms,
				}
				mc, _ := builder.Build(context.TODO(), ms, &testCleanedTracker{}, cih)

				numItemsCleaned, err := mc.Clean(prefix + "load-0")

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treported number of cleaned items must be zero"
				if numItemsCleaned == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numItemsCleaned)
				}

			}

			t.Log("\t\twhen all operations performed in scope of cleaning are successful")
			{
				builder := DefaultSingleMapCleanerBuilder{}

				prefix := "ht_"
				numItemsInPayloadMaps := 9
				ms := populateTestMapStore(1, []string{prefix}, numItemsInPayloadMaps)
				syncMap := ms.maps[mapCleanersSyncMapName]
				syncMap.tryLockReturnValue = true

				tr := &testCleanedTracker{}
				cih := &DefaultLastCleanedInfoHandler{
					Ms:  ms,
					Cfg: &LastCleanedInfoHandlerConfig{},
				}
				mc, _ := builder.Build(context.TODO(), ms, tr, cih)

				numItemsCleaned, err := mc.Clean(prefix + "load-0")

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\treported number of cleaned items must be equal to number of items previously held by payload data structure"
				if numItemsCleaned == numItemsInPayloadMaps {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numItemsCleaned)
				}

				msg = "\t\t\tinformation on one cleaned data structure must have been added to cleaned data structures tracker"
				if tr.numAddInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tr.numAddInvocations)
				}

			}
		}
	}

}

func TestDefaultBatchMapCleaner_Clean(t *testing.T) {

	t.Log("given a target hazelcast cluster potentially containing maps susceptible to getting cleaned")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues, and all retrieval operations are successful")
		{
			t.Log("\t\twhen prefix usage has been enabled")
			{
				numMapObjects := 9
				prefixToConsider := "ht_"
				prefixes := []string{prefixToConsider, "gimli_"}

				testMapStore := populateTestMapStore(numMapObjects, prefixes, 1)
				testObjectInfoStore := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

				// Add object representing queue, so we can verify that no attempt was made to retrieve it
				// The name of this object matches the given predicate, so method under test must use service name to establish
				// object in question represents queue
				queueObjectName := fmt.Sprintf("%sload-42", prefixToConsider)
				testObjectInfoStore.objectInfos = append(testObjectInfoStore.objectInfos, *newQueueObjectInfoFromName(queueObjectName))
				testMapStore.maps[queueObjectName] = &testHzMap{data: make(map[string]any)}

				// Add Hazelcast-internal map
				hzInternalMapName := "__sql.catalog"
				testObjectInfoStore.objectInfos = append(testObjectInfoStore.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				testMapStore.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled:   true,
					usePrefix: true,
					prefix:    prefixToConsider,
				}
				ch := &testHzClientHandler{}

				ctx := context.TODO()
				// Default last cleaned info handler used in place of test variant for this "happy-path" test
				// in order to increase test integration level by verifying number and kind of invocations performed
				// on the test map store.
				cih := &DefaultLastCleanedInfoHandler{
					Ms:  testMapStore,
					Ctx: ctx,
					Cfg: &LastCleanedInfoHandlerConfig{
						UseCleanAgainThreshold: true,
						CleanAgainThresholdMs:  30_000,
					},
				}

				tracker := &testCleanedTracker{}
				mc := assembleBatchMapCleaner(c, testMapStore, testObjectInfoStore, ch, cih, tracker)

				mapCleanersSyncMap := testMapStore.maps[mapCleanersSyncMapName]
				mapCleanersSyncMap.tryLockReturnValue = true

				numCleaned, err := mc.Clean()

				msg := "\t\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tdistributed objects info must have been queried once"
				if testObjectInfoStore.getDistributedObjectInfoInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tget map must have been invoked only on payload maps whose prefix matches configuration"
				if testMapStore.getMapInvocationsPayloadMap == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, testMapStore.getMapInvocationsPayloadMap)
				}

				// Sync map proxy value now passed along with map lock info value, hence has to be queried only once
				// per payload map
				msg = "\t\t\tnumber of get map invocations on map cleaners sync map must be equal to number of payload maps whose name matches prefix"
				if testMapStore.getMapInvocationsMapsSyncMap == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, testMapStore.getMapInvocationsMapsSyncMap)
				}

				msg = "\t\t\tthere must be no get map invocations on queue cleaners sync map"
				if testMapStore.getMapInvocationsQueueSyncMap == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, testMapStore.getMapInvocationsQueueSyncMap)
				}

				invokedOnceMsg := "\t\t\tevict all must have been invoked on all maps whose prefix matches configuration"
				notInvokedMsg := "\t\t\tevict all must not have been invoked on data structure that is either not a map or whose name does not correspond to given prefix"
				for k, v := range testMapStore.maps {
					if strings.HasPrefix(k, prefixToConsider) && resolveObjectKindForNameFromObjectInfoList(k, testObjectInfoStore.objectInfos) == HzMapService {
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

				msg = "\t\t\tnumber of try lock invocations on map cleaners sync map must be equal to number of payload maps whose name matches prefix"
				if mapCleanersSyncMap.tryLockInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, mapCleanersSyncMap.tryLockInvocations)
				}

				msg = "\t\t\tnumber of unlock invocations on map cleaners sync map must be equal to number of payload maps whose name matches prefix"
				if mapCleanersSyncMap.unlockInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, mapCleanersSyncMap.unlockInvocations)
				}

				msg = "\t\t\tnumber of get invocations on map cleaners sync map must be equal to number of payload maps whose name matches prefix"
				if mapCleanersSyncMap.getInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, mapCleanersSyncMap.getInvocations)
				}

				msg = "\t\t\tnumber of set with ttl and max idle time invocations on map cleaners sync map must be equal to number of payload maps whose name matches prefix"
				if mapCleanersSyncMap.setWithTTLAndMaxIdleInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, mapCleanersSyncMap.setWithTTLAndMaxIdleInvocations)
				}

				msg = fmt.Sprintf("\t\t\tcleaner must report %d cleaned data structures", numMapObjects)
				if numCleaned == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, numCleaned)
				}

				msg = fmt.Sprintf("\t\t\ttracker must have been invoked %d times", numMapObjects)
				if tracker.numAddInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numAddInvocations)
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

				ms := populateTestMapStore(numMapObjects, prefixes, 1)
				mapsSyncMap := ms.maps[mapCleanersSyncMapName]
				mapsSyncMap.tryLockReturnValue = true

				ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

				// Add Hazelcast-internal map to make sure cleaner does not consider such maps
				// even when prefix usage has been disabled
				hzInternalMapName := hzInternalDataStructurePrefix + "sql.catalog"
				ois.objectInfos = append(ois.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				ms.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}

				cih := &testLastCleanedInfoHandler{
					syncMap:        mapsSyncMap,
					shouldCleanAll: true,
				}
				mc := assembleBatchMapCleaner(c, ms, ois, ch, cih, tracker)

				numCleaned, err := mc.Clean()

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tget all must have been invoked on all maps that are not hazelcast-internal maps"
				expectedCleaned := numMapObjects * len(prefixes)
				if ms.getMapInvocationsPayloadMap == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.getMapInvocationsPayloadMap)
				}

				invokedMsg := "\t\t\tevict all must have been invoked on all maps that are not hazelcast-internal maps"
				notInvokedMsg := "\t\t\tevict all must not have been invoked on hazelcast-internal maps"
				for k, v := range ms.maps {
					if !strings.HasPrefix(k, hzInternalDataStructurePrefix) {
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

				msg = fmt.Sprintf("\t\t\ttracker must have been invoked %d times", expectedCleaned)
				if tracker.numAddInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numAddInvocations)
				}

			}

		}
		t.Log("\twhen target hazelcast cluster does not contain any maps")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			ms := &testHzMapStore{
				maps:                        make(map[string]*testHzMap),
				getMapInvocationsPayloadMap: 0,
			}
			ois := &testHzObjectInfoStore{
				objectInfos:                         make([]hazelcastwrapper.ObjectInfo, 0),
				getDistributedObjectInfoInvocations: 0,
			}
			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.Clean()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tno get map operations must have been performed"
			if ms.getMapInvocationsPayloadMap == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsPayloadMap)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
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
			tracker := &testCleanedTracker{}
			mc := assembleBatchMapCleaner(c, &testHzMapStore{}, ois, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, tracker)

			numCleaned, err := mc.Clean()

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

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
		t.Log("\twhen retrieval of object info succeeds, but get map operation fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}

			ms := populateTestMapStore(numMapObjects, prefixes, 1)
			ms.returnErrorUponGetPayloadMap = true

			mapsSyncMap := ms.maps[mapCleanersSyncMapName]
			mapsSyncMap.tryLockReturnValue = true

			ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

			cih := &testLastCleanedInfoHandler{
				syncMap:        mapsSyncMap,
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.Clean()

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, getPayloadMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tthere must have been only one get map invocation"
			if ms.getMapInvocationsPayloadMap == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsPayloadMap)
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

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
		t.Log("\twhen info retrieval and get map operations succeed, but evict all fails")
		{
			numMapObjects := 9
			prefixes := []string{"ht_"}

			ms := populateTestMapStore(numMapObjects, prefixes, 1)
			mapsSyncMap := ms.maps[mapCleanersSyncMapName]
			mapsSyncMap.tryLockReturnValue = true

			ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

			erroneousEvictAllMapName := "ht_load-0"
			ms.maps[erroneousEvictAllMapName].returnErrorUponEvictAll = true

			c := &cleanerConfig{
				enabled: true,
			}

			cih := &testLastCleanedInfoHandler{
				syncMap:        mapsSyncMap,
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.Clean()

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, mapEvictAllError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tthere must have been only one get map invocation"
			if ms.getMapInvocationsPayloadMap == 1 {
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

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}

		}
		t.Log("\twhen should clean check yields true only for subset of data structures")
		{

			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}
			ms := populateTestMapStore(numMapObjects, prefixes, 1)
			mapsSyncMap := ms.maps[mapCleanersSyncMapName]
			mapsSyncMap.tryLockReturnValue = true

			ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

			numMapsToBeCleaned := 3
			cih := &testLastCleanedInfoHandler{
				syncMap:                  mapsSyncMap,
				shouldCleanAll:           false,
				shouldCleanIndividualMap: createShouldCleanIndividualMapSetup(ms, numMapsToBeCleaned),
			}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.Clean()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tnumber of cleaned data structures must be equal to number of data structures for which should clean check is set up to yield true"
			if numCleaned == numMapsToBeCleaned {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", numCleaned, numMapsToBeCleaned))
			}

			msg = "\t\tnumber of should clean check invocations must still be equal to number of data structures filtered from candidate list"
			if cih.checkInvocations == numMapObjects {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.checkInvocations, numMapObjects))
			}

			msg = "\t\tnumber of update invocations must be equal to number of cleaned maps"
			if cih.updateInvocations == numMapsToBeCleaned {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.updateInvocations, numMapsToBeCleaned))
			}

		}
		t.Log("\twhen should clean check fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}
			ms := populateTestMapStore(numMapObjects, prefixes, 1)
			ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

			cih := &testLastCleanedInfoHandler{
				returnErrorUponCheck: true,
			}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.Clean()

			msg := "\t\terror must be returned"
			if errors.Is(err, lastCleanedInfoCheckError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tno data structure must have been cleaned"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", numCleaned, 0))
			}

			msg = "\t\tonly one should clean check must have been performed"
			if cih.checkInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.checkInvocations, 1))
			}

			msg = "\t\tno updates of last cleaned infos must have been performed"
			if cih.updateInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.updateInvocations, 0))
			}
		}
		t.Log("\twhen update of last cleaned info fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}
			ms := populateTestMapStore(numMapObjects, prefixes, 1)
			ois := populateTestObjectInfos(numMapObjects, prefixes, HzMapService)

			cih := &testLastCleanedInfoHandler{
				syncMap:               ms.maps[mapCleanersSyncMapName],
				shouldCleanAll:        true,
				returnErrorUponUpdate: true,
			}
			mc := assembleBatchMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.Clean()

			msg := "\t\terror must be returned"
			if errors.Is(err, lastCleanedInfoUpdateError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tnumber of cleaned data structures must be one"
			if numCleaned == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", numCleaned, 1))
			}

			msg = "\t\tnumber of update last cleaned info invocations must be one"
			if cih.updateInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.updateInvocations, 1))
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

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleBatchMapCleaner(c, ms, ois, ch, cih, tracker)

			numCleaned, err := mc.Clean()

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
			if ms.getMapInvocationsPayloadMap == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.getMapInvocationsPayloadMap)
			}

			msg = "\t\tcleaner must report zero cleaned data structures"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\ttracker must have been invoked zero times"
			if tracker.numAddInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numAddInvocations)
			}
		}
	}

}

func TestDefaultSingleQueueCleanerBuilder_Build(t *testing.T) {

	t.Log("given the capability to build a single queue cleaner")
	{
		t.Log("\twhen properties required for build are provided")
		{
			ctx := context.TODO()
			ms := populateTestMapStore(1, []string{}, 1)
			qs := populateTestQueueStore(1, []string{}, "", 0)

			builder := DefaultSingleQueueCleanerBuilder{}
			tr := &testCleanedTracker{}
			cih := &DefaultLastCleanedInfoHandler{
				Ms: ms,
			}
			cleaner, hzService := builder.Build(ctx, qs, ms, tr, cih)

			msg := "\t\tcleaner must be built"
			if cleaner != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstring indicating correct Hazelcast service type cleaner refers to must be returned along with cleaner"
			if hzService == HzQueueService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must have correct type"
			qc, ok := cleaner.(*DefaultSingleQueueCleaner)

			if ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner must carry correct context"
			if qc.ctx == ctx {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must carry correct queue store"
			if qc.qs == qs {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must carry correct map store"
			if qc.ms == ms {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must carry correct cleaned tracker"
			if qc.t == tr {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestDefaultBatchQueueCleanerBuilder_Build(t *testing.T) {

	t.Log("given the properties necessary to assemble a batch queue cleaner")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newQueueCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{testConfig: assembleTestConfig(queueCleanerBasePath)}

			tch := &testHzClientHandler{}
			g := status.NewGatherer()
			c, hzService, err := b.Build(tch, context.TODO(), g, hzCluster, hzMembers)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tbuild method must report hazelcast service type corresponding to map cleaner"
			if hzService == HzQueueService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, hzService)
			}

			qc := c.(*DefaultBatchQueueCleaner)
			msg = "\t\tqueue cleaner built must carry context"
			if qc.ctx != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry queue state cleaner key path"

			if qc.keyPath == queueCleanerBasePath {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qc.keyPath)
			}

			msg = "\t\tqueue cleaner built must carry state cleaner config"
			if qc.cfg != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry hazelcast queue store"
			if qc.qs != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry hazelcast object info store"
			if qc.ois != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry hazelcast client handler"
			if qc.ch != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry last cleaned info handler"
			if qc.cih != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tqueue cleaner built must carry tracker"
			if qc.t != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen populate config is unsuccessful")
		{
			b := newQueueCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{returnErrorUponAssignConfigValue: true}

			c, service, err := b.Build(
				&testHzClientHandler{},
				context.TODO(),
				status.NewGatherer(),
				hzCluster,
				hzMembers,
			)

			msg := "\t\tcleaner must be nil"
			if c == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tbuilder must report type of hazelcast service for which builder was to be assembled"
			if service == HzQueueService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, service)
			}

			msg = "\t\tright kind of error must be returned"
			if errors.Is(err, assignConfigPropertyError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

		}
	}

}

func TestDefaultSingleMapCleanerBuilder_Build(t *testing.T) {

	t.Log("given the capability to build a single map cleaner")
	{
		t.Log("\twhen properties required for build are provided")
		{
			ctx := context.TODO()
			ms := populateTestMapStore(1, []string{}, 1)

			builder := DefaultSingleMapCleanerBuilder{}
			tr := &testCleanedTracker{}
			cih := &DefaultLastCleanedInfoHandler{
				Ms: ms,
			}
			cleaner, hzService := builder.Build(ctx, ms, tr, cih)

			msg := "\t\tcleaner must be built"
			if cleaner != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstring indicating correct Hazelcast service type cleaner refers to must be returned along with cleaner"
			if hzService == HzMapService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must have correct type"
			mc, ok := cleaner.(*DefaultSingleMapCleaner)

			if ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner must carry correct context"
			if mc.ctx == ctx {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must carry correct map store"
			if mc.ms == ms {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcleaner built must carry correct cleaned info tracker"
			if mc.t == tr {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestDefaultBatchMapCleanerBuilder_Build(t *testing.T) {

	t.Log("given the properties necessary to assemble a batch map cleaner")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{testConfig: assembleTestConfig(mapCleanerBasePath)}

			tch := &testHzClientHandler{}
			g := status.NewGatherer()
			c, hzService, err := b.Build(tch, context.TODO(), g, hzCluster, hzMembers)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tbuild method must report hazelcast service type corresponding to map cleaner"
			if hzService == HzMapService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, hzService)
			}

			mc := c.(*DefaultBatchMapCleaner)
			msg = "\t\tmap cleaner built must carry context"
			if mc.ctx != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap cleaner built must carry map state cleaner key path"

			if mc.keyPath == mapCleanerBasePath {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mc.keyPath)
			}

			msg = "\t\tmap cleaner built must carry state cleaner config"
			if mc.cfg != nil {
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

			msg = "\t\tmap cleaner built must carry last cleaned info handler"
			if mc.cih != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap cleaner built must carry tracker"
			if mc.t != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
		t.Log("\twhen populate config is unsuccessful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{returnErrorUponAssignConfigValue: true}

			c, service, err := b.Build(
				&testHzClientHandler{},
				context.TODO(),
				status.NewGatherer(),
				hzCluster,
				hzMembers,
			)

			msg := "\t\tcleaner must be nil"
			if c == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tbuilder must report type of hazelcast service for which builder was to be assembled"
			if service == HzMapService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, service)
			}

			msg = "\t\tright kind of error must be returned"
			if errors.Is(err, assignConfigPropertyError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

		}
	}

}

func configValuesAsExpected(cfg *cleanerConfig, expectedValues map[string]any) (bool, string) {

	keyPath := cleanerKeyPath + ".enabled"
	if cfg.enabled != expectedValues[keyPath].(bool) {
		return false, keyPath
	}

	keyPath = cleanerKeyPath + ".prefix.enabled"
	if cfg.usePrefix != expectedValues[keyPath].(bool) {
		return false, keyPath
	}

	keyPath = cleanerKeyPath + ".prefix.prefix"
	if cfg.prefix != expectedValues[keyPath].(string) {
		return false, keyPath
	}

	keyPath = cleanerKeyPath + ".cleanAgainThreshold.enabled"
	if cfg.useCleanAgainThreshold != expectedValues[keyPath].(bool) {
		return false, keyPath
	}

	keyPath = cleanerKeyPath + ".cleanAgainThreshold.thresholdMs"
	if cfg.cleanAgainThresholdMs != uint64(expectedValues[keyPath].(int)) {
		return false, keyPath
	}

	return true, ""

}

func TestRunCleaners(t *testing.T) {

	t.Log("given at least one registered state cleaner")
	{
		t.Log("\twhen one state cleaner builder has registered")
		{
			t.Log("\t\twhen both build and Clean invocations are successful")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: emptyTestCleanerBehavior}
					builders = []BatchCleanerBuilder{b}

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

					msg = "\t\t\tClean method must have been invoked once"
					if cw.cleanAllInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, cw.cleanAllInvocations)
					}
				})
			}
			t.Log("\t\twhen build invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						returnErrorUponBuild: true,
					}}
					builders = []BatchCleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during build must be returned"
					if errors.Is(err, cleanerBuildError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}
			t.Log("\t\twhen Clean invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						returnErrorUponClean: true,
					}}
					builders = []BatchCleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during Clean must be returned"
					if errors.Is(err, batchCleanerCleanError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}
		}
		t.Log("\twhen multiple state cleaner builders have registered")
		{
			runTestCaseAndResetState(func() {
				b0, b1 := &testCleanerBuilder{behavior: emptyTestCleanerBehavior}, &testCleanerBuilder{behavior: emptyTestCleanerBehavior}
				builders = []BatchCleanerBuilder{b0, b1}

				err := RunCleaners(hzCluster, hzMembers)

				msg := "\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\tbuilders must have received own status gatherer"
				if b0.gathererPassedIn != b1.gathererPassedIn {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			})
		}
	}

}

func createShouldCleanIndividualMapSetup(ms *testHzMapStore, numShouldBeCleaned int) map[string]bool {

	keys := make([]string, 0, len(ms.maps))
	for k := range ms.maps {
		keys = append(keys, k)
	}

	shouldCleanIndividualMap := make(map[string]bool)
	numTrueEntries := 0
	for _, key := range keys {
		if numTrueEntries < numShouldBeCleaned && !strings.HasPrefix(key, hzInternalDataStructurePrefix) {
			shouldCleanIndividualMap[key] = true
			numTrueEntries++
		} else {
			shouldCleanIndividualMap[key] = false
		}
	}

	return shouldCleanIndividualMap

}

func runTestCaseAndResetState(testFunc func()) {

	defer cw.reset()
	testFunc()

}

func assembleTestConfig(basePath string) map[string]any {

	return map[string]any{
		basePath + ".enabled":                         true,
		basePath + ".errorBehavior":                   "ignore",
		basePath + ".prefix.enabled":                  true,
		basePath + ".prefix.prefix":                   "ht_",
		basePath + ".cleanAgainThreshold.enabled":     true,
		basePath + ".cleanAgainThreshold.thresholdMs": 30_000,
	}

}

func waitForStatusGatheringDone(g *status.Gatherer) {

	for {
		if done := g.ListeningStopped(); done {
			return
		}
	}

}

func newMapObjectInfoFromName(objectInfoName string) *hazelcastwrapper.SimpleObjectInfo {
	return &hazelcastwrapper.SimpleObjectInfo{
		Name:        objectInfoName,
		ServiceName: HzMapService,
	}
}

func newQueueObjectInfoFromName(objectInfoName string) *hazelcastwrapper.SimpleObjectInfo {
	return &hazelcastwrapper.SimpleObjectInfo{
		Name:        objectInfoName,
		ServiceName: HzQueueService,
	}
}

func populateTestQueueStore(numPayloadQueueObjects int, objectNamePrefixes []string, baseName string, numItemsInQueues int) *testHzQueueStore {

	testQueues := make(map[string]*testHzQueue)

	for i := 0; i < numPayloadQueueObjects; i++ {
		for _, v := range objectNamePrefixes {
			ch := make(chan string, numItemsInQueues)
			for j := 0; j < numItemsInQueues; j++ {
				ch <- fmt.Sprintf("awesome-test-value-%d", j)
			}
			testQueues[fmt.Sprintf("%s%s-%d", v, baseName, i)] = &testHzQueue{
				data: ch,
			}
		}
	}

	return &testHzQueueStore{
		queues: testQueues,
	}

}

func populateTestMapStore(numPayloadMapObjects int, objectNamePrefixes []string, numItemsInPayloadMaps int) *testHzMapStore {

	testMaps := make(map[string]*testHzMap)

	for i := 0; i < numPayloadMapObjects; i++ {
		for _, v := range objectNamePrefixes {
			m := make(map[string]any)
			for j := 0; j < numItemsInPayloadMaps; j++ {
				dummyKey, dummyValue := fmt.Sprintf("awesome-test-key-%d", j), fmt.Sprintf("awesome-test-value-%d", j)
				m[dummyKey] = dummyValue
			}
			testMaps[fmt.Sprintf("%sload-%d", v, i)] = &testHzMap{data: m}
		}
	}

	testMaps[mapCleanersSyncMapName] = &testHzMap{data: make(map[string]any)}
	testMaps[queueCleanersSyncMapName] = &testHzMap{data: make(map[string]any)}

	return &testHzMapStore{maps: testMaps}

}

func populateTestObjectInfos(numObjects int, objectNamePrefixes []string, hzServiceName string) *testHzObjectInfoStore {

	var objectInfos []hazelcastwrapper.ObjectInfo
	for i := 0; i < numObjects; i++ {
		for _, v := range objectNamePrefixes {
			objectInfos = append(objectInfos, hazelcastwrapper.SimpleObjectInfo{Name: fmt.Sprintf("%sload-%d", v, i), ServiceName: hzServiceName})
		}
	}

	return &testHzObjectInfoStore{objectInfos: objectInfos}

}

func resolveObjectKindForNameFromObjectInfoList(name string, objectInfos []hazelcastwrapper.ObjectInfo) string {

	for _, v := range objectInfos {
		if v.GetName() == name {
			return v.GetServiceName()
		}
	}

	return ""

}

func assembleBatchQueueCleaner(c *cleanerConfig, qs *testHzQueueStore, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, cih LastCleanedInfoHandler, t CleanedTracker) *DefaultBatchQueueCleaner {

	return &DefaultBatchQueueCleaner{
		name:      queueCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   queueCleanerBasePath,
		cfg:       c,
		qs:        qs,
		ms:        ms,
		ois:       ois,
		ch:        ch,
		cih:       cih,
		t:         t,
	}

}

func assembleBatchMapCleaner(c *cleanerConfig, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, cih LastCleanedInfoHandler, t CleanedTracker) *DefaultBatchMapCleaner {

	return &DefaultBatchMapCleaner{
		name:      mapCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   mapCleanerBasePath,
		cfg:       c,
		ms:        ms,
		ois:       ois,
		ch:        ch,
		cih:       cih,
		t:         t,
	}

}
