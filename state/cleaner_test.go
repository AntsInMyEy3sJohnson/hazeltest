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
		dummyConfig                      map[string]any
		returnErrorUponAssignConfigValue bool
	}
	testCleanerBuilder struct {
		behavior         *testCleanerBehavior
		gathererPassedIn *status.Gatherer
		buildInvocations int
	}
	testCleanerBehavior struct {
		returnErrorUponBuild, returnErrorUponCleanAll, returnErrorUponCleanSingle bool
	}
	testCleaner struct {
		behavior *testCleanerBehavior
	}
	cleanerWatcher struct {
		m                      sync.Mutex
		cleanAllInvocations    int
		cleanSingleInvocations int
	}
	testHzClientHandler struct {
		hzClient            *hazelcast.Client
		shutdownInvocations int
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
		checkInvocations, updateInvocations                         int
		shouldCleanIndividualMap                                    map[string]bool
		shouldCleanAll, returnErrorUponCheck, returnErrorUponUpdate bool
	}
	testCleanedTracker struct {
		numInvocations int
	}
)

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
	hzMembers                     = []string{"awesome-hz-member:5701", "another-awesome-hz-member:5701"}
	cw                            = cleanerWatcher{}
	assignConfigPropertyError     = errors.New("something somewhere went terribly wrong during config property assignment")
	cleanerBuildError             = errors.New("something went terribly wrong when attempting to build the cleaner")
	cleanerCleanAllError          = errors.New("something went terribly wrong when attempting to clean state in all data structures")
	cleanerCleanSingleError       = errors.New("something somewhere went terribly wrong when attempting to clean state in single data structure")
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
		return errors.New("dummy error upon Unlock")
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
		return errors.New("dummy error upon Set")
	}

	return nil

}

func (m *testHzMap) SetWithTTLAndMaxIdle(_ context.Context, _, _ any, _ time.Duration, _ time.Duration) error {

	m.setWithTTLAndMaxIdleInvocations++
	if m.returnErrorUponSetWithTTLAndMaxIdle {
		return errors.New("dummy error upon SetWithTTLAndMaxIdle")
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

	return ms.maps[name], nil

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

func (qs *testHzQueueStore) GetQueue(_ context.Context, names string) (hazelcastwrapper.Queue, error) {

	qs.getQueueInvocations++

	if qs.returnErrorUponGetQueue {
		return nil, getQueueError
	}

	return qs.queues[names], nil

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

	cw.cleanAllInvocations = 0
}

func (c *testCleaner) CleanAll() (int, error) {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanAllInvocations++

	if c.behavior.returnErrorUponCleanAll {
		return 0, cleanerCleanAllError
	}

	return cw.cleanAllInvocations, nil

}

func (c *testCleaner) CleanSingle(_ string) error {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanSingleInvocations++

	if c.behavior.returnErrorUponCleanSingle {
		return cleanerCleanSingleError
	}

	return nil

}

func (b *testCleanerBuilder) Build(_ hazelcastwrapper.HzClientHandler, _ context.Context, g *status.Gatherer, _ string, _ []string) (Cleaner, string, error) {

	b.buildInvocations++
	b.gathererPassedIn = g

	if b.behavior.returnErrorUponBuild {
		return nil, hzMapService, cleanerBuildError
	}

	return &testCleaner{behavior: b.behavior}, hzMapService, nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnErrorUponAssignConfigValue {
		return assignConfigPropertyError
	}

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

func (cih *testLastCleanedInfoHandler) check(syncMapName, payloadDataStructureName, _ string) (mapLockInfo, bool, error) {

	cih.checkInvocations++

	if cih.returnErrorUponCheck {
		return emptyMapLockInfo, false, lastCleanedInfoCheckError
	}

	return mapLockInfo{syncMapName, payloadDataStructureName}, cih.shouldCleanAll || cih.shouldCleanIndividualMap[payloadDataStructureName], nil

}

func (cih *testLastCleanedInfoHandler) update(_, _, _ string) error {

	cih.updateInvocations++

	if cih.returnErrorUponUpdate {
		return lastCleanedInfoUpdateError
	}

	return nil

}

func (t *testCleanedTracker) addCleanedDataStructure(_ string, _ int) {

	t.numInvocations++

}

func TestDefaultLastCleanedInfoHandlerCheck(t *testing.T) {

	t.Log("given a map store containing sync map for map cleaners that needs to be checked for last cleaned info on payload map")
	{
		t.Log("\twhen get map on sync map yields error")
		{

			ms := populateDummyMapStore(1, []string{"ht_"})
			ms.returnErrorUponGetSyncMap = true
			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_aragorn-0", hzMapService)

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
			ms := populateDummyMapStore(1, []string{"ht_"})
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.returnErrorUponTryLock = true

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_gimli-0", hzMapService)

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

			ms := populateDummyMapStore(1, []string{"blubbedi_"})
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = false

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, "ht_legolas-0", hzMapService)

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
		t.Log("\twhen get on sync map for key-value pair related to payload data structure yields error")
		{
			prefix := "waldo_"
			ms := populateDummyMapStore(1, []string{prefix})

			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.returnErrorUponGet = true

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			payloadMapName := prefix + "load-0"
			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, hzMapService)

			msg := "\t\terror must be returned"
			if errors.Is(err, getOnMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.keyName == payloadMapName {
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
			ms := populateDummyMapStore(1, []string{"ht_"})
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			payloadMapName := "ht_aragorn-0"
			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, hzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.keyName == payloadMapName {
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
			ms := populateDummyMapStore(1, []string{prefix})

			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap.data[payloadMapName] = "clearly not a valid timestamp"

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, hzMapService)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.keyName == payloadMapName {
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
			ms := populateDummyMapStore(1, []string{prefix})

			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.data[payloadMapName] = time.Now().UnixNano()

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, hzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.keyName == payloadMapName {
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
			ms := populateDummyMapStore(1, []string{prefix})

			payloadMapName := prefix + "load-0"
			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
			mapCleanersSyncMap.tryLockReturnValue = true
			mapCleanersSyncMap.data[payloadMapName] = int64(0)

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			lockInfo, shouldCheck, err := cih.check(mapCleanersSyncMapName, payloadMapName, hzMapService)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock info must be populated with name of sync map and name of payload map"
			if lockInfo.mapName == mapCleanersSyncMapName && lockInfo.keyName == payloadMapName {
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

func TestDefaultLastCleanedInfoHandlerUpdate(t *testing.T) {

	t.Log("given a map store containing sync map for map cleaners that needs to be updated with new last cleaned info")
	{
		t.Log("\twhen get map for map cleaners sync map yields error")
		{
			func() {

				defer func() {
					msg := "\t\tno invocation on nil object representing map holding lock must have been performed"
					if r := recover(); r == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, r)
					}
				}()

				ms := populateDummyMapStore(1, []string{})
				ms.returnErrorUponGetSyncMap = true

				cih := &defaultLastCleanedInfoHandler{
					ms:  ms,
					ctx: context.TODO(),
				}

				err := cih.update(mapCleanersSyncMapName, "ht_load-0", hzMapService)

				msg := "\t\terror must be returned"
				if errors.Is(err, getSyncMapError) {
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

			}()

		}

		t.Log("\twhen get map for map cleaners sync map is successful")
		{
			ms := populateDummyMapStore(1, []string{})

			cih := &defaultLastCleanedInfoHandler{
				ms:  ms,
				ctx: context.TODO(),
			}

			err := cih.update(mapCleanersSyncMapName, "ht_load-0", hzMapService)

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]

			msg = "\t\tlast cleaned info must have been updated"
			if mapCleanersSyncMap.setWithTTLAndMaxIdleInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlock on map cleaners sync map for payload map must have been released"
			if mapCleanersSyncMap.unlockInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen final unlock operation yields error")
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

				ms := populateDummyMapStore(1, []string{})
				mapCleanersSyncMap := ms.maps[mapCleanersSyncMapName]
				mapCleanersSyncMap.returnErrorUponUnlock = true

				cih := &defaultLastCleanedInfoHandler{
					ms:  ms,
					ctx: context.TODO(),
				}

				err := cih.update(mapCleanersSyncMapName, "ht_load-0", hzMapService)

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

func TestQueueCleanerRetrieveAndClean(t *testing.T) {

	t.Log("given a queue store containing at least one queue that needs to be retrieved and cleaned")
	{
		t.Log("\twhen get queue operation yields error")
		{
			dummyQueueStore := &testHzQueueStore{returnErrorUponGetQueue: true}
			qc := assembleQueueCleaner(&cleanerConfig{}, dummyQueueStore, &testHzMapStore{}, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := qc.retrieveAndClean(context.TODO(), "some-name")

			msg := "\t\tmethod must report zero cleaned entries"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, getQueueError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
		t.Log("\twhen query size operation yields error")
		{
			queueName := "some-name"
			testQueues := map[string]*testHzQueue{
				queueName: {returnErrorUponSize: true},
			}
			dummyQueueStore := &testHzQueueStore{queues: testQueues}

			qc := assembleQueueCleaner(&cleanerConfig{}, dummyQueueStore, &testHzMapStore{}, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := qc.retrieveAndClean(context.TODO(), queueName)

			msg := "\t\tmethod must report zero cleaned entries"

			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, queueSizeError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen target queue contains zero entries")
		{
			queueName := "awesome-queue"
			dummyData := make(chan string)
			testQueues := map[string]*testHzQueue{
				queueName: {data: dummyData},
			}
			dummyQueueStore := &testHzQueueStore{queues: testQueues}

			qc := assembleQueueCleaner(&cleanerConfig{}, dummyQueueStore, &testHzMapStore{}, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := qc.retrieveAndClean(context.TODO(), queueName)

			msg := "\t\tnumber of cleaned elements must be reported to be zero"
			if numCleaned == len(dummyData) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tclear must not have been invoked"
			clearInvocations := testQueues[queueName].clearInvocations
			if clearInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, clearInvocations)
			}
		}
		t.Log("\twhen clear on target queue yields error")
		{
			dummyQueue := make(chan string, 1)
			dummyQueue <- "my-value"

			queueName := "awesome-queue"
			testQueues := map[string]*testHzQueue{
				queueName: {data: dummyQueue, returnErrorUponClear: true},
			}
			dummyQueueStore := &testHzQueueStore{queues: testQueues}

			qc := assembleQueueCleaner(&cleanerConfig{}, dummyQueueStore, &testHzMapStore{}, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := qc.retrieveAndClean(context.TODO(), queueName)

			msg := "\t\treported number of cleaned elements must be zero"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, queueClearError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
		t.Log("\twhen clear is successful on target queue that previously contained at least one element")
		{
			dummyQueue := make(chan string, 2)
			dummyQueue <- "my-value"
			dummyQueue <- "my-other-value"

			queueName := "awesome-queue"
			testQueues := map[string]*testHzQueue{
				queueName: {data: dummyQueue},
			}
			dummyQueueStore := &testHzQueueStore{queues: testQueues}

			ct := &testCleanedTracker{}
			qc := assembleQueueCleaner(&cleanerConfig{}, dummyQueueStore, &testHzMapStore{}, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, ct)

			sizeDummyQueueBeforeClear := len(dummyQueue)
			numCleaned, err := qc.retrieveAndClean(context.TODO(), queueName)

			msg := "\t\treported number of cleaned elements in data structure must be equal to size of target queue before eviction"
			if numCleaned == sizeDummyQueueBeforeClear {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tcleaned data structure must have been added to tracker"
			if ct.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ct.numInvocations)
			}
		}
	}

}

func TestMapCleanerRetrieveAndClean(t *testing.T) {

	t.Log("given a map store containing at least one map that needs to be retrieved and cleaned")
	{
		t.Log("\twhen get payload map operation yields error")
		{
			dummyMapStore := &testHzMapStore{returnErrorUponGetPayloadMap: true}
			mc := assembleMapCleaner(&cleanerConfig{}, dummyMapStore, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := mc.retrieveAndClean(context.TODO(), "blubb")

			msg := "\t\tmethod must report zero cleaned entries"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, getPayloadMapError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
		t.Log("\twhen query size operation yields error")
		{
			mapName := "awesome-map"
			testMaps := map[string]*testHzMap{
				mapName: {returnErrorUponSize: true},
			}
			dummyMapStore := &testHzMapStore{maps: testMaps}

			mc := assembleMapCleaner(&cleanerConfig{}, dummyMapStore, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := mc.retrieveAndClean(context.TODO(), mapName)

			msg := "\t\tmethod must report zero cleaned entries"

			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, mapSizeError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen target map contains zero entries")
		{
			mapName := "another-awesome-map"
			dummyData := map[string]any{}
			testMaps := map[string]*testHzMap{
				mapName: {data: dummyData},
			}
			dummyMapStore := &testHzMapStore{maps: testMaps}

			mc := assembleMapCleaner(&cleanerConfig{}, dummyMapStore, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := mc.retrieveAndClean(context.TODO(), mapName)

			msg := "\t\tnumber of cleaned elements must be reported to be zero"
			if numCleaned == len(dummyData) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tevict all must not have been invoked"
			evictAllInvocations := testMaps[mapName].evictAllInvocations
			if evictAllInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, evictAllInvocations)
			}
		}
		t.Log("\twhen evict all on target map yields error")
		{
			dummyMap := map[string]any{
				"blubb": "blubbi",
			}
			mapName := "yet-another-awesome-map"
			testMaps := map[string]*testHzMap{
				mapName: {data: dummyMap, returnErrorUponEvictAll: true},
			}
			dummyMapStore := &testHzMapStore{maps: testMaps}

			mc := assembleMapCleaner(&cleanerConfig{}, dummyMapStore, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, &testCleanedTracker{})

			numCleaned, err := mc.retrieveAndClean(context.TODO(), mapName)

			msg := "\t\treported number of cleaned elements must be zero"
			if numCleaned == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\terror must be returned"
			if errors.Is(err, mapEvictAllError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
		t.Log("\twhen evict all is successful on target map that previously contained at least one element")
		{
			dummyMap := map[string]any{
				"blubba": "blubbo",
				"blubbi": "blubbe",
			}
			mapName := "super-cool-awesome-map"
			testMaps := map[string]*testHzMap{
				mapName: {data: dummyMap},
			}
			dummyMapStore := &testHzMapStore{maps: testMaps}

			ct := &testCleanedTracker{}
			mc := assembleMapCleaner(&cleanerConfig{}, dummyMapStore, &testHzObjectInfoStore{}, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, ct)

			lenDummyMapBeforeEviction := len(dummyMap)
			numCleaned, err := mc.retrieveAndClean(context.TODO(), mapName)

			msg := "\t\treported number of cleaned elements in data structure must be equal to size of target map before eviction"
			if numCleaned == lenDummyMapBeforeEviction {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, numCleaned)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tcleaned data structure must have been added to tracker"
			if ct.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ct.numInvocations)
			}
		}
	}

}

func TestAddCleanedDataStructure(t *testing.T) {

	t.Log("given a status update about a cleaned data structure to be added to the cleaned data structure tracker")
	{
		t.Log("\twhen status gatherer has been correctly populated")
		{
			g := status.NewGatherer()
			go g.Listen()

			tracker := &cleanedDataStructureTracker{g}

			name := "awesome-map"
			size := 9
			tracker.addCleanedDataStructure(name, size)

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

func TestIdentifyCandidateDataStructuresFromObjectInfo(t *testing.T) {

	t.Log("given information about data structures stored in hazelcast that need to be checked for whether they are susceptible to getting cleaned")
	{
		valid := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "ht_load-1",
			ServiceName: hzMapService,
		}
		invalidBecauseSystemInternal := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "__sql.catalog",
			ServiceName: hzMapService,
		}
		invalidBecauseRepresentsQueue := &hazelcastwrapper.SimpleObjectInfo{
			Name:        "ht_load-2",
			ServiceName: hzQueueService,
		}

		t.Log("\twhen object info retrieval does not yield error")
		{
			t.Log("\t\twhen object info list contains information on both valid candidates and elements not viable as candidates")
			{
				objectInfos := []hazelcastwrapper.ObjectInfo{valid, invalidBecauseSystemInternal, invalidBecauseRepresentsQueue}
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
					objectInfos: []hazelcastwrapper.ObjectInfo{invalidBecauseSystemInternal, invalidBecauseRepresentsQueue},
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
					objectInfos: make([]hazelcastwrapper.ObjectInfo, 0),
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

func TestQueueCleanerClean(t *testing.T) {

	t.Log("given a target hazelcast cluster potentially containing queues susceptible to getting cleaned")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues, and all retrieval operations are successful")
		{
			t.Log("\t\twhen prefix usage has been enabled")
			{
				numQueueObjects := 9
				prefixToConsider := "ht_"
				prefixes := []string{prefixToConsider, "aragorn_"}

				dummyQueueStore := populateDummyQueueStore(numQueueObjects, prefixes)
				dummyObjectInfoStore := populateDummyObjectInfos(numQueueObjects, prefixes, hzQueueService)

				// Add object representing map, so we can verify that no attempt was made to retrieve it
				// The name of this object matches the given predicate, so method under test must use service name to establish
				// object in question represents map
				mapObjectName := fmt.Sprintf("%sload-42", prefixToConsider)
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newMapObjectInfoFromName(mapObjectName))
				dummyQueueStore.queues[mapObjectName] = &testHzQueue{}

				// Add Hazelcast-internal map
				hzInternalQueueName := "__awesome.internal.queue"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newQueueObjectInfoFromName(hzInternalQueueName))
				dummyQueueStore.queues[hzInternalQueueName] = &testHzQueue{}

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
				// on the dummy map store.
				cih := &defaultLastCleanedInfoHandler{
					ms:  ms,
					ctx: ctx,
				}
				queueCleanersSyncMap := ms.maps[queueCleanersSyncMapName]
				queueCleanersSyncMap.tryLockReturnValue = true

				qc := assembleQueueCleaner(c, dummyQueueStore, ms, dummyObjectInfoStore, ch, cih, tracker)

				numCleaned, err := qc.CleanAll()

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

				msg = "\t\t\tget queue must have been invoked only on queues whose prefix matches configuration"
				if dummyQueueStore.getQueueInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyQueueStore.getQueueInvocations)
				}

				msg = "\t\t\tnumber of get map invocations on queue cleaners sync map must be twice the number of payload queues whose name matches given prefix"
				if ms.getMapInvocationsQueueSyncMap == numQueueObjects*2 {
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

				invokedOnceMsg := "\t\t\tclear must have been invoked on all data structures whose prefix matches configuration"
				notInvokedMsg := "\t\t\tclear must not have been invoked on data structure that is either not a queue or whose name does not correspond to given prefix"
				for k, v := range dummyQueueStore.queues {
					if strings.HasPrefix(k, prefixToConsider) && resolveObjectKindForNameFromObjectInfoList(k, dummyObjectInfoStore.objectInfos) == hzQueueService {
						if v.clearInvocations == 1 {
							t.Log(invokedOnceMsg, checkMark, k)
						} else {
							t.Fatal(invokedOnceMsg, ballotX, k, v.clearInvocations)
						}
					} else {
						if v.clearInvocations == 0 {
							t.Log(notInvokedMsg, checkMark, k)
						} else {
							t.Fatal(notInvokedMsg, ballotX, k, v.clearInvocations)
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
				if tracker.numInvocations == numQueueObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numInvocations)
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

				dummyQueueStore := populateDummyQueueStore(numQueueObjects, prefixes)
				dummyObjectInfoStore := populateDummyObjectInfos(numQueueObjects, prefixes, hzQueueService)

				// Add Hazelcast-internal map to make sure cleaner does not consider such maps
				// even when prefix usage has been disabled
				hzInternalQueueName := "__awesome.internal.queue"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newQueueObjectInfoFromName(hzInternalQueueName))
				dummyQueueStore.queues[hzInternalQueueName] = &testHzQueue{}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}

				cih := &testLastCleanedInfoHandler{
					shouldCleanAll: true,
				}
				qc := assembleQueueCleaner(c, dummyQueueStore, &testHzMapStore{}, dummyObjectInfoStore, ch, cih, tracker)

				numCleaned, err := qc.CleanAll()

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tget all must have been invoked on all maps that are not hazelcast-internal maps"
				expectedCleaned := numQueueObjects * len(prefixes)
				if dummyQueueStore.getQueueInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyQueueStore.getQueueInvocations)
				}

				invokedMsg := "\t\t\tclear must have been invoked on all queues that are not hazelcast-internal queues"
				notInvokedMsg := "\t\t\tclear must not have been invoked on hazelcast-internal queues"
				for k, v := range dummyQueueStore.queues {
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
				if tracker.numInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numInvocations)
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
			qc := assembleQueueCleaner(c, qs, &testHzMapStore{}, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
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
			qc := assembleQueueCleaner(c, &testHzQueueStore{}, ms, ois, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, tracker)

			numCleaned, err := qc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}
		}
		t.Log("\twhen retrieval of object info succeeds, but get queue operation fails")
		{
			c := &cleanerConfig{
				enabled: true,
			}
			numQueueOperations := 9
			prefixes := []string{"ht_"}

			qs := populateDummyQueueStore(numQueueOperations, prefixes)
			qs.returnErrorUponGetQueue = true

			ois := populateDummyObjectInfos(numQueueOperations, prefixes, hzQueueService)

			tracker := &testCleanedTracker{}

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			qc := assembleQueueCleaner(c, qs, &testHzMapStore{}, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}
		}
		t.Log("\twhen info retrieval and get queue operations succeed, but clear operation fails")
		{
			numQueueObjects := 9
			prefixes := []string{"ht_"}

			qs := populateDummyQueueStore(numQueueObjects, prefixes)
			ois := populateDummyObjectInfos(numQueueObjects, prefixes, hzQueueService)

			erroneousClearMapName := "ht_load-0"
			qs.queues[erroneousClearMapName].returnErrorUponClear = true

			c := &cleanerConfig{
				enabled: true,
			}

			tracker := &testCleanedTracker{}

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			qc := assembleQueueCleaner(c, qs, &testHzMapStore{}, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := qc.CleanAll()

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
				if k == erroneousClearMapName {
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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
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
			qc := assembleQueueCleaner(c, qs, ms, ois, ch, cih, tracker)

			numCleaned, err := qc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}
		}
	}

}

func TestMapCleanerClean(t *testing.T) {

	t.Log("given a target hazelcast cluster potentially containing maps susceptible to getting cleaned")
	{
		t.Log("\twhen target hazeltest cluster contains multiple maps and queues, and all retrieval operations are successful")
		{
			t.Log("\t\twhen prefix usage has been enabled")
			{
				numMapObjects := 9
				prefixToConsider := "ht_"
				prefixes := []string{prefixToConsider, "gimli_"}

				dummyMapStore := populateDummyMapStore(numMapObjects, prefixes)
				dummyObjectInfoStore := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

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

				ctx := context.TODO()
				// Default last cleaned info handler used in place of test variant for this "happy-path" test
				// in order to increase test integration level by verifying number and kind of invocations performed
				// on the dummy map store.
				cih := &defaultLastCleanedInfoHandler{
					ms:  dummyMapStore,
					ctx: ctx,
				}

				tracker := &testCleanedTracker{}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch, cih, tracker)

				mapCleanersSyncMap := dummyMapStore.maps[mapCleanersSyncMapName]
				mapCleanersSyncMap.tryLockReturnValue = true

				numCleaned, err := mc.CleanAll()

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

				msg = "\t\t\tget map must have been invoked only on payload maps whose prefix matches configuration"
				if dummyMapStore.getMapInvocationsPayloadMap == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocationsPayloadMap)
				}

				msg = "\t\t\tnumber of get map invocations on map cleaners sync map must be twice the number of payload maps whose name matches prefix"
				if dummyMapStore.getMapInvocationsMapsSyncMap == numMapObjects*2 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocationsMapsSyncMap)
				}

				msg = "\t\t\tthere must be no get map invocations on queue cleaners sync map"
				if dummyMapStore.getMapInvocationsQueueSyncMap == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocationsQueueSyncMap)
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
				if tracker.numInvocations == numMapObjects {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numInvocations)
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
				dummyObjectInfoStore := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

				// Add Hazelcast-internal map to make sure cleaner does not consider such maps
				// even when prefix usage has been disabled

				hzInternalMapName := hzInternalDataStructurePrefix + "sql.catalog"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				dummyMapStore.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}

				cih := &testLastCleanedInfoHandler{
					shouldCleanAll: true,
				}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch, cih, tracker)

				numCleaned, err := mc.CleanAll()

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tget all must have been invoked on all maps that are not hazelcast-internal maps"
				expectedCleaned := numMapObjects * len(prefixes)
				if dummyMapStore.getMapInvocationsPayloadMap == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, dummyMapStore.getMapInvocationsPayloadMap)
				}

				invokedMsg := "\t\t\tevict all must have been invoked on all maps that are not hazelcast-internal maps"
				notInvokedMsg := "\t\t\tevict all must not have been invoked on hazelcast-internal maps"
				for k, v := range dummyMapStore.maps {
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
				if tracker.numInvocations == expectedCleaned {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, tracker.numInvocations)
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
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
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
			mc := assembleMapCleaner(c, &testHzMapStore{}, ois, &testHzClientHandler{}, &testLastCleanedInfoHandler{}, tracker)

			numCleaned, err := mc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
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
			ms.returnErrorUponGetPayloadMap = true

			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}
		}
		t.Log("\twhen info retrieval and get map operations succeed, but evict all fails")
		{
			numMapObjects := 9
			prefixes := []string{"ht_"}

			ms := populateDummyMapStore(numMapObjects, prefixes)
			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			erroneousEvictAllMapName := "ht_load-0"
			ms.maps[erroneousEvictAllMapName].returnErrorUponEvictAll = true

			c := &cleanerConfig{
				enabled: true,
			}

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll: true,
			}
			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, tracker)

			numCleaned, err := mc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}

		}
		t.Log("\twhen should clean check yields true only for subset of data structures")
		{

			c := &cleanerConfig{
				enabled: true,
			}
			numMapObjects := 9
			prefixes := []string{"ht_"}
			ms := populateDummyMapStore(numMapObjects, prefixes)
			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			numMapsToBeCleaned := 3
			cih := &testLastCleanedInfoHandler{
				shouldCleanAll:           false,
				shouldCleanIndividualMap: createShouldCleanIndividualMapSetup(ms, numMapsToBeCleaned),
			}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.CleanAll()

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
			ms := populateDummyMapStore(numMapObjects, prefixes)
			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			cih := &testLastCleanedInfoHandler{
				returnErrorUponCheck: true,
			}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.CleanAll()

			// In case of failing should clean check, error is not returned; rather, the loop over filtered
			// data structures potentially susceptible to cleaning continues with the next data structure. Thus, even
			// in case should clean check fails for all data structures, we still expect a nil error on the
			// caller's side.
			msg := "\t\tno error must be returned"
			if err == nil {
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

			msg = "\t\tnumber of should clean invocations must be equal to number of filtered data structures"
			if cih.checkInvocations == numMapObjects {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.checkInvocations, numMapObjects))
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
			ms := populateDummyMapStore(numMapObjects, prefixes)
			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			cih := &testLastCleanedInfoHandler{
				shouldCleanAll:        true,
				returnErrorUponUpdate: true,
			}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, cih, &testCleanedTracker{})
			numCleaned, err := mc.CleanAll()

			// Similar to the behavior in case of failed should clean checks, failure to update last cleaned info
			// should not propagate as an error to the caller. Instead, the loop should continue with the next
			// data structure.
			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tnumber of cleaned data structures must be equal to number of data structures provided in test setup"
			if numCleaned == numMapObjects {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", numCleaned, numMapObjects))
			}

			msg = "\t\tnumber of update last cleaned info invocations must be equal to number of data structures provided in test setup"
			if cih.updateInvocations == numMapObjects {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", cih.updateInvocations, numMapObjects))
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
			mc := assembleMapCleaner(c, ms, ois, ch, cih, tracker)

			numCleaned, err := mc.CleanAll()

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
			if tracker.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, tracker.numInvocations)
			}
		}
	}

}

func TestQueueCleanerBuilderBuild(t *testing.T) {

	t.Log("given the properties necessary to assemble a queue cleaner")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newQueueCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig(queueCleanerBasePath)}

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
			if hzService == hzQueueService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, hzService)
			}

			qc := c.(*QueueCleaner)
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
			if qc.c != nil {
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
			if service == hzQueueService {
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

func TestMapCleanerBuilderBuild(t *testing.T) {

	t.Log("given the properties necessary to assemble a map cleaner")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig(mapCleanerBasePath)}

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
			if hzService == hzMapService {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, hzService)
			}

			mc := c.(*MapCleaner)
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
			if service == hzMapService {
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

func TestRunCleaners(t *testing.T) {

	t.Log("given at least one registered state cleaner")
	{
		t.Log("\twhen one state cleaner builder has registered")
		{
			t.Log("\t\twhen both build and CleanAll invocations are successful")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: emptyTestCleanerBehavior}
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

					msg = "\t\t\tCleanAll method must have been invoked once"
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
			t.Log("\t\twhen CleanAll invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						returnErrorUponCleanAll: true,
					}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during CleanAll must be returned"
					if errors.Is(err, cleanerCleanAllError) {
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
				builders = []cleanerBuilder{b0, b1}

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
		basePath + ".enabled":        true,
		basePath + ".prefix.enabled": true,
		basePath + ".prefix.prefix":  "ht_",
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
		ServiceName: hzMapService,
	}
}

func newQueueObjectInfoFromName(objectInfoName string) *hazelcastwrapper.SimpleObjectInfo {
	return &hazelcastwrapper.SimpleObjectInfo{
		Name:        objectInfoName,
		ServiceName: hzQueueService,
	}
}

func populateDummyQueueStore(numQueueObjects int, objectNamePrefixes []string) *testHzQueueStore {

	testQueues := make(map[string]*testHzQueue)

	for i := 0; i < numQueueObjects; i++ {
		for _, v := range objectNamePrefixes {
			ch := make(chan string, 9)
			ch <- "awesome-dummy-value"
			testQueues[fmt.Sprintf("%sload-%d", v, i)] = &testHzQueue{
				data: ch,
			}
		}
	}

	return &testHzQueueStore{
		queues: testQueues,
	}

}

func populateDummyMapStore(numMapObjects int, objectNamePrefixes []string) *testHzMapStore {

	testMaps := make(map[string]*testHzMap)

	for i := 0; i < numMapObjects; i++ {
		for _, v := range objectNamePrefixes {
			m := map[string]any{
				"awesome-dummy-key": "awesome-dummy-value",
			}
			testMaps[fmt.Sprintf("%sload-%d", v, i)] = &testHzMap{data: m}
		}
	}

	testMaps[mapCleanersSyncMapName] = &testHzMap{data: make(map[string]any)}

	return &testHzMapStore{maps: testMaps}

}

func populateDummyObjectInfos(numObjects int, objectNamePrefixes []string, hzServiceName string) *testHzObjectInfoStore {

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

func assembleQueueCleaner(c *cleanerConfig, qs *testHzQueueStore, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, cih lastCleanedInfoHandler, t cleanedTracker) *QueueCleaner {

	return &QueueCleaner{
		name:      queueCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   queueCleanerBasePath,
		c:         c,
		qs:        qs,
		ms:        ms,
		ois:       ois,
		ch:        ch,
		cih:       cih,
		t:         t,
	}

}

func assembleMapCleaner(c *cleanerConfig, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, cih lastCleanedInfoHandler, t cleanedTracker) *MapCleaner {

	return &MapCleaner{
		name:      mapCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   mapCleanerBasePath,
		c:         c,
		ms:        ms,
		ois:       ois,
		ch:        ch,
		cih:       cih,
		t:         t,
	}

}
