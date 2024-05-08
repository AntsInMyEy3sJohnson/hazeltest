package state

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"hazeltest/status"
	"strings"
	"sync"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		dummyConfig                      map[string]any
		returnErrorUponAssignConfigValue bool
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
	testHzQueueStore struct {
		queues                  map[string]*testHzQueue
		getQueueInvocations     int
		returnErrorUponGetQueue bool
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
	testHzQueue struct {
		data                 chan string
		clearInvocations     int
		returnErrorUponClear bool
	}
	testCleanedTracker struct {
		numInvocations int
	}
)

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
	cleanerCleanError             = errors.New("something went terribly wrong when attempting to clean state")
	getDistributedObjectInfoError = errors.New("something somewhere went terribly wrong upon retrieval of distributed object info")
	getMapError                   = errors.New("something somewhere went terribly wrong when attempting to get a map from the target hazelcast cluster")
	mapEvictAllError              = errors.New("something somewhere went terribly wrong upon attempt to perform evict all")
	getQueueError                 = errors.New("something somewhere went terribly wrong when attempting to get a queue from the target hazelcast cluster")
	queueClearError               = errors.New("something somewhere went terribly wrong upon attempt to perform clear operation on queue")
)

func (m *testHzMap) EvictAll(_ context.Context) error {

	m.evictAllInvocations++

	if m.returnErrorUponEvictAll {
		return mapEvictAllError
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

func (qs *testHzQueueStore) GetQueue(_ context.Context, names string) (hzQueue, error) {

	qs.getQueueInvocations++

	if qs.returnErrorUponGetQueue {
		return nil, getQueueError
	}

	return qs.queues[names], nil

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

func (b *testCleanerBuilder) build(_ hzClientHandler, _ context.Context, _ *status.Gatherer, _ string, _ []string) (cleaner, string, error) {

	b.buildInvocations++

	if b.behavior.throwErrorUponBuild {
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

func (t *testCleanedTracker) addCleanedDataStructure(_, _ string) {

	t.numInvocations++

}

func TestAddCleanedDataStructure(t *testing.T) {

	t.Log("given a cleaned data structure tracker with a method to add a cleaned data structure")
	{
		t.Log("\twhen status gatherer has been correctly populated")
		{
			g := status.NewGatherer()
			go g.Listen()

			tracker := &cleanedDataStructureTracker{g}

			name, kind := "awesome-map", "map"
			tracker.addCleanedDataStructure(name, kind)

			g.StopListen()

			waitForStatusGatheringDone(g)

			msg := "\t\tinformation about cleaned data structure must have been added to status gatherer"

			statusCopy := g.AssembleStatusCopy()
			if statusCopy[name].(string) == kind {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
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

func TestQueueCleanerClean(t *testing.T) {

	t.Log("given a queue cleaner build method")
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
				qc := assembleQueueCleaner(c, dummyQueueStore, dummyObjectInfoStore, ch, tracker)

				numCleaned, err := qc.clean(context.TODO())

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
				qc := assembleQueueCleaner(c, dummyQueueStore, dummyObjectInfoStore, ch, tracker)

				numCleaned, err := qc.clean(context.TODO())

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
				objectInfos:                         make([]hzObjectInfo, 0),
				getDistributedObjectInfoInvocations: 0,
			}

			tracker := &testCleanedTracker{}
			qc := assembleQueueCleaner(c, qs, ois, &testHzClientHandler{}, tracker)

			numCleaned, err := qc.clean(context.TODO())

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
			qc := assembleQueueCleaner(c, &testHzQueueStore{}, ois, &testHzClientHandler{}, tracker)

			numCleaned, err := qc.clean(context.TODO())

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
			qc := assembleQueueCleaner(c, qs, ois, &testHzClientHandler{}, tracker)

			numCleaned, err := qc.clean(context.TODO())

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
			qc := assembleQueueCleaner(c, qs, ois, &testHzClientHandler{}, tracker)

			numCleaned, err := qc.clean(context.TODO())

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
			ois := &testHzObjectInfoStore{}
			ch := &testHzClientHandler{}

			tracker := &testCleanedTracker{}
			qc := assembleQueueCleaner(c, qs, ois, ch, tracker)

			numCleaned, err := qc.clean(context.TODO())

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

				tracker := &testCleanedTracker{}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch, tracker)

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
				hzInternalMapName := "__sql.catalog"
				dummyObjectInfoStore.objectInfos = append(dummyObjectInfoStore.objectInfos, *newMapObjectInfoFromName(hzInternalMapName))
				dummyMapStore.maps[hzInternalMapName] = &testHzMap{data: make(map[string]any)}

				c := &cleanerConfig{
					enabled: true,
				}
				ch := &testHzClientHandler{}
				tracker := &testCleanedTracker{}
				mc := assembleMapCleaner(c, dummyMapStore, dummyObjectInfoStore, ch, tracker)

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
				maps:              make(map[string]*testHzMap),
				getMapInvocations: 0,
			}
			ois := &testHzObjectInfoStore{
				objectInfos:                         make([]hzObjectInfo, 0),
				getDistributedObjectInfoInvocations: 0,
			}
			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, tracker)

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
			mc := assembleMapCleaner(c, &testHzMapStore{}, ois, &testHzClientHandler{}, tracker)

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
			ms.returnErrorUponGetMap = true

			ois := populateDummyObjectInfos(numMapObjects, prefixes, hzMapService)

			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, tracker)

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

			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, &testHzClientHandler{}, tracker)

			numCleaned, err := mc.clean(context.TODO())

			msg := "\t\tcorresponding error must be returned"
			if errors.Is(err, mapEvictAllError) {
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

			msg = "\t\ttracker must have been invoked zero times"
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
			ms := &testHzMapStore{}
			ois := &testHzObjectInfoStore{}
			ch := &testHzClientHandler{}

			tracker := &testCleanedTracker{}
			mc := assembleMapCleaner(c, ms, ois, ch, tracker)

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

	t.Log("given a method to build a queue cleaner builder")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newQueueCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig(queueCleanerBasePath)}

			tch := &testHzClientHandler{}
			g := status.NewGatherer()
			c, hzService, err := b.build(tch, context.TODO(), g, hzCluster, hzMembers)

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

			msg = "\t\tqueue cleaner built must carry queue state cleaner key path"
			qc := c.(*queueCleaner)

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

			c, service, err := b.build(
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

	t.Log("given a method to build a map cleaner builder")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig(mapCleanerBasePath)}

			tch := &testHzClientHandler{}
			g := status.NewGatherer()
			c, hzService, err := b.build(tch, context.TODO(), g, hzCluster, hzMembers)

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

			c, service, err := b.build(
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

func populateDummyQueueStore(numQueueObjects int, objectNamePrefixes []string) *testHzQueueStore {

	testQueues := make(map[string]*testHzQueue)

	for i := 0; i < numQueueObjects; i++ {
		for _, v := range objectNamePrefixes {
			testQueues[fmt.Sprintf("%sload-%d", v, i)] = &testHzQueue{
				data: make(chan string, 9),
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
			testMaps[fmt.Sprintf("%sload-%d", v, i)] = &testHzMap{data: make(map[string]any)}
		}
	}

	return &testHzMapStore{maps: testMaps}

}

func populateDummyObjectInfos(numObjects int, objectNamePrefixes []string, hzServiceName string) *testHzObjectInfoStore {

	var objectInfos []hzObjectInfo
	for i := 0; i < numObjects; i++ {
		for _, v := range objectNamePrefixes {
			objectInfos = append(objectInfos, *&simpleObjectInfo{name: fmt.Sprintf("%sload-%d", v, i), serviceName: hzServiceName})
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

func assembleQueueCleaner(c *cleanerConfig, qs *testHzQueueStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, t cleanedTracker) *queueCleaner {

	return &queueCleaner{
		name:      queueCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   queueCleanerBasePath,
		c:         c,
		qs:        qs,
		ois:       ois,
		ch:        ch,
		t:         t,
	}

}

func assembleMapCleaner(c *cleanerConfig, ms *testHzMapStore, ois *testHzObjectInfoStore, ch *testHzClientHandler, t cleanedTracker) *mapCleaner {

	return &mapCleaner{
		name:      mapCleanerName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   mapCleanerBasePath,
		c:         c,
		ms:        ms,
		ois:       ois,
		ch:        ch,
		t:         t,
	}

}
