package maps

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
	"time"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		testConfig  map[string]any
	}
	testHzClientHandler struct {
		getClientInvocations, initClientInvocations, shutdownInvocations int
		hzClusterName                                                    string
		hzClusterMembers                                                 []string
	}
	testHzMapStore struct {
		m                     *testHzMap
		returnErrorUponGetMap bool
		observations          *testHzMapStoreObservations
	}
	testHzMapStoreObservations struct {
		numInitInvocations int
	}
	boundaryMonitoring struct {
		upperBoundaryNumElements       int
		lowerBoundaryNumElements       int
		upperBoundaryThresholdViolated bool
		lowerBoundaryThresholdViolated bool
		upperBoundaryViolationValue    int
		lowerBoundaryViolationValue    int
	}
	testHzMap struct {
		containsKeyInvocations                    int
		setInvocations                            int
		getInvocations                            int
		removeInvocations                         int
		destroyInvocations                        int
		sizeInvocations                           int
		removeAllInvocations                      int
		evictAllInvocations                       int
		lastPredicateFilterForRemoveAllInvocation string
		// TODO Use regular map rather than sync.Map because access to testHzMap properties has to ge guarded by lock anyway
		data                       *sync.Map
		returnErrorUponGet         bool
		returnErrorUponSet         bool
		returnErrorUponContainsKey bool
		returnErrorUponRemove      bool
		returnErrorUponRemoveAll   bool
		returnErrorUponEvictAll    bool
		bm                         *boundaryMonitoring
	}
)

func (d *testHzClientHandler) GetClusterName() string {
	return d.hzClusterName
}

func (d *testHzClientHandler) GetClusterMembers() []string {
	return d.hzClusterMembers
}

func (d *testHzClientHandler) GetClient() *hazelcast.Client {
	d.getClientInvocations++
	return nil
}

func (d *testHzClientHandler) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	d.initClientInvocations++
}

func (d *testHzClientHandler) Shutdown(_ context.Context) error {
	d.shutdownInvocations++
	return nil
}

func (m *testHzMap) SetWithTTLAndMaxIdle(_ context.Context, _, _ any, _ time.Duration, _ time.Duration) error {
	return nil
}

func (m *testHzMap) TryLock(_ context.Context, _ any) (bool, error) {
	return false, nil
}

func (m *testHzMap) Unlock(_ context.Context, _ any) error {
	return nil
}

const (
	checkMark     = "\u2713"
	ballotX       = "\u2717"
	runnerKeyPath = "testMapRunner"
	mapPrefix     = "t_"
	mapBaseName   = "test"
)

var (
	hzCluster                = "awesome-hz-cluster"
	hzMembers                = []string{"awesome-hz-cluster-svc.cluster.local"}
	expectedStatesForFullRun = []runnerState{start, populateConfigComplete, checkEnabledComplete, assignTestLoopComplete, raiseReadyComplete, testLoopStart, testLoopComplete}
	testMapOperationLock     sync.Mutex
)

func waitForStatusGatheringDone(g *status.Gatherer) {

	for {
		if done := g.ListeningStopped(); done {
			return
		}
	}

}

func latestStatePresentInGatherer(g *status.Gatherer, desiredState runnerState) bool {

	if value, ok := g.AssembleStatusCopy()[string(statusKeyCurrentState)]; ok && value == string(desiredState) {
		return true
	}

	return false

}

func (d testHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (d testHzMapStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d testHzMapStore) GetMap(_ context.Context, _ string) (hazelcastwrapper.Map, error) {
	if d.returnErrorUponGetMap {
		return nil, errors.New("i was told to throw an error")
	}
	return d.m, nil
}

func (m *testHzMap) ContainsKey(_ context.Context, key any) (bool, error) {

	testMapOperationLock.Lock()
	{
		m.containsKeyInvocations++
	}
	testMapOperationLock.Unlock()

	if m.returnErrorUponContainsKey {
		return false, errors.New("a deliberately returned error")
	}

	keyString, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("unable to parse given key into string for querying test data source: %v", key)
	}
	if _, ok := m.data.Load(keyString); ok {
		return true, nil
	}

	return false, nil

}

func (m *testHzMap) Set(_ context.Context, key any, value any) error {

	testMapOperationLock.Lock()
	{
		m.setInvocations++
	}
	testMapOperationLock.Unlock()

	if m.returnErrorUponSet {
		return errors.New("also a deliberately thrown error")
	}

	keyString, ok := key.(string)
	if !ok {
		return fmt.Errorf("unable to parse given key into string for querying test data source: %v", key)
	}

	m.data.Store(keyString, value)

	if m.bm != nil && !m.bm.upperBoundaryThresholdViolated && !m.bm.lowerBoundaryThresholdViolated {
		currentMapSize := 0
		m.data.Range(func(_, _ any) bool {
			currentMapSize++
			return true
		})
		if currentMapSize > m.bm.upperBoundaryNumElements {
			m.bm.upperBoundaryThresholdViolated = true
			m.bm.upperBoundaryViolationValue = currentMapSize
		}
		if currentMapSize < m.bm.lowerBoundaryNumElements {
			// Make sure threshold violation does not get triggered
			// while map is being initially filled by checking
			// we've had more set invocations than the maximum number
			// of elements expected in the map based on the given
			// percentage threshold
			if m.setInvocations > m.bm.upperBoundaryNumElements {
				m.bm.lowerBoundaryThresholdViolated = true
				m.bm.lowerBoundaryViolationValue = currentMapSize
			}
		}
	}

	return nil

}

func (m *testHzMap) Get(_ context.Context, key any) (any, error) {

	testMapOperationLock.Lock()
	{
		m.getInvocations++
	}
	testMapOperationLock.Unlock()

	if m.returnErrorUponGet {
		return nil, errors.New("awesome test error")
	}

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying test data source: %v", key)
	}

	value, _ := m.data.Load(keyString)
	return value, nil

}

func applyFunctionToTestMapContents(m *testHzMap, fn func(key, value any) bool) {

	m.data.Range(func(key, value any) bool {
		return fn(key, value)
	})

}

func (m *testHzMap) Remove(_ context.Context, key any) (any, error) {

	testMapOperationLock.Lock()
	{
		m.removeInvocations++
	}
	testMapOperationLock.Unlock()

	if m.returnErrorUponRemove {
		return nil, errors.New("lo and behold, an error")
	}

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying test data source: %v", key)
	}

	if value, ok := m.data.Load(keyString); ok {
		defer m.data.Delete(keyString)
		return value, nil
	}

	// A Hazelcast map won't actually return an error upon request to delete a non-existing key,
	// so it's more accurate to mirror this behavior here, and not return an error
	return nil, nil

}

func (m *testHzMap) Destroy(_ context.Context) error {

	testMapOperationLock.Lock()
	{
		m.destroyInvocations++
	}
	testMapOperationLock.Unlock()

	return nil

}

func (m *testHzMap) Size(_ context.Context) (int, error) {

	size := 0
	testMapOperationLock.Lock()
	{
		m.sizeInvocations++
		applyFunctionToTestMapContents(m, func(key, value any) bool {
			size++
			return true
		})
	}
	testMapOperationLock.Unlock()

	return size, nil

}

func extractFilterFromPredicate(p predicate.Predicate) string {

	return strings.ReplaceAll(strings.Fields(p.String())[2], "%)", "")

}

func (m *testHzMap) RemoveAll(_ context.Context, predicate predicate.Predicate) error {

	testMapOperationLock.Lock()
	defer testMapOperationLock.Unlock()

	m.removeAllInvocations++

	if m.returnErrorUponRemoveAll {
		return errors.New("i'm the error everyone told you was never going to happen")
	}

	predicateFilter := extractFilterFromPredicate(predicate)

	m.lastPredicateFilterForRemoveAllInvocation = predicateFilter
	applyFunctionToTestMapContents(m, func(key, value any) bool {
		if strings.HasPrefix(key.(string), predicateFilter) {
			m.data.Delete(key)
		}
		return true
	})

	return nil

}

func (m *testHzMap) EvictAll(_ context.Context) error {

	testMapOperationLock.Lock()
	defer testMapOperationLock.Unlock()

	m.evictAllInvocations++

	if m.returnErrorUponEvictAll {
		return errors.New("totally unexpected error")
	}

	m.data = &sync.Map{}

	return nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnError {
		return errors.New("deliberately thrown error")
	}

	if value, ok := a.testConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	}

	return nil

}

func checkRunnerStateTransitions(expected []runnerState, actual []runnerState) (string, bool) {

	if len(expected) != len(actual) {
		return fmt.Sprintf("expected %d state transition(-s), got %d", len(expected), len(actual)), false
	}

	for i, expectedValue := range expected {
		if actual[i] != expectedValue {
			return fmt.Sprintf("expected '%s' in index '%d', got '%s'", expectedValue, i, actual[i]), false
		}
	}

	return "", true

}
