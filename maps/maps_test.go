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
		dummyConfig map[string]any
	}
	dummyHzClientHandler struct {
		getClientInvocations, initClientInvocations, shutdownInvocations int
	}
	dummyHzMapStore struct {
		m                     *dummyHzMap
		returnErrorUponGetMap bool
	}
	boundaryMonitoring struct {
		upperBoundaryNumElements       int
		lowerBoundaryNumElements       int
		upperBoundaryThresholdViolated bool
		lowerBoundaryThresholdViolated bool
		upperBoundaryViolationValue    int
		lowerBoundaryViolationValue    int
	}
	dummyHzMap struct {
		containsKeyInvocations                    int
		setInvocations                            int
		getInvocations                            int
		removeInvocations                         int
		destroyInvocations                        int
		sizeInvocations                           int
		removeAllInvocations                      int
		evictAllInvocations                       int
		lastPredicateFilterForRemoveAllInvocation string
		// TODO Use regular map rather than sync.Map because access to dummyHzMap properties has to ge guarded by lock anyway
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

func (d dummyHzClientHandler) GetClient() *hazelcast.Client {
	d.getClientInvocations++
	return nil
}

func (d dummyHzClientHandler) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	d.initClientInvocations++
}

func (d dummyHzClientHandler) Shutdown(_ context.Context) error {
	d.shutdownInvocations++
	return nil
}

func (m *dummyHzMap) SetWithTTLAndMaxIdle(_ context.Context, _, _ any, _ time.Duration, _ time.Duration) error {
	return nil
}

func (m *dummyHzMap) TryLock(_ context.Context, _ any) (bool, error) {
	return false, nil
}

func (m *dummyHzMap) Unlock(_ context.Context, _ any) error {
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
	dummyMapOperationLock    sync.Mutex
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

func (d dummyHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzMapStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d dummyHzMapStore) GetMap(_ context.Context, _ string) (hazelcastwrapper.Map, error) {
	if d.returnErrorUponGetMap {
		return nil, errors.New("i was told to throw an error")
	}
	return d.m, nil
}

func (m *dummyHzMap) ContainsKey(_ context.Context, key any) (bool, error) {

	dummyMapOperationLock.Lock()
	{
		m.containsKeyInvocations++
	}
	dummyMapOperationLock.Unlock()

	if m.returnErrorUponContainsKey {
		return false, errors.New("a deliberately returned error")
	}

	keyString, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}
	if _, ok := m.data.Load(keyString); ok {
		return true, nil
	}

	return false, nil

}

func (m *dummyHzMap) Set(_ context.Context, key any, value any) error {

	dummyMapOperationLock.Lock()
	{
		m.setInvocations++
	}
	dummyMapOperationLock.Unlock()

	if m.returnErrorUponSet {
		return errors.New("also a deliberately thrown error")
	}

	keyString, ok := key.(string)
	if !ok {
		return fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
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

func (m *dummyHzMap) Get(_ context.Context, key any) (any, error) {

	dummyMapOperationLock.Lock()
	{
		m.getInvocations++
	}
	dummyMapOperationLock.Unlock()

	if m.returnErrorUponGet {
		return nil, errors.New("awesome dummy error")
	}

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}

	value, _ := m.data.Load(keyString)
	return value, nil

}

func applyFunctionToDummyMapContents(m *dummyHzMap, fn func(key, value any) bool) {

	m.data.Range(func(key, value any) bool {
		return fn(key, value)
	})

}

func (m *dummyHzMap) Remove(_ context.Context, key any) (any, error) {

	dummyMapOperationLock.Lock()
	{
		m.removeInvocations++
	}
	dummyMapOperationLock.Unlock()

	if m.returnErrorUponRemove {
		return nil, errors.New("lo and behold, an error")
	}

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}

	if value, ok := m.data.Load(keyString); ok {
		defer m.data.Delete(keyString)
		return value, nil
	}

	// A Hazelcast map won't actually return an error upon request to delete a non-existing key,
	// so it's more accurate to mirror this behavior here, and not return an error
	return nil, nil

}

func (m *dummyHzMap) Destroy(_ context.Context) error {

	dummyMapOperationLock.Lock()
	{
		m.destroyInvocations++
	}
	dummyMapOperationLock.Unlock()

	return nil

}

func (m *dummyHzMap) Size(_ context.Context) (int, error) {

	size := 0
	dummyMapOperationLock.Lock()
	{
		m.sizeInvocations++
		applyFunctionToDummyMapContents(m, func(key, value any) bool {
			size++
			return true
		})
	}
	dummyMapOperationLock.Unlock()

	return size, nil

}

func extractFilterFromPredicate(p predicate.Predicate) string {

	return strings.ReplaceAll(strings.Fields(p.String())[2], "%)", "")

}

func (m *dummyHzMap) RemoveAll(_ context.Context, predicate predicate.Predicate) error {

	dummyMapOperationLock.Lock()
	defer dummyMapOperationLock.Unlock()

	m.removeAllInvocations++

	if m.returnErrorUponRemoveAll {
		return errors.New("i'm the error everyone told you was never going to happen")
	}

	predicateFilter := extractFilterFromPredicate(predicate)

	m.lastPredicateFilterForRemoveAllInvocation = predicateFilter
	applyFunctionToDummyMapContents(m, func(key, value any) bool {
		if strings.HasPrefix(key.(string), predicateFilter) {
			m.data.Delete(key)
		}
		return true
	})

	return nil

}

func (m *dummyHzMap) EvictAll(_ context.Context) error {

	dummyMapOperationLock.Lock()
	defer dummyMapOperationLock.Unlock()

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

	if value, ok := a.dummyConfig[keyPath]; ok {
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
