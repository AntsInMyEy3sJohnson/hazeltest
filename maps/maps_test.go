package maps

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		dummyConfig map[string]any
	}
	dummyHzMapStore struct {
		m                     *dummyHzMap
		returnErrorUponGetMap bool
	}
	dummyHzMap struct {
		containsKeyInvocations     int
		setInvocations             int
		getInvocations             int
		removeInvocations          int
		destroyInvocations         int
		sizeInvocations            int
		data                       *sync.Map
		returnErrorUponGet         bool
		returnErrorUponSet         bool
		returnErrorUponContainsKey bool
		returnErrorUponRemove      bool
	}
)

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
	expectedStatesForFullRun = []state{start, populateConfigComplete, checkEnabledComplete, raiseReadyComplete, testLoopStart, testLoopComplete}
	dummyMapOperationLock    sync.Mutex
)

func (d dummyHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzMapStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d dummyHzMapStore) GetMap(_ context.Context, _ string) (hzMap, error) {
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

	return nil, fmt.Errorf("key not contained in map: %s", keyString)

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
		m.data.Range(func(_, _ any) bool {
			size++
			return true
		})
	}
	dummyMapOperationLock.Unlock()

	return size, nil

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

func checkRunnerStateTransitions(expected []state, actual []state) (string, bool) {

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
