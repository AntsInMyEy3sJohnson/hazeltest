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
		dummyConfig map[string]interface{}
	}
	dummyHzMapStore struct {
		m              *dummyHzMap
		returnDummyMap bool
		invocations    int
	}
	dummyHzMap struct {
		containsKeyInvocations int
		setInvocations         int
		getInvocations         int
		removeInvocations      int
		destroyInvocations     int
		data                   map[string]interface{}
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
	dummyMapOperationLock.Lock()
	{
		d.invocations++
	}
	dummyMapOperationLock.Unlock()
	if d.returnDummyMap {
		return d.m, nil
	}
	return nil, errors.New("i'm only a dummy implementation")
}

func (m *dummyHzMap) ContainsKey(_ context.Context, key interface{}) (bool, error) {

	dummyMapOperationLock.Lock()
	{
		m.containsKeyInvocations++
	}
	dummyMapOperationLock.Unlock()

	if m.data == nil {
		return false, errors.New("dummy data source not initialized")
	}

	keyString, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}
	if _, ok := m.data[keyString]; ok {
		return true, nil
	}

	return false, nil

}

func (m *dummyHzMap) Set(_ context.Context, key interface{}, value interface{}) error {

	dummyMapOperationLock.Lock()
	{
		m.setInvocations++
	}
	dummyMapOperationLock.Unlock()

	keyString, ok := key.(string)
	if !ok {
		return fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}

	m.data[keyString] = value

	return nil

}

func (m *dummyHzMap) Get(_ context.Context, key interface{}) (interface{}, error) {

	dummyMapOperationLock.Lock()
	{
		m.getInvocations++
	}
	dummyMapOperationLock.Unlock()

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}

	return m.data[keyString], nil

}

func (m *dummyHzMap) Remove(_ context.Context, key interface{}) (interface{}, error) {

	dummyMapOperationLock.Lock()
	{
		m.removeInvocations++
	}
	dummyMapOperationLock.Unlock()

	keyString, ok := key.(string)
	if !ok {
		return nil, fmt.Errorf("unable to parse given key into string for querying dummy data source: %v", key)
	}

	if value, ok := m.data[keyString]; ok {
		defer delete(m.data, keyString)
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
