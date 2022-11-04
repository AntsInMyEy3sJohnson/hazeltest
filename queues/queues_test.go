package queues

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		dummyConfig map[string]interface{}
	}
	dummyHzQueueStore struct {
		q                       *dummyHzQueue
		returnErrorUponGetQueue bool
	}
	dummyHzQueue struct {
		queueCapacity      int
		data               *list.List
		putInvocations     int
		pollInvocations    int
		destroyInvocations int
	}
)

const (
	checkMark     = "\u2713"
	ballotX       = "\u2717"
	runnerKeyPath = "testQueueRunner"
	queuePrefix   = "t_"
	queueBaseName = "test"
)

var (
	hzCluster                = "awesome-hz-cluster"
	hzMembers                = []string{"awesome-hz-cluster-svc.cluster.local"}
	expectedStatesForFullRun = []state{start, populateConfigComplete, checkEnabledComplete, raiseReadyComplete, testLoopStart, testLoopComplete}
	dummyQueueOperationLock  sync.Mutex
)

func (d dummyHzQueueStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzQueueStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d dummyHzQueueStore) GetQueue(_ context.Context, _ string) (hzQueue, error) {
	if d.returnErrorUponGetQueue {
		return nil, errors.New("it is but a scratch")
	}
	return d.q, nil
}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnError {
		return errors.New("lo and behold, here is a deliberately thrown error")
	}

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	}

	return nil
}

func (d *dummyHzQueue) Put(_ context.Context, element interface{}) error {

	dummyQueueOperationLock.Lock()
	{
		d.putInvocations++
	}
	dummyQueueOperationLock.Unlock()

	d.data.PushBack(element)
	return nil

}

func (d *dummyHzQueue) Poll(_ context.Context) (interface{}, error) {

	dummyQueueOperationLock.Lock()
	{
		d.pollInvocations++
	}
	dummyQueueOperationLock.Unlock()

	// A hazelcast.Queue will return nil for both the value and the error in case a poll is executed
	// against an empty queue --> Replicate behavior here
	if d.data.Len() == 0 {
		// Nothing to poll
		return nil, nil
	}

	element := d.data.Front()
	d.data.Remove(element)

	return element, nil

}

func (d *dummyHzQueue) RemainingCapacity(_ context.Context) (int, error) {

	if d.queueCapacity < 0 {
		return 0, errors.New("invalid test setup -- queue capacity cannot be negative")
	}

	if d.queueCapacity == 0 {
		return math.MaxInt, nil
	}

	return d.queueCapacity - d.data.Len(), nil

}

func (d *dummyHzQueue) Destroy(_ context.Context) error {

	dummyQueueOperationLock.Lock()
	{
		d.destroyInvocations++
	}
	dummyQueueOperationLock.Unlock()

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
