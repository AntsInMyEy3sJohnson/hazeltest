package queues

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"hazeltest/hazelcastwrapper"
	"sync"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		testConfig  map[string]any
	}
	testHzQueueStore struct {
		q        *testHzQueue
		behavior *testQueueStoreBehavior
	}
	testHzQueue struct {
		queueCapacity                int
		data                         *list.List
		putInvocations               int
		pollInvocations              int
		destroyInvocations           int
		remainingCapacityInvocations int
		behavior                     *testQueueStoreBehavior
	}
	testQueueStoreBehavior struct {
		returnErrorUponGetQueue, returnErrorUponRemainingCapacity, returnErrorUponPut, returnErrorUponPoll bool
	}
	testHzClientHandler struct {
		getClientInvocations, initClientInvocations, shutdownInvocations int
		hzClusterName                                                    string
		hzClusterMembers                                                 []string
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
	testQueueOperationLock   sync.Mutex
)

func (d *testHzQueue) Clear(_ context.Context) error {
	return nil
}

func (d *testHzQueue) Size(_ context.Context) (int, error) {
	return 0, nil
}

func (d testHzQueueStore) Shutdown(_ context.Context) error {
	return nil
}

func (d testHzQueueStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d testHzQueueStore) GetQueue(_ context.Context, _ string) (hazelcastwrapper.Queue, error) {
	if d.behavior.returnErrorUponGetQueue {
		return nil, errors.New("it is but a scratch")
	}
	return d.q, nil
}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnError {
		return errors.New("lo and behold, here is a deliberately thrown error")
	}

	if value, ok := a.testConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	}

	return nil
}

func (d *testHzQueue) Put(_ context.Context, element any) error {

	testQueueOperationLock.Lock()
	defer testQueueOperationLock.Unlock()

	d.putInvocations++

	if d.behavior.returnErrorUponPut {
		return errors.New("some unexpected error")
	}

	d.data.PushBack(element)

	return nil

}

func (d *testHzQueue) Poll(_ context.Context) (any, error) {

	testQueueOperationLock.Lock()
	defer testQueueOperationLock.Unlock()

	d.pollInvocations++

	if d.behavior.returnErrorUponPoll {
		return nil, errors.New("i find your lack of faith disturbing")
	}

	var element *list.Element
	// A hazelcastwrapper.Queue will return nil for both the value and the error in case a poll is executed
	// against an empty queue --> Replicate behavior here
	if d.data.Len() == 0 {
		// Nothing to poll
		return nil, nil
	} else {
		element = d.data.Front()
		d.data.Remove(element)
		return element, nil
	}

}

func (d *testHzQueue) RemainingCapacity(_ context.Context) (int, error) {

	testQueueOperationLock.Lock()
	defer testQueueOperationLock.Unlock()

	d.remainingCapacityInvocations++

	if d.behavior.returnErrorUponRemainingCapacity {
		return -1, errors.New("resistance is futile")
	}

	if d.queueCapacity < 0 {
		return 0, errors.New("invalid test setup -- queue capacity cannot be negative")
	}

	remaining := d.queueCapacity - d.data.Len()
	if remaining < 0 {
		remaining = 0
	}

	return remaining, nil

}

func (d *testHzQueue) Destroy(_ context.Context) error {

	testQueueOperationLock.Lock()
	{
		d.destroyInvocations++
	}
	testQueueOperationLock.Unlock()

	return nil

}

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
