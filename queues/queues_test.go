package queues

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		dummyConfig map[string]interface{}
	}
	dummyHzQueueStore struct{}
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
)

func (d dummyHzQueueStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzQueueStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d dummyHzQueueStore) GetQueue(_ context.Context, _ string) (*hazelcast.Queue, error) {
	return nil, errors.New("it is but a scratch")
}

func (a testConfigPropertyAssigner) Assign(keyPath string, assignFunc func(string, any) error) error {

	if a.returnError {
		return errors.New("lo and behold, here is a deliberately thrown error")
	}

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := assignFunc(keyPath, value); err != nil {
			return err
		}
		return nil
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
