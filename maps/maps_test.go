package maps

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
	dummyHzMapStore struct{}
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
)

func (d dummyHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzMapStore) InitHazelcastClient(_ context.Context, _ string, _ string, _ []string) {
	// No-op
}

func (d dummyHzMapStore) GetMap(_ context.Context, _ string) (*hazelcast.Map, error) {
	return nil, errors.New("i'm only a dummy implementation")
}

func (a testConfigPropertyAssigner) Assign(keyPath string, assignFunc func(any)) error {

	if a.returnError {
		return errors.New("deliberately thrown error")
	}

	if value, ok := a.dummyConfig[keyPath]; ok {
		assignFunc(value)
	}

	return nil

}

func checkRunnerStateTransitions(expected []state, actual []state) (bool, string) {

	if len(expected) != len(actual) {
		return false, fmt.Sprintf("expected %d state transition(-s), got %d", len(expected), len(actual))
	}

	for i, expectedValue := range expected {
		if actual[i] != expectedValue {
			return false, fmt.Sprintf("expected '%s' in index '%d', got '%s'", expectedValue, i, actual[i])
		}
	}

	return true, ""

}
