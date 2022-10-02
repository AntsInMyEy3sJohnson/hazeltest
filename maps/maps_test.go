package maps

import (
	"context"
	"errors"
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
	runnerKeyPath = "testRunner"
	mapPrefix     = "t_"
	mapBaseName   = "test"
)

func (d dummyHzMapStore) Shutdown(_ context.Context) error {
	return nil
}

func (d dummyHzMapStore) InitHazelcast(_ context.Context, _ string, _ string, _ []string) {
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
