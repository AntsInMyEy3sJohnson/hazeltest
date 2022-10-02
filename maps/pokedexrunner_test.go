package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"testing"
)

type (
	dummyHzMapStore struct{}
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

func TestRunMapTests(t *testing.T) {

	hzCluster := "awesome-hz-cluster"
	hzMembers := []string{"awesome-hz-cluster-svc.cluster.local"}

	t.Log("given the need to test running map tests")
	{
		t.Log("\twhen the runner configuration cannot be populated")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError:   true,
				runnerKeyPath: "",
				dummyConfig:   nil,
			}
			r := pokedexRunner{ls: start, mapStore: dummyHzMapStore{}}

			r.runMapTests(hzCluster, hzMembers)

			msg := fmt.Sprintf("\t\tlast state must be '%s'", start)
			if r.ls == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen runner has been disabled")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError:   false,
				runnerKeyPath: runnerKeyPath,
				dummyConfig: map[string]interface{}{
					"maptests.pokedex.enabled": false,
				},
			}
			r := pokedexRunner{ls: start, mapStore: dummyHzMapStore{}}

			r.runMapTests(hzCluster, hzMembers)

			msg := fmt.Sprintf("\t\tlast state must be '%s'", populateConfigComplete)

			if r.ls == populateConfigComplete {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}
