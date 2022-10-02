package maps

import (
	"fmt"
	"testing"
)

type (
	dummyPokedexTestLoop struct{}
)

func (d dummyPokedexTestLoop) init(_ *testLoopConfig[pokemon]) {
	// No-op
}

func (d dummyPokedexTestLoop) run() {
	// No-op
}

func TestRunMapTests(t *testing.T) {

	hzCluster := "awesome-hz-cluster"
	hzMembers := []string{"awesome-hz-cluster-svc.cluster.local"}

	t.Log("given the need to test running map tests")
	{
		t.Log("\twhen the runner configuration cannot be populated")
		genericMsg := fmt.Sprint("\t\tstate transitions must be correct")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := pokedexRunner{stateList: []state{start}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if ok, msg := checkStateTransitions(r.stateList, []state{start}); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen runner has been disabled")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]interface{}{
					"maptests.pokedex.enabled": false,
				},
			}
			r := pokedexRunner{stateList: []state{start}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if ok, msg := checkStateTransitions(r.stateList, []state{start, populateConfigComplete}); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen hazelcast map store has been initialized and test loop has executed")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]interface{}{
					"maptests.pokedex.enabled": true,
				},
			}
			r := pokedexRunner{stateList: []state{start}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			expectedForFullRun := []state{start, populateConfigComplete, checkEnabledComplete, raiseReadyComplete, testLoopStart, testLoopComplete}
			if ok, msg := checkStateTransitions(r.stateList, expectedForFullRun); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}

func checkStateTransitions(expected []state, actual []state) (bool, string) {

	if len(expected) != len(actual) {
		return false, fmt.Sprintf("expected %d state transitions, got %d", len(expected), len(actual))
	}

	for i, expectedValue := range expected {
		if actual[i] != expectedValue {
			return false, fmt.Sprintf("expected '%s' in index '%d', got '%s'", expectedValue, i, actual[i])
		}
	}

	return true, ""

}
