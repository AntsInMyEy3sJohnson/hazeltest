package maps

import (
	"hazeltest/status"
	"testing"
)

type dummyPokedexTestLoop struct{}

func (d dummyPokedexTestLoop) init(_ *testLoopExecution[pokemon], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d dummyPokedexTestLoop) run() {
	// No-op
}

func TestRunPokedexMapTests(t *testing.T) {

	t.Log("given the need to test running map tests in the pokedex runner")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsg := "\t\tstate transitions must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := pokedexRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start}, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen runner has been disabled")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]any{
					"mapTests.pokedex.enabled": false,
				},
			}
			r := pokedexRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start, populateConfigComplete}, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen hazelcast map store has been initialized and test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]any{
					"mapTests.pokedex.enabled": true,
				},
			}
			r := pokedexRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
