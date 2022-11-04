package maps

import (
	"testing"
)

type dummyPokedexTestLoop struct{}

func (d dummyPokedexTestLoop) init(_ *testLoopConfig[pokemon]) {
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
			propertyAssigner = testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := pokedexRunner{stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start}, r.stateList); ok {
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
			r := pokedexRunner{stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start, populateConfigComplete}, r.stateList); ok {
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
			r := pokedexRunner{stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyPokedexTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
