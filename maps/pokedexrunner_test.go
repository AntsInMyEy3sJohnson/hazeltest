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

func TestInitializePokemonTestLoop(t *testing.T) {

	t.Log("given a function to initialize the test loop from the provided loop type")
	{
		t.Log("\twhen boundary test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: boundary})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*boundaryTestLoop[pokemon]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen batch test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: batch})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*batchTestLoop[pokemon]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen unknown test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: "saruman"})

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlooper must be nil"
			if l == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestRunPokedexMapTests(t *testing.T) {

	t.Log("given the pokedex runner to run map tests")
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
					"mapTests.pokedex.enabled":       true,
					"mapTests.pokedex.testLoop.type": "batch",
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
