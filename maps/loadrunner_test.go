package maps

import (
	"fmt"
	"hazeltest/status"
	"testing"
)

type dummyLoadTestLoop struct{}

func (d dummyLoadTestLoop) init(_ *testLoopExecution[loadElement], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d dummyLoadTestLoop) run() {
	// No-op
}

func TestInitializeLoadElementTestLoop(t *testing.T) {

	t.Log("given a function to initialize the test loop from the provided loop type")
	{
		t.Log("\twhen boundary test loop type is provided")
		{
			l, err := initializeLoadElementTestLoop(&runnerConfig{loopType: boundary})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*boundaryTestLoop[loadElement]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen batch test loop type is provided")
		{
			l, err := initializeLoadElementTestLoop(&runnerConfig{loopType: batch})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*batchTestLoop[loadElement]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen unknown test loop type is provided")
		{
			l, err := initializeLoadElementTestLoop(&runnerConfig{loopType: "saruman"})

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

func TestRunLoadMapTests(t *testing.T) {

	t.Log("given a load runner to run map test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsg := fmt.Sprint("\t\tstate transitions must be correct")
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyLoadTestLoop{}}

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
					"mapTests.load.enabled": false,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyLoadTestLoop{}}

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
					"mapTests.load.enabled":       true,
					"mapTests.load.testLoop.type": "batch",
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyLoadTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}

		t.Log("\twhen test loop cannot be initialized")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]any{
					"mapTests.load.enabled":       true,
					"mapTests.load.testLoop.type": "awesome-non-existing-test-loop-type",
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, mapStore: dummyHzMapStore{}, l: dummyLoadTestLoop{}}

			r.runMapTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start}, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
