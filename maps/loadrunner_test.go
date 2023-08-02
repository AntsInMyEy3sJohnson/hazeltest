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

func TestRunLoadMapTests(t *testing.T) {

	t.Log("given the need to test running map tests in the load runner")
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
					"mapTests.load.enabled": true,
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
	}

}
