package queues

import (
	"hazeltest/status"
	"testing"
)

type dummyLoadRunnerTestLoop struct{}

func (d dummyLoadRunnerTestLoop) init(_ *testLoopConfig[loadElement], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d dummyLoadRunnerTestLoop) run() {
	// No-op
}

func TestRunLoadQueueTests(t *testing.T) {

	t.Log("given the need to test running queue tests in the load runner")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsg := "\t\tstate transitions must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

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
					"queueTests.load.enabled": false,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start, populateConfigComplete}, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen hazelcast queue store has been initialized and test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]any{
					"queueTests.load.enabled": true,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
