package queues

import (
	"hazeltest/status"
	"testing"
)

type testLoadRunnerTestLoop struct{}

func (d testLoadRunnerTestLoop) init(_ *testLoopExecution[loadElement], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d testLoadRunnerTestLoop) run() {
	// No-op
}

func TestRunLoadQueueTests(t *testing.T) {

	t.Log("given a load runner to run queue test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsgStateTransitions := "\t\tstate transitions must be correct"
		genericMsgLatestStateInGatherer := "\t\tlatest state in gatherer must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				testConfig:  nil,
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testLoadRunnerTestLoop{}}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runQueueTests(hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions([]state{start}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(r.gatherer, start) {
				t.Log(genericMsgLatestStateInGatherer, checkMark)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, start)
			}

			msg := "\t\tgatherer must have been assigned"
			if gatherer == r.gatherer {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
		t.Log("\twhen runner has been disabled")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"queueTests.load.enabled": false,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testLoadRunnerTestLoop{}}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runQueueTests(hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			latestState := populateConfigComplete
			if msg, ok := checkRunnerStateTransitions([]state{start, latestState}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(r.gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}
		}
		t.Log("\twhen hazelcast queue store has been initialized and test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"queueTests.load.enabled": true,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testLoadRunnerTestLoop{}, hzClientHandler: &testHzClientHandler{}}
			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runQueueTests(hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			latestState := expectedStatesForFullRun[len(expectedStatesForFullRun)-1]
			if latestStatePresentInGatherer(r.gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}
		}
	}

}
