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

	t.Log("given a load runner to run queue test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsgStateTransitions := "\t\tstate transitions must be correct"
		genericMsgLatestStateInGatherer := "\t\tlatest state in gatherer must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}

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
				dummyConfig: map[string]any{
					"queueTests.load.enabled": false,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}

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
				dummyConfig: map[string]any{
					"queueTests.load.enabled": true,
				},
			}
			r := loadRunner{assigner: assigner, stateList: []state{}, hzQueueStore: dummyHzQueueStore{}, l: dummyLoadRunnerTestLoop{}}
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
