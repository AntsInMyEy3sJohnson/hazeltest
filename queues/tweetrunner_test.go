package queues

import (
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"testing"
)

type testTweetRunnerTestLoop struct{}

func (d testTweetRunnerTestLoop) init(_ *testLoopExecution[tweet], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d testTweetRunnerTestLoop) run() {
	// No-op
}

func TestRunTweetQueueTests(t *testing.T) {

	t.Log("given a tweet runner to run queue test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsgStateTransitions := "\t\tstate transitions must be correct"
		genericMsgLatestStateInGatherer := "\t\tlatest state in gatherer must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				testConfig:  nil,
			}
			r := tweetRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testTweetRunnerTestLoop{}}
			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runQueueTests(hzCluster, hzMembers, gatherer, initTestQueueStore)
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
					"queueTests.tweets.enabled": false,
				},
			}
			r := tweetRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testTweetRunnerTestLoop{}}
			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runQueueTests(hzCluster, hzMembers, gatherer, initTestQueueStore)
			gatherer.StopListen()

			latestState := populateConfigComplete
			if msg, ok := checkRunnerStateTransitions([]state{start, populateConfigComplete}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}

		}
		t.Log("\twhen test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"queueTests.tweets.enabled": true,
				},
			}
			ch := &testHzClientHandler{}
			r := tweetRunner{assigner: assigner, stateList: []state{}, hzQueueStore: testHzQueueStore{}, l: testTweetRunnerTestLoop{}, hzClientHandler: ch}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			qs := &testHzQueueStore{observations: &testQueueStoreObservations{}}
			r.runQueueTests(hzCluster, hzMembers, gatherer, func(_ hazelcastwrapper.HzClientHandler) hazelcastwrapper.QueueStore {
				qs.observations.numInitInvocations++
				return qs
			})
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			latestState := expectedStatesForFullRun[len(expectedStatesForFullRun)-1]
			if latestStatePresentInGatherer(gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}

			msg := "\t\thazelcast client handler must have initialized hazelcast client once"
			if ch.initClientInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.initClientInvocations)
			}

			msg = "\t\thazelcast client handler must have performed shutdown of hazelcast client once"
			if ch.shutdownInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.shutdownInvocations)
			}

			msg = "\t\tqueue store must have been initialized once"
			if qs.observations.numInitInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.observations.numInitInvocations)
			}

		}
	}

}
