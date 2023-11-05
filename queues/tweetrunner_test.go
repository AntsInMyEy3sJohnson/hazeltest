package queues

import (
	"hazeltest/status"
	"testing"
)

type dummyTweetRunnerTestLoop struct{}

func (d dummyTweetRunnerTestLoop) init(_ *testLoopConfig[tweet], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d dummyTweetRunnerTestLoop) run() {
	// No-op
}

func TestRunTweetQueueTests(t *testing.T) {

	t.Log("given a tweet runner to run queue test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsg := "\t\tstate transitions must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := tweetRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

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
					"queueTests.tweets.enabled": false,
				},
			}
			r := tweetRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

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
					"queueTests.tweets.enabled": true,
				},
			}
			r := tweetRunner{assigner: assigner, stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
