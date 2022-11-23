package queues

import (
	"hazeltest/status"
	"testing"
)

type dummyTweetRunnerTestLoop struct{}

func (d dummyTweetRunnerTestLoop) init(_ *testLoopConfig[tweet], _ *status.Gatherer) {
	// No-op
}

func (d dummyTweetRunnerTestLoop) run() {
	// No-op
}

func TestRunTweetQueueTests(t *testing.T) {

	t.Log("given the need to test running queue tests in the tweet runner")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsg := "\t\tstate transitions must be correct"
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError: true,
				dummyConfig: nil,
			}
			r := tweetRunner{stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

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
				dummyConfig: map[string]any{
					"queuetests.tweets.enabled": false,
				},
			}
			r := tweetRunner{stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions([]state{start, populateConfigComplete}, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
		t.Log("\twhen hazelcast queue store has been initialized and test loop has executed")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError: false,
				dummyConfig: map[string]interface{}{
					"queuetests.tweets.enabled": true,
				},
			}
			r := tweetRunner{stateList: []state{}, queueStore: dummyHzQueueStore{}, l: dummyTweetRunnerTestLoop{}}

			r.runQueueTests(hzCluster, hzMembers)

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, msg)
			}
		}
	}

}
