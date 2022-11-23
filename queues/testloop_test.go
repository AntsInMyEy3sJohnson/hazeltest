package queues

import (
	"container/list"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/status"
	"math"
	"testing"
	"time"
)

var (
	aNewHope = []string{
		"Princess Leia",
		"Luke Skywalker",
		"Obi-Wan Kenobi",
		"Han Solo",
		"Chewbacca",
		"Jabba the Hutt",
		"C-3PO",
		"R2-D2",
		"Darth Vader",
	}
	sleepDurationMs     = 10
	sleepConfigDisabled = &sleepConfig{
		enabled:          false,
		durationMs:       0,
		enableRandomness: false,
	}
	sleepConfigEnabled = &sleepConfig{
		enabled:          true,
		durationMs:       sleepDurationMs,
		enableRandomness: false,
	}
	sleepConfigEnabledWithEnabledRandomness = &sleepConfig{
		enabled:          true,
		durationMs:       sleepDurationMs,
		enableRandomness: true,
	}
)

func TestRun(t *testing.T) {

	testSource := "aNewHope"

	t.Log("given the need to test running the queue test loop")
	{
		t.Log("\twhen only put config is provided")
		{
			id := uuid.New()
			qs := assembleDummyQueueStore(false, 9)
			rc := assembleRunnerConfig(true, 1, false, 0, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleTestLoop(id, testSource, qs, &rc)

			tl.run()

			msg := "\t\texpected number of puts must have been executed"
			if qs.q.putInvocations == len(aNewHope) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpoll must not have been executed because test loop did not have enabled poll config"
			if qs.q.pollInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdata must be present in queue"
			if qs.q.data.Len() == len(aNewHope) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loop status must be correct"
			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}

		}
	}

	t.Log("\twhen both put and poll config are provided, and put runs twice as many times as poll")
	{
		id := uuid.New()
		qs := assembleDummyQueueStore(false, 18)
		rc := assembleRunnerConfig(true, 2, true, 1, sleepConfigDisabled, sleepConfigDisabled)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		tl.run()

		msg := "\t\texpected number of puts must have been executed"
		if qs.q.putInvocations == 2*len(aNewHope) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\texpected number of polls must have been executed"
		if qs.q.pollInvocations == len(aNewHope) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\ttest loop status must be correct"
		if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	t.Log("\twhen poll is configured but put is not")
	{
		id := uuid.New()
		qs := assembleDummyQueueStore(false, 1)
		rc := assembleRunnerConfig(false, 0, true, 5, sleepConfigDisabled, sleepConfigDisabled)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		tl.run()

		msg := "\t\tall poll attempts must have been made anyway"
		if qs.q.pollInvocations == 5*len(aNewHope) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\ttest loop status must be correct"
		if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	t.Log("\twhen queue reaches its capacity")
	{
		id := uuid.New()
		queueCapacity := 9
		qs := assembleDummyQueueStore(false, queueCapacity)
		rc := assembleRunnerConfig(true, 2, false, 0, sleepConfigDisabled, sleepConfigDisabled)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		tl.run()

		msg := "\t\tno puts must be executed"
		if qs.q.putInvocations == queueCapacity {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\ttest loop status must be correct"
		if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	var elapsedMsSleepEnabled int64
	t.Log("\twhen enabled sleep config is provided for sleep between runs")
	{
		id := uuid.New()
		qs := assembleDummyQueueStore(false, 9)
		numRunsPutAndPoll := 20
		rc := assembleRunnerConfig(true, numRunsPutAndPoll, true, numRunsPutAndPoll, sleepConfigDisabled, sleepConfigEnabled)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		start := time.Now()
		tl.run()
		elapsedMsSleepEnabled = time.Since(start).Milliseconds()

		msg := "\t\ttest run execution time must be at least number of runs into milliseconds slept after each run"
		if elapsedMsSleepEnabled > int64(numRunsPutAndPoll*sleepDurationMs) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

	}

	t.Log("\twhen enabled sleep config with enabled randomness is provided for sleep between runs")
	{
		id := uuid.New()
		qs := assembleDummyQueueStore(false, 9)
		numRunsPutAndPoll := 20
		rc := assembleRunnerConfig(true, numRunsPutAndPoll, true, numRunsPutAndPoll, sleepConfigDisabled, sleepConfigEnabledWithEnabledRandomness)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		start := time.Now()
		tl.run()
		elapsedMs := time.Since(start).Milliseconds()

		msg := "\t\ttest run execution time must be less than number of runs into given number of milliseconds to sleep " +
			"due to random factor reducing actual time slept"
		if elapsedMs < int64(numRunsPutAndPoll*sleepDurationMs) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

	}

	t.Log("\twhen sleeps are enabled for both put config and poll config")
	{
		// Put and poll operations run concurrently, so when sleeps are enabled for both,
		// the total execution time should still be roughly what it had been if only one of them
		// had received an enabled sleep config
		id := uuid.New()
		qs := assembleDummyQueueStore(false, 9)
		numRunsPutAndPoll := 20
		rc := assembleRunnerConfig(true, numRunsPutAndPoll, true, numRunsPutAndPoll, sleepConfigEnabled, sleepConfigEnabled)
		tl := assembleTestLoop(id, testSource, qs, &rc)

		start := time.Now()
		tl.run()
		elapsedMs := time.Since(start).Milliseconds()

		msg := "\t\ttime slept must be roughly equal to time slept when only one kind of operation received enabled sleep config"
		if math.Abs(float64(elapsedMs-elapsedMsSleepEnabled)) < float64(elapsedMsSleepEnabled)*0.1 {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

	}

}

func statusContainsExpectedValues(status map[string]interface{}, expectedNumQueues int, expectedPutStatus, expectedPollStatus *operationConfig) (bool, string, string) {

	if numQueuesFromStatus, ok := status[statusKeyNumQueues]; ok && numQueuesFromStatus != expectedNumQueues {
		return false, statusKeyNumQueues, fmt.Sprintf("want: %d; got: %d", expectedNumQueues, numQueuesFromStatus)
	}

	if ok, key, detail := operationConfigStatusContainsExpectedValues(status[string(put)].(map[string]interface{}), expectedNumQueues, expectedPutStatus); !ok {
		return false, fmt.Sprintf("%s.%s", string(put), key), detail
	}

	if ok, key, detail := operationConfigStatusContainsExpectedValues(status[string(poll)].(map[string]interface{}), expectedNumQueues, expectedPollStatus); !ok {
		return false, fmt.Sprintf("%s.%s", string(poll), key), detail
	}

	return true, "", ""

}

func operationConfigStatusContainsExpectedValues(status map[string]interface{}, expectedNumQueues int, expectedStatus *operationConfig) (bool, string, string) {

	if enabledFromStatus, ok := status[statusKeyOperationEnabled]; ok && enabledFromStatus != expectedStatus.enabled {
		return false, statusKeyOperationEnabled, fmt.Sprintf("want: %t; got: %t", expectedStatus.enabled, enabledFromStatus)
	}

	if numRunsFromStatus, ok := status[statusKeyNumRuns]; ok && numRunsFromStatus != expectedStatus.numRuns {
		return false, statusKeyNumRuns, fmt.Sprintf("want: %d; got: %d", expectedStatus.numRuns, numRunsFromStatus)
	}

	if batchSizeFromStatus, ok := status[statusKeyBatchSize]; ok && batchSizeFromStatus != expectedStatus.batchSize {
		return false, statusKeyBatchSize, fmt.Sprintf("want: %d; got: %d", expectedStatus.batchSize, batchSizeFromStatus)
	}

	expectedTotalRuns := uint32(expectedNumQueues) * expectedStatus.numRuns
	if totalNumRunsFromStatus, ok := status[statusKeyTotalNumRuns]; ok && totalNumRunsFromStatus != expectedTotalRuns {
		return false, statusKeyTotalNumRuns, fmt.Sprintf("want: %d; got: %d", expectedTotalRuns, totalNumRunsFromStatus)
	}

	return true, "", ""

}

func assembleTestLoop(id uuid.UUID, source string, qs hzQueueStore, rc *runnerConfig) testLoop[string] {

	tlc := assembleTestLoopConfig(id, source, qs, rc)
	tl := testLoop[string]{}
	tl.init(&tlc, status.NewGatherer())

	return tl

}

func assembleTestLoopConfig(id uuid.UUID, source string, qs hzQueueStore, rc *runnerConfig) testLoopConfig[string] {

	return testLoopConfig[string]{
		id:           id,
		source:       source,
		hzQueueStore: qs,
		runnerConfig: rc,
		elements:     aNewHope,
		ctx:          nil,
	}

}

func assembleRunnerConfig(enablePut bool, numRunsPut int, enablePoll bool, numRunsPoll int, sleepConfigPutBetweenRuns *sleepConfig, sleepConfigPollBetweenRuns *sleepConfig) runnerConfig {

	putConfig := operationConfig{
		enabled:                   enablePut,
		numRuns:                   uint32(numRunsPut),
		batchSize:                 1,
		initialDelay:              sleepConfigDisabled,
		sleepBetweenActionBatches: sleepConfigDisabled,
		sleepBetweenRuns:          sleepConfigPutBetweenRuns,
	}
	pollConfig := operationConfig{
		enabled:                   enablePoll,
		numRuns:                   uint32(numRunsPoll),
		batchSize:                 1,
		initialDelay:              sleepConfigDisabled,
		sleepBetweenActionBatches: sleepConfigDisabled,
		sleepBetweenRuns:          sleepConfigPollBetweenRuns,
	}
	return runnerConfig{
		enabled:                     true,
		numQueues:                   1,
		queueBaseName:               "test",
		appendQueueIndexToQueueName: false,
		appendClientIdToQueueName:   false,
		useQueuePrefix:              true,
		queuePrefix:                 "ht_",
		putConfig:                   &putConfig,
		pollConfig:                  &pollConfig,
	}

}

func assembleDummyQueueStore(returnErrorUponGetQueue bool, queueCapacity int) dummyHzQueueStore {

	dummyBackend := &list.List{}

	return dummyHzQueueStore{
		q:                       &dummyHzQueue{data: dummyBackend, queueCapacity: queueCapacity},
		returnErrorUponGetQueue: returnErrorUponGetQueue,
	}

}
