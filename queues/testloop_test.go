package queues

import (
	"container/list"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/status"
	"testing"
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
	sleepConfigDisabled = &sleepConfig{
		enabled:          false,
		durationMs:       0,
		enableRandomness: false,
	}
)

func TestIncreaseValueInStatusRecordFunctions(t *testing.T) {

	statusRecordModificationFunctions := map[statusKey]func(*status.Gatherer, map[statusKey]any){
		statusKeyNumFailedPuts:  increaseNumFailedPuts,
		statusKeyNumFailedPolls: increaseNumFailedPolls,
		statusKeyNumNilPolls:    increaseNumNilPolls,
	}

	statusRecord := map[statusKey]any{
		statusKeyNumFailedPuts:  0,
		statusKeyNumFailedPolls: 0,
		statusKeyNumNilPolls:    0,
	}

	t.Log("given a status gatherer and a status record indicating no operations have failed yet")
	{
		g := &status.Gatherer{Updates: make(chan status.Update, 1)}
		for k, v := range statusRecord {
			t.Log(fmt.Sprintf("\twhen function '%s' is invoked on status record", k))
			{
				f := statusRecordModificationFunctions[k]
				f(g, statusRecord)

				msg := "\t\tcorresponding value in status record must have been updated"
				expected := v.(int) + 1
				actual := statusRecord[k]

				if expected == actual {
					t.Log(msg, checkMark, k)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("after invoking '%s', expected %d, got %d", k, expected, actual))
				}

				msg = "\t\tstatus gatherer must have received corresponding update"
				update := <-g.Updates

				if update.Key == string(k) && update.Value == expected {
					t.Log(msg, checkMark, k)
				} else {
					t.Fatal(msg, ballotX, k)
				}
			}
		}
	}

}

func TestRun(t *testing.T) {

	testSource := "aNewHope"

	t.Log("given the queue test loop")
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
			if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
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
		if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
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
		if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
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
		if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	t.Log("\twhen given sleep between run configs for put and poll are disabled")
	{
		scBetweenRunsPut := &sleepConfig{}
		scBetweenRunsPoll := &sleepConfig{}
		rc := assembleRunnerConfig(true, 20, true, 20, scBetweenRunsPut, scBetweenRunsPoll)
		tl := assembleTestLoop(uuid.New(), testSource, assembleDummyQueueStore(false, 9), &rc)

		numInvocationsSleepBetweenRunsPut := 0
		numInvocationsSleepBetweenRunsPoll := 0
		sleepTimeFunc = func(sc *sleepConfig) int {
			if sc == scBetweenRunsPut {
				numInvocationsSleepBetweenRunsPut++
			} else if sc == scBetweenRunsPoll {
				numInvocationsSleepBetweenRunsPoll++
			}
			return 0
		}

		tl.run()

		msg := "\t\tsleep between runs for put must have zero invocations"
		if numInvocationsSleepBetweenRunsPut == 0 {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\tsleep between runs for poll must have zero invocations"
		if numInvocationsSleepBetweenRunsPoll == 0 {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

	}

	t.Log("\twhen given sleep between run configs for put and poll are enabled")
	{
		numRunsPut := 20
		numRunsPoll := 21
		scBetweenRunsPut := &sleepConfig{enabled: true}
		scBetweenRunsPoll := &sleepConfig{enabled: true}
		rc := assembleRunnerConfig(true, numRunsPut, true, numRunsPoll, scBetweenRunsPut, scBetweenRunsPoll)
		tl := assembleTestLoop(uuid.New(), testSource, assembleDummyQueueStore(false, 9), &rc)

		numInvocationsSleepBetweenRunsPut := 0
		numInvocationsSleepBetweenRunsPoll := 0
		sleepTimeFunc = func(sc *sleepConfig) int {
			if sc == scBetweenRunsPut {
				numInvocationsSleepBetweenRunsPut++
			} else if sc == scBetweenRunsPoll {
				numInvocationsSleepBetweenRunsPoll++
			}
			return 0
		}

		tl.run()

		msg := "\t\tnumber of sleeps between runs for put must be equal to number of runs for put"
		if numInvocationsSleepBetweenRunsPut == numRunsPut {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, numInvocationsSleepBetweenRunsPut)
		}

		msg = "\t\tnumber of sleeps between runs for poll must be equal to number of runs for pull"
		if numInvocationsSleepBetweenRunsPoll == numRunsPoll {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, numInvocationsSleepBetweenRunsPoll)
		}

	}
	t.Log("\twhen initial delay sleep is enabled for both put and poll")
	{
		scInitialDelayPut := &sleepConfig{enabled: true}
		scInitialDelayPoll := &sleepConfig{enabled: true}
		rc := assembleRunnerConfig(true, 20, true, 20, sleepConfigDisabled, sleepConfigDisabled)
		rc.putConfig.initialDelay = scInitialDelayPut
		rc.pollConfig.initialDelay = scInitialDelayPoll
		tl := assembleTestLoop(uuid.New(), testSource, assembleDummyQueueStore(false, 9), &rc)

		numInvocationsInitialDelayPut := 0
		numInvocationsInitialDelayPoll := 0
		sleepTimeFunc = func(sc *sleepConfig) int {
			if sc == scInitialDelayPut {
				numInvocationsInitialDelayPut++
			} else if sc == scInitialDelayPoll {
				numInvocationsInitialDelayPoll++
			}
			return 0
		}

		tl.run()

		msg := "\t\tput initial delay sleep must be invoked exactly once"
		if numInvocationsInitialDelayPut == 1 {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\tpoll initial delay sleep must be invoked exactly once"
		if numInvocationsInitialDelayPoll == 1 {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}
	}

}

func statusContainsExpectedValues(status map[string]any, expectedNumQueues int, expectedPutStatus, expectedPollStatus *operationConfig) (bool, string, string) {

	if numQueuesFromStatus, ok := status[statusKeyNumQueues]; ok && numQueuesFromStatus != expectedNumQueues {
		return false, statusKeyNumQueues, fmt.Sprintf("want: %d; got: %d", expectedNumQueues, numQueuesFromStatus)
	}

	if ok, key, detail := operationConfigStatusContainsExpectedValues(status[string(put)].(map[string]any), expectedNumQueues, expectedPutStatus); !ok {
		return false, fmt.Sprintf("%s.%s", string(put), key), detail
	}

	if ok, key, detail := operationConfigStatusContainsExpectedValues(status[string(poll)].(map[string]any), expectedNumQueues, expectedPollStatus); !ok {
		return false, fmt.Sprintf("%s.%s", string(poll), key), detail
	}

	return true, "", ""

}

func operationConfigStatusContainsExpectedValues(status map[string]any, expectedNumQueues int, expectedStatus *operationConfig) (bool, string, string) {

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
	tl.init(&tlc, &defaultSleeper{}, status.NewGatherer())

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

func assembleRunnerConfig(enablePut bool, numRunsPut int, enablePoll bool, numRunsPoll int, sleepConfigBetweenRunsPut *sleepConfig, sleepConfigBetweenRunsPoll *sleepConfig) runnerConfig {

	putConfig := operationConfig{
		enabled:                   enablePut,
		numRuns:                   uint32(numRunsPut),
		batchSize:                 1,
		initialDelay:              sleepConfigDisabled,
		sleepBetweenActionBatches: sleepConfigDisabled,
		sleepBetweenRuns:          sleepConfigBetweenRunsPut,
	}
	pollConfig := operationConfig{
		enabled:                   enablePoll,
		numRuns:                   uint32(numRunsPoll),
		batchSize:                 1,
		initialDelay:              sleepConfigDisabled,
		sleepBetweenActionBatches: sleepConfigDisabled,
		sleepBetweenRuns:          sleepConfigBetweenRunsPoll,
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
