package queues

import (
	"container/list"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"sync"
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
	testSource = "aNewHope"
)

func TestQueueTestLoopCountersTrackerInit(t *testing.T) {

	t.Log("given the tracker's init function")
	{
		t.Log("\twhen init method is invoked")
		{
			ct := &queueTestLoopCountersTracker{}
			g := status.NewGatherer()

			go g.Listen(make(chan struct{}, 1))
			ct.init(g)
			g.StopListen()

			msg := "\t\tgatherer must have been assigned"
			if ct.gatherer == g {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tall status keys must have been inserted into status record"
			initialCounterValue := 0
			for _, v := range counters {
				if counter, ok := ct.counters[v]; ok && counter == initialCounterValue {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}

			waitForStatusGatheringDone(g)
			msg = "\t\tgatherer must have received all status keys with initial values"
			statusCopy := g.AssembleStatusCopy()

			for _, v := range counters {
				if ok, detail := expectedStatusPresent(statusCopy, v, initialCounterValue); ok {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}
		}
	}

}

func TestQueueTestLoopCountersTrackerIncreaseCounter(t *testing.T) {

	t.Log("given a method for increasing the value of a specific counter")
	{
		t.Log("\twhen method is not invoked concurrently")
		{
			ct := &queueTestLoopCountersTracker{
				counters: make(map[statusKey]int),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			for _, v := range counters {
				t.Log(fmt.Sprintf("\t\twhen initial value is zero for counter '%s' and increase method is invoked", v))
				{
					ct.counters[v] = 0
					ct.increaseCounter(v)

					msg := "\t\t\tcounter increase must be reflected in counter tracker's state"
					if ct.counters[v] == 1 {
						t.Log(msg, checkMark, v)
					} else {
						t.Fatal(msg, ballotX, v)
					}

					msg = "\t\t\tcorresponding update must have been sent to status gatherer"
					update := <-ct.gatherer.Updates
					if update.Key == string(v) && update.Value == 1 {
						t.Log(msg, checkMark, v)
					} else {
						t.Fatal(msg, ballotX, v)
					}
				}
			}
		}
		t.Log("\twhen multiple goroutines want to increase a counter")
		{
			wg := sync.WaitGroup{}
			ct := &queueTestLoopCountersTracker{
				counters: make(map[statusKey]int),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			go ct.gatherer.Listen(make(chan struct{}, 1))
			ct.counters[statusKeyNumFailedPuts] = 0
			numInvokingGoroutines := 100
			for i := 0; i < numInvokingGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ct.increaseCounter(statusKeyNumFailedPuts)
				}()
			}
			wg.Wait()
			ct.gatherer.StopListen()

			msg := "\t\tfinal counter value must be equal to number of invoking goroutines"

			if ct.counters[statusKeyNumFailedPuts] == numInvokingGoroutines {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestPutElements(t *testing.T) {

	t.Log("given a hazelcast queue and a status record")
	{
		t.Log("\twhen check for remaining capacity yields error")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{
				returnErrorUponGetQueue:          false,
				returnErrorUponRemainingCapacity: true,
			}, 9)
			rc := assembleRunnerConfig(true, 1, false, 1, sleepConfigDisabled, sleepConfigDisabled)
			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.putElements(qs.q, "awesomeQueue")
			gatherer.StopListen()

			msg := "\t\tnumber of checks for remaining queue capacity must be equal to number of elements in source data"
			expectedNumRemainingCapacityInvocations := len(aNewHope)
			if qs.q.remainingCapacityInvocations == expectedNumRemainingCapacityInvocations {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tno put must have been executed"
			if qs.q.putInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.q.putInvocations)
			}

			msg = fmt.Sprintf("\t\tstatus gatherer must have received update about %d failed capacity checks", expectedNumRemainingCapacityInvocations)
			waitForStatusGatheringDone(gatherer)

			statusCopy := gatherer.AssembleStatusCopy()
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedCapacityChecks, expectedNumRemainingCapacityInvocations); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}

		t.Log("\twhen queue has no remaining capacity")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 0)
			rc := assembleRunnerConfig(true, 1, false, 1, sleepConfigDisabled, sleepConfigDisabled)
			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.putElements(qs.q, "awesomeQueue")
			gatherer.StopListen()

			msg := "\t\tstatus gatherer must indicate zero failed remaining capacity checks"
			waitForStatusGatheringDone(gatherer)

			statusCopy := tl.gatherer.AssembleStatusCopy()
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedCapacityChecks, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tnumber of queue full events in status gatherer must be equal to number of elements in queue test loop source data"
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumQueueFullEvents, len(aNewHope)); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tnumber of executed put operations must be zero"
			if qs.q.putInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.q.putInvocations)
			}
		}

		t.Log("\twhen queue has remaining capacity and puts fail")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{
				returnErrorUponPut: true,
			}, 42)
			rc := assembleRunnerConfig(true, 1, false, 1, sleepConfigDisabled, sleepConfigDisabled)
			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.putElements(qs.q, "anotherAwesomeQueue")
			gatherer.StopListen()

			msg := "\t\tnumber of executed put attempts must be equal to number of elements in test loop source data"
			expectedNumPuts := len(aNewHope)
			if qs.q.putInvocations == expectedNumPuts {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.q.putInvocations)
			}

			msg = fmt.Sprintf("\t\tstatus gatherer must indicate %d failed put attempts", expectedNumPuts)

			waitForStatusGatheringDone(gatherer)

			statusCopy := gatherer.AssembleStatusCopy()
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedPuts, expectedNumPuts); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}

		t.Log("\twhen queue has remaining capacity and puts succeed")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 42)
			rc := assembleRunnerConfig(true, 1, false, 1, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, status.NewGatherer())

			go tl.gatherer.Listen(make(chan struct{}, 1))
			tl.putElements(qs.q, "yetAnotherAwesomeQueue")
			tl.gatherer.StopListen()

			msg := "\t\tnumber of put invocations must be equal to number of elements in test loop source data"
			if qs.q.putInvocations == len(aNewHope) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.q)
			}

			waitForStatusGatheringDone(tl.gatherer)

			msg = "\t\tstatus gatherer must indicate zero failed put attempts"
			if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedPuts, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}
	}

}

func TestPollElements(t *testing.T) {

	t.Log("given a hazelcast queue and a status record")
	{
		t.Log("\twhen poll operations yield error")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{
				returnErrorUponPoll: true,
			}, 9)
			rc := assembleRunnerConfig(false, 0, true, 1, sleepConfigDisabled, sleepConfigDisabled)

			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.pollElements(qs.q, "yeehawQueue")
			gatherer.StopListen()

			msg := "\t\tnumber of poll attempts must be equal to number of elements in test loop source data"
			expectedPolls := len(aNewHope)
			if qs.q.pollInvocations == expectedPolls {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, qs.q.pollInvocations)
			}

			msg = fmt.Sprintf("\t\tstatus gatherer must indicate %d failed poll attempts", expectedPolls)
			waitForStatusGatheringDone(gatherer)

			statusCopy := gatherer.AssembleStatusCopy()
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedPolls, expectedPolls); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}

		t.Log("\twhen poll operations succeed and polled value is always nil")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 9)
			rc := assembleRunnerConfig(false, 0, true, 1, sleepConfigDisabled, sleepConfigDisabled)

			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.pollElements(qs.q, "anotherYeehawQueue")
			gatherer.StopListen()

			msg := "\t\tstatus gatherer must indicate zero failed polls"
			waitForStatusGatheringDone(gatherer)

			statusCopy := gatherer.AssembleStatusCopy()
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedPolls, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			expectedNumNilPolls := len(aNewHope)
			msg = fmt.Sprintf("\t\tstatus gatherer must indicate %d nil polls", expectedNumNilPolls)
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumNilPolls, expectedNumNilPolls); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}

		t.Log("\twhen poll operations succeed and retrieved value is not nil")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 9)
			for _, v := range aNewHope {
				qs.q.data.PushFront(v)
			}
			rc := assembleRunnerConfig(false, 0, true, 1, sleepConfigDisabled, sleepConfigDisabled)

			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.pollElements(qs.q, "yetAnotherYeehawQueue")
			gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			statusCopy := tl.gatherer.AssembleStatusCopy()
			msg := "\t\tstatus gatherer must indicate zero failed polls"
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedPolls, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tstatus gatherer must indicate zero nil polls"
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumNilPolls, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}
	}

}

func TestRun(t *testing.T) {

	t.Log("given the queue test loop")
	{
		t.Log("\twhen only put config is provided")
		{
			qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 9)
			rc := assembleRunnerConfig(true, 1, false, 0, sleepConfigDisabled, sleepConfigDisabled)

			gatherer := status.NewGatherer()
			tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

			go gatherer.Listen(make(chan struct{}, 1))
			tl.run()
			gatherer.StopListen()

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
			waitForStatusGatheringDone(gatherer)
			statusCopy := tl.gatherer.AssembleStatusCopy()
			if ok, key, detail := statusContainsExpectedValues(statusCopy, rc.numQueues, rc.putConfig, rc.pollConfig); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}

		}
	}

	t.Log("\twhen both put and poll config are provided, and put runs twice as many times as poll")
	{
		qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 18)
		rc := assembleRunnerConfig(true, 2, true, 1, sleepConfigDisabled, sleepConfigDisabled)

		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

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
		waitForStatusGatheringDone(gatherer)
		if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	t.Log("\twhen poll is configured but put is not")
	{
		qs := assembleTestQueueStore(&testQueueStoreBehavior{}, 1)
		rc := assembleRunnerConfig(false, 0, true, 5, sleepConfigDisabled, sleepConfigDisabled)

		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

		msg := "\t\tall poll attempts must have been made anyway"
		if qs.q.pollInvocations == 5*len(aNewHope) {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\ttest loop status must be correct"
		waitForStatusGatheringDone(gatherer)
		if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), rc.numQueues, rc.putConfig, rc.pollConfig); ok {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX, key, detail)
		}

	}

	t.Log("\twhen queue reaches its capacity")
	{
		queueCapacity := 9
		qs := assembleTestQueueStore(&testQueueStoreBehavior{}, queueCapacity)
		rc := assembleRunnerConfig(true, 2, false, 0, sleepConfigDisabled, sleepConfigDisabled)

		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, qs, &rc, gatherer)

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

		msg := "\t\tno puts must be executed"
		if qs.q.putInvocations == queueCapacity {
			t.Log(msg, checkMark)
		} else {
			t.Fatal(msg, ballotX)
		}

		msg = "\t\ttest loop status must be correct"
		waitForStatusGatheringDone(gatherer)
		statusCopy := tl.gatherer.AssembleStatusCopy()
		if ok, key, detail := statusContainsExpectedValues(statusCopy, rc.numQueues, rc.putConfig, rc.pollConfig); ok {
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
		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, assembleTestQueueStore(&testQueueStoreBehavior{}, 9), &rc, gatherer)

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

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

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
		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, assembleTestQueueStore(&testQueueStoreBehavior{}, 9), &rc, gatherer)

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

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

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
		gatherer := status.NewGatherer()
		tl := assembleTestLoop(uuid.New(), testSource, assembleTestQueueStore(&testQueueStoreBehavior{}, 9), &rc, gatherer)

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

		go gatherer.Listen(make(chan struct{}, 1))
		tl.run()
		gatherer.StopListen()

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

func expectedStatusPresent(statusCopy map[string]any, expectedKey statusKey, expectedValue int) (bool, string) {

	recordedValue := statusCopy[string(expectedKey)].(int)

	if recordedValue == expectedValue {
		return true, ""
	} else {
		return false, fmt.Sprintf("expected %d, got %d\n", expectedValue, recordedValue)
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

func assembleTestLoop(id uuid.UUID, source string, qs hazelcastwrapper.QueueStore, rc *runnerConfig, g *status.DefaultGatherer) testLoop[string] {

	tlc := assembleTestLoopConfig(id, source, qs, rc)
	tl := testLoop[string]{}
	tl.init(&tlc, &defaultSleeper{}, g)

	return tl

}

func assembleTestLoopConfig(id uuid.UUID, source string, qs hazelcastwrapper.QueueStore, rc *runnerConfig) testLoopExecution[string] {

	return testLoopExecution[string]{
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

func assembleTestQueueStore(b *testQueueStoreBehavior, queueCapacity int) testHzQueueStore {

	testBackend := &list.List{}

	return testHzQueueStore{
		q:        &testHzQueue{data: testBackend, queueCapacity: queueCapacity, behavior: b},
		behavior: b,
	}

}
