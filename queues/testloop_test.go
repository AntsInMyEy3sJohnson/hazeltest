package queues

import (
	"container/list"
	"github.com/google/uuid"
	"testing"
)

var aNewHope = []string{
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

func TestRun(t *testing.T) {

	testSource := "aNewHope"

	t.Log("given the need to test running the queue test loop")
	{
		t.Log("\twhen only a put config is provided")
		{
			id := uuid.New()
			qs := assembleDummyQueueStore(false)
			rc := assembleRunnerConfig()
			tl := assembleTestLoop(id, testSource, qs, &rc)
			tlc := assembleTestLoopConfig(id, testSource, qs, &rc)
			tl.init(&tlc)

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

		}
	}

}

func assembleTestLoop(id uuid.UUID, source string, qs hzQueueStore, rc *runnerConfig) testLoop[string] {

	tlc := assembleTestLoopConfig(id, source, qs, rc)
	tl := testLoop[string]{}
	tl.init(&tlc)

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

func assembleRunnerConfig() runnerConfig {

	disabledSleepConfig := sleepConfig{
		enabled:    false,
		durationMs: 0,
	}
	putConfig := operationConfig{
		enabled:                   true,
		numRuns:                   1,
		batchSize:                 1,
		initialDelay:              &disabledSleepConfig,
		sleepBetweenActionBatches: &disabledSleepConfig,
		sleepBetweenRuns:          &disabledSleepConfig,
	}
	pollConfig := operationConfig{
		enabled:                   false,
		numRuns:                   0,
		batchSize:                 0,
		initialDelay:              nil,
		sleepBetweenActionBatches: nil,
		sleepBetweenRuns:          nil,
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

func assembleDummyQueueStore(returnErrorUponGetQueue bool) dummyHzQueueStore {

	dummyBackend := &list.List{}

	return dummyHzQueueStore{
		q:                       &dummyHzQueue{data: dummyBackend},
		returnErrorUponGetQueue: returnErrorUponGetQueue,
	}

}
