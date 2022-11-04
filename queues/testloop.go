package queues

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"sync"
	"time"
)

type (
	looper[t any] interface {
		init(lc *testLoopConfig[t])
		run()
	}
	testLoop[t any] struct {
		config *testLoopConfig[t]
	}
	testLoopConfig[t any] struct {
		id           uuid.UUID
		source       string
		hzQueueStore hzQueueStore
		runnerConfig *runnerConfig
		elements     []t
		ctx          context.Context
	}
	operation string
)

const (
	put  = operation("put")
	poll = operation("poll")
)

func (l *testLoop[t]) init(lc *testLoopConfig[t]) {
	l.config = lc
}

func (l *testLoop[t]) run() {

	// TODO Implement integration with api.TestLoopStatus -- but make it so api pulls what it needs rather than the test loop pushing it
	// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/8

	c := l.config
	ctx := l.config.ctx

	var numQueuesWg sync.WaitGroup
	for i := 0; i < c.runnerConfig.numQueues; i++ {
		numQueuesWg.Add(1)
		go func(i int) {
			defer numQueuesWg.Done()

			queueName := l.assembleQueueName(i)
			lp.LogInternalStateEvent(fmt.Sprintf("using queue name '%s' in queue goroutine %d", queueName, i), log.InfoLevel)
			start := time.Now()
			q, err := l.config.hzQueueStore.GetQueue(ctx, queueName)
			if err != nil {
				lp.LogHzEvent("unable to retrieve queue from hazelcast cluster", log.FatalLevel)
			}
			defer func() {
				_ = q.Destroy(ctx)
			}()
			elapsed := time.Since(start).Milliseconds()
			lp.LogTimingEvent("getQueue()", queueName, int(elapsed), log.InfoLevel)

			var putWg sync.WaitGroup
			if c.runnerConfig.putConfig.enabled {
				putWg.Add(1)
				go func() {
					defer putWg.Done()
					l.runElementLoop(l.config.elements, q, put, queueName, i)
				}()

			}

			var pollWg sync.WaitGroup
			if c.runnerConfig.pollConfig.enabled {
				pollWg.Add(1)
				go func() {
					defer pollWg.Done()
					l.runElementLoop(l.config.elements, q, poll, queueName, i)
				}()
			}

			putWg.Wait()
			pollWg.Wait()
		}(i)
	}

	numQueuesWg.Wait()

}

func (l testLoop[t]) runElementLoop(elements []t, q hzQueue, o operation, queueName string, queueNumber int) {

	var config *operationConfig
	var queueFunction func(queue hzQueue, queueName string)
	if o == put {
		config = l.config.runnerConfig.putConfig
		queueFunction = l.putElements
	} else {
		config = l.config.runnerConfig.pollConfig
		queueFunction = l.pollElements
	}

	sleep(config.initialDelay, "initialDelay", queueName, o)

	numRuns := config.numRuns
	for i := uint32(0); i < numRuns; i++ {
		if i > 0 {
			sleep(config.sleepBetweenRuns, "betweenRuns", queueName, o)
		}
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			lp.LogInternalStateEvent(fmt.Sprintf("finished %d of %d %s runs for queue %s in queue goroutine %d", i, numRuns, o, queueName, queueNumber), log.InfoLevel)
		}
		queueFunction(q, queueName)
		lp.LogInternalStateEvent(fmt.Sprintf("finished %sing one set of %d tweets in queue %s after run %d of %d on queue goroutine %d", o, len(elements), queueName, i, numRuns, queueNumber), log.TraceLevel)
	}

	lp.LogInternalStateEvent(fmt.Sprintf("%s test loop done on queue '%s' in queue goroutine %d", o, queueName, queueNumber), log.InfoLevel)

}

func (l testLoop[t]) putElements(q hzQueue, queueName string) {

	elements := l.config.elements
	putConfig := l.config.runnerConfig.putConfig

	for i := 0; i < len(elements); i++ {
		e := elements[i]
		err := q.Put(l.config.ctx, e)
		if err != nil {
			lp.LogInternalStateEvent(fmt.Sprintf("unable to put tweet item into queue '%s': %s", queueName, err), log.WarnLevel)
		} else {
			lp.LogInternalStateEvent(fmt.Sprintf("successfully wrote value to queue '%s': %v", queueName, e), log.TraceLevel)
		}
		if i > 0 && i%putConfig.batchSize == 0 {
			sleep(putConfig.sleepBetweenActionBatches, "betweenActionBatches", queueName, "put")
		}
	}

}

func (l testLoop[t]) pollElements(q hzQueue, queueName string) {

	pollConfig := l.config.runnerConfig.pollConfig

	for i := 0; i < len(l.config.elements); i++ {
		valueFromQueue, err := q.Poll(l.config.ctx)
		if err != nil {
			lp.LogInternalStateEvent(fmt.Sprintf("unable to poll tweet from queue '%s': %s", queueName, err), log.WarnLevel)
		} else if valueFromQueue == nil {
			lp.LogInternalStateEvent(fmt.Sprintf("nothing to poll from queue '%s'", queueName), log.TraceLevel)
		} else {
			lp.LogInternalStateEvent(fmt.Sprintf("retrieved value from queue '%s': %v", queueName, valueFromQueue), log.TraceLevel)
		}
		if i > 0 && i%pollConfig.batchSize == 0 {
			sleep(pollConfig.sleepBetweenActionBatches, "betweenActionBatches", queueName, "poll")
		}
	}

}

func (l testLoop[t]) assembleQueueName(queueIndex int) string {

	c := l.config

	queueName := c.runnerConfig.queueBaseName

	if c.runnerConfig.useQueuePrefix && c.runnerConfig.queuePrefix != "" {
		queueName = fmt.Sprintf("%s%s", c.runnerConfig.queuePrefix, queueName)
	}
	if c.runnerConfig.appendQueueIndexToQueueName {
		queueName = fmt.Sprintf("%s-%d", queueName, queueIndex)
	}
	if c.runnerConfig.appendClientIdToQueueName {
		queueName = fmt.Sprintf("%s-%s", queueName, client.ID())
	}

	return queueName

}

func sleep(sleepConfig *sleepConfig, kind string, queueName string, o operation) {

	if sleepConfig.enabled {
		lp.LogInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds for kind '%s' on queue '%s' for operation '%s'", sleepConfig.durationMs, kind, queueName, o), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.durationMs) * time.Millisecond)
	}

}
