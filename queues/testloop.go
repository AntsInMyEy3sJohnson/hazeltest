package queues

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"math/rand"
	"sync"
	"time"
)

type (
	evaluateTimeToSleep func(sc *sleepConfig) int
	looper[t any]       interface {
		init(lc *testLoopExecution[t], s sleeper, g *status.DefaultGatherer)
		run()
	}
	sleeper interface {
		sleep(sc *sleepConfig, sf evaluateTimeToSleep, kind, queueName, runnerName string, o operation)
	}
	counterTracker interface {
		init(gatherer *status.DefaultGatherer)
		increaseCounter(sk statusKey)
	}
	testLoop[t any] struct {
		tle      *testLoopExecution[t]
		s        sleeper
		gatherer *status.DefaultGatherer
		ct       counterTracker
	}
	testLoopExecution[t any] struct {
		id           uuid.UUID
		runnerName   string
		source       string
		hzQueueStore hazelcastwrapper.QueueStore
		runnerConfig *runnerConfig
		elements     []t
		ctx          context.Context
	}
	operation                    string
	defaultSleeper               struct{}
	queueTestLoopCountersTracker struct {
		counters map[statusKey]int
		l        sync.Mutex
		gatherer *status.DefaultGatherer
	}
)

const (
	statusKeyOperationEnabled = "enabled"
	statusKeyNumQueues        = "numQueues"
	statusKeyNumRuns          = "numRuns"
	statusKeyBatchSize        = "batchSize"
	statusKeyTotalNumRuns     = "totalNumRuns"
)

const (
	put  = operation("put")
	poll = operation("poll")
)

const (
	statusKeyNumFailedPuts           statusKey = "numFailedPuts"
	statusKeyNumFailedPolls          statusKey = "numFailedPolls"
	statusKeyNumNilPolls             statusKey = "numNilPolls"
	statusKeyNumFailedCapacityChecks statusKey = "numFailedCapacityChecks"
	statusKeyNumQueueFullEvents      statusKey = "numQueueFullEvents"
)

var (
	sleepTimeFunc evaluateTimeToSleep = func(sc *sleepConfig) int {
		var sleepDuration int
		if sc.enableRandomness {
			sleepDuration = rand.Intn(sc.durationMs + 1)
		} else {
			sleepDuration = sc.durationMs
		}
		return sleepDuration
	}
	counters = []statusKey{statusKeyNumFailedPuts, statusKeyNumFailedPolls, statusKeyNumNilPolls, statusKeyNumFailedCapacityChecks, statusKeyNumQueueFullEvents}
)

func (ct *queueTestLoopCountersTracker) init(gatherer *status.DefaultGatherer) {
	ct.gatherer = gatherer

	ct.counters = make(map[statusKey]int)

	initialCounterValue := 0
	for _, v := range counters {
		ct.counters[v] = initialCounterValue
		gatherer.Updates <- status.Update{Key: string(v), Value: initialCounterValue}
	}

}

func (ct *queueTestLoopCountersTracker) increaseCounter(sk statusKey) {

	var newValue int
	ct.l.Lock()
	{
		newValue = ct.counters[sk] + 1
		ct.counters[sk] = newValue
	}
	ct.l.Unlock()

	ct.gatherer.Updates <- status.Update{Key: string(sk), Value: newValue}

}

func (l *testLoop[t]) init(tle *testLoopExecution[t], s sleeper, g *status.DefaultGatherer) {
	l.tle = tle
	l.s = s
	l.gatherer = g

	ct := &queueTestLoopCountersTracker{}
	ct.init(g)

	l.ct = ct
}

func (l *testLoop[t]) run() {

	l.insertLoopWithInitialStatus()

	var numQueuesWg sync.WaitGroup
	tle := l.tle
	for i := 0; i < tle.runnerConfig.numQueues; i++ {
		numQueuesWg.Add(1)
		go func(i int) {
			defer numQueuesWg.Done()

			queueName := l.assembleQueueName(i)
			lp.LogQueueRunnerEvent(fmt.Sprintf("using queue name '%s' in queue goroutine %d", queueName, i), l.tle.runnerName, log.InfoLevel)
			start := time.Now()
			q, err := l.tle.hzQueueStore.GetQueue(l.tle.ctx, queueName)
			if err != nil {
				lp.LogHzEvent("unable to retrieve queue from hazelcast cluster", log.FatalLevel)
			}
			defer func() {
				_ = q.Destroy(l.tle.ctx)
			}()
			elapsed := time.Since(start).Milliseconds()
			lp.LogTimingEvent("getQueue()", queueName, int(elapsed), log.InfoLevel)

			// TODO Check whether queue should be cleaned prior to starting put and pull operations
			// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/69

			var putWg sync.WaitGroup
			if tle.runnerConfig.putConfig.enabled {
				putWg.Add(1)
				go func() {
					defer putWg.Done()
					l.runElementLoop(l.tle.elements, q, put, queueName, i)
				}()

			}

			var pollWg sync.WaitGroup
			if tle.runnerConfig.pollConfig.enabled {
				pollWg.Add(1)
				go func() {
					defer pollWg.Done()
					l.runElementLoop(l.tle.elements, q, poll, queueName, i)
				}()
			}

			putWg.Wait()
			pollWg.Wait()
		}(i)
	}

	numQueuesWg.Wait()

}

func (l *testLoop[t]) insertLoopWithInitialStatus() {

	tle := l.tle

	numQueues := tle.runnerConfig.numQueues
	l.gatherer.Updates <- status.Update{Key: statusKeyNumQueues, Value: numQueues}
	l.gatherer.Updates <- status.Update{Key: string(put), Value: assembleInitialOperationStatus(numQueues, tle.runnerConfig.putConfig)}
	l.gatherer.Updates <- status.Update{Key: string(poll), Value: assembleInitialOperationStatus(numQueues, tle.runnerConfig.pollConfig)}

}

func assembleInitialOperationStatus(numQueues int, o *operationConfig) map[string]any {

	return map[string]any{
		statusKeyOperationEnabled: o.enabled,
		statusKeyNumRuns:          o.numRuns,
		statusKeyBatchSize:        o.batchSize,
		statusKeyTotalNumRuns:     uint32(numQueues) * o.numRuns,
	}

}

func (l *testLoop[t]) runElementLoop(elements []t, q hazelcastwrapper.Queue, o operation, queueName string, queueNumber int) {

	var config *operationConfig
	var queueFunction func(queue hazelcastwrapper.Queue, queueName string)
	if o == put {
		config = l.tle.runnerConfig.putConfig
		queueFunction = l.putElements
	} else {
		config = l.tle.runnerConfig.pollConfig
		queueFunction = l.pollElements
	}

	l.s.sleep(config.initialDelay, sleepTimeFunc, "initialDelay", queueName, l.tle.runnerName, o)

	numRuns := config.numRuns
	for i := uint32(0); i < numRuns; i++ {
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			lp.LogQueueRunnerEvent(fmt.Sprintf("finished %d of %d %s runs for queue %s in queue goroutine %d", i, numRuns, o, queueName, queueNumber), l.tle.runnerName, log.InfoLevel)
		}
		queueFunction(q, queueName)
		l.s.sleep(config.sleepBetweenRuns, sleepTimeFunc, "betweenRuns", queueName, l.tle.runnerName, o)
		lp.LogQueueRunnerEvent(fmt.Sprintf("finished %sing one set of %d tweets in queue %s after run %d of %d on queue goroutine %d", o, len(elements), queueName, i, numRuns, queueNumber), l.tle.runnerName, log.TraceLevel)
	}

	lp.LogQueueRunnerEvent(fmt.Sprintf("%s test loop done on queue '%s' in queue goroutine %d", o, queueName, queueNumber), l.tle.runnerName, log.InfoLevel)

}

func (l *testLoop[t]) putElements(q hazelcastwrapper.Queue, queueName string) {

	elements := l.tle.elements
	putConfig := l.tle.runnerConfig.putConfig

	for i := 0; i < len(elements); i++ {
		e := elements[i]
		if remaining, err := q.RemainingCapacity(l.tle.ctx); err != nil {
			l.ct.increaseCounter(statusKeyNumFailedCapacityChecks)
			lp.LogQueueRunnerEvent(fmt.Sprintf("unable to check remaining capacity for queue with name '%s'", queueName), l.tle.runnerName, log.WarnLevel)
		} else if remaining == 0 {
			l.ct.increaseCounter(statusKeyNumQueueFullEvents)
			lp.LogQueueRunnerEvent(fmt.Sprintf("no capacity left in queue '%s' -- won't execute put", queueName), l.tle.runnerName, log.WarnLevel)
		} else {
			err := q.Put(l.tle.ctx, e)
			if err != nil {
				l.ct.increaseCounter(statusKeyNumFailedPuts)
				lp.LogQueueRunnerEvent(fmt.Sprintf("unable to put tweet item into queue '%s': %s", queueName, err), l.tle.runnerName, log.WarnLevel)
			} else {
				lp.LogQueueRunnerEvent(fmt.Sprintf("successfully wrote value to queue '%s'", queueName), l.tle.runnerName, log.TraceLevel)
			}
		}
		if i > 0 && i%putConfig.batchSize == 0 {
			l.s.sleep(putConfig.sleepBetweenActionBatches, sleepTimeFunc, "betweenActionBatches", queueName, l.tle.runnerName, "put")
		}
	}

}

func (l *testLoop[t]) pollElements(q hazelcastwrapper.Queue, queueName string) {

	pollConfig := l.tle.runnerConfig.pollConfig

	for i := 0; i < len(l.tle.elements); i++ {
		valueFromQueue, err := q.Poll(l.tle.ctx)
		if err != nil {
			l.ct.increaseCounter(statusKeyNumFailedPolls)
			lp.LogQueueRunnerEvent(fmt.Sprintf("unable to poll tweet from queue '%s': %s", queueName, err), l.tle.runnerName, log.WarnLevel)
		} else if valueFromQueue == nil {
			l.ct.increaseCounter(statusKeyNumNilPolls)
			lp.LogQueueRunnerEvent(fmt.Sprintf("nothing to poll from queue '%s'", queueName), l.tle.runnerName, log.TraceLevel)
		} else {
			lp.LogQueueRunnerEvent(fmt.Sprintf("successfully retrieved value from queue '%s'", queueName), l.tle.runnerName, log.TraceLevel)
		}
		if i > 0 && i%pollConfig.batchSize == 0 {
			l.s.sleep(pollConfig.sleepBetweenActionBatches, sleepTimeFunc, "betweenActionBatches", queueName, l.tle.runnerName, "poll")
		}
	}

}

func (l *testLoop[t]) assembleQueueName(queueIndex int) string {

	tle := l.tle

	queueName := tle.runnerConfig.queueBaseName

	if tle.runnerConfig.useQueuePrefix && tle.runnerConfig.queuePrefix != "" {
		queueName = fmt.Sprintf("%s%s", tle.runnerConfig.queuePrefix, queueName)
	}
	if tle.runnerConfig.appendQueueIndexToQueueName {
		queueName = fmt.Sprintf("%s-%d", queueName, queueIndex)
	}
	if tle.runnerConfig.appendClientIdToQueueName {
		queueName = fmt.Sprintf("%s-%s", queueName, client.ID())
	}

	return queueName

}

func (s *defaultSleeper) sleep(sc *sleepConfig, sf evaluateTimeToSleep, kind, queueName, runnerName string, o operation) {

	if sc.enabled {
		sleepDuration := sf(sc)
		lp.LogQueueRunnerEvent(fmt.Sprintf("sleeping for %d milliseconds for kind '%s' on queue '%s' for operation '%s'",
			sleepDuration, kind, queueName, o), runnerName, log.TraceLevel)
		time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	}

}
