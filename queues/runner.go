package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runner interface {
		runQueueTests(hzCluster string, hzMembers []string)
	}
	runnerConfig struct {
		enabled                     bool
		numQueues                   int
		queueBaseName               string
		appendQueueIndexToQueueName bool
		appendClientIdToQueueName   bool
		useQueuePrefix              bool
		queuePrefix                 string
		putConfig                   *operationConfig
		pollConfig                  *operationConfig
	}
	operationConfig struct {
		enabled                   bool
		numRuns                   uint32
		batchSize                 int
		initialDelay              *sleepConfig
		sleepBetweenActionBatches *sleepConfig
		sleepBetweenRuns          *sleepConfig
	}
	sleepConfig struct {
		enabled    bool
		durationMs int
	}
	runnerConfigBuilder struct {
		runnerKeyPath string
		queueBaseName string
	}
	QueueTester struct {
		HzCluster string
		HzMembers []string
	}
)

var (
	runners []runner
	a       client.DefaultConfigPropertyAssigner
	lp      *logging.LogProvider
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
	a = client.DefaultConfigPropertyAssigner{}
}

func (b runnerConfigBuilder) populateConfig() *runnerConfig {

	// TODO Error handling
	var enabled bool
	_ = a.Assign(b.runnerKeyPath+".enabled", func(a any) {
		enabled = a.(bool)
	})

	var numQueues int
	_ = a.Assign(b.runnerKeyPath+".numQueues", func(a any) {
		numQueues = a.(int)
	})

	var appendQueueIndexToQueueName bool
	_ = a.Assign(b.runnerKeyPath+".appendQueueIndexToQueueName", func(a any) {
		appendQueueIndexToQueueName = a.(bool)
	})

	var appendClientIdToQueueName bool
	_ = a.Assign(b.runnerKeyPath+".appendClientIdToQueueName", func(a any) {
		appendClientIdToQueueName = a.(bool)
	})

	var useQueuePrefix bool
	_ = a.Assign(b.runnerKeyPath+".queuePrefix.enabled", func(a any) {
		useQueuePrefix = a.(bool)
	})

	var queuePrefix string
	_ = a.Assign(b.runnerKeyPath+".queuePrefix.prefix", func(a any) {
		queuePrefix = a.(string)
	})

	return &runnerConfig{
		enabled:                     enabled,
		numQueues:                   numQueues,
		queueBaseName:               b.queueBaseName,
		appendQueueIndexToQueueName: appendQueueIndexToQueueName,
		appendClientIdToQueueName:   appendClientIdToQueueName,
		useQueuePrefix:              useQueuePrefix,
		queuePrefix:                 queuePrefix,
		putConfig:                   b.populateOperationConfig("put"),
		pollConfig:                  b.populateOperationConfig("poll"),
	}

}

func (b runnerConfigBuilder) populateOperationConfig(operation string) *operationConfig {

	c := b.runnerKeyPath + "." + fmt.Sprintf("%sConfig", operation)

	// TODO Error handling
	var enabled bool
	_ = a.Assign(c+".enabled", func(a any) {
		enabled = a.(bool)
	})

	var numRuns uint32
	_ = a.Assign(c+".numRuns", func(a any) {
		numRuns = uint32(a.(int))
	})

	var batchSizePoll int
	_ = a.Assign(c+".batchSize", func(a any) {
		batchSizePoll = a.(int)
	})

	return &operationConfig{
		enabled:                   enabled,
		numRuns:                   numRuns,
		batchSize:                 batchSizePoll,
		initialDelay:              b.populateSleepConfig(c + ".sleeps.initialDelay"),
		sleepBetweenActionBatches: b.populateSleepConfig(c + ".sleeps.betweenActionBatches"),
		sleepBetweenRuns:          b.populateSleepConfig(c + ".sleeps.betweenRuns"),
	}

}

func (b runnerConfigBuilder) populateSleepConfig(configBasePath string) *sleepConfig {

	keyPath := configBasePath + ".enabled"

	var enabled bool
	_ = a.Assign(keyPath, func(a any) {
		enabled = a.(bool)
	})

	keyPath = configBasePath + ".durationMs"
	var durationMs int
	_ = a.Assign(keyPath, func(a any) {
		durationMs = a.(int)
	})

	return &sleepConfig{enabled, durationMs}

}

func populateConfig(runnerKeyPath string, queueBaseName string) *runnerConfig {

	return runnerConfigBuilder{
		runnerKeyPath,
		queueBaseName,
	}.populateConfig()

}

func (t *QueueTester) TestQueues() {

	clientID := client.ID()
	logInternalStateInfo(fmt.Sprintf("%s: queuetester starting %d runner/-s", clientID, len(runners)))

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := runners[i]
			runner.runQueueTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}

func logInternalStateInfo(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.InternalStateEvent,
		"client": client.ID(),
	}).Trace(msg)

}
