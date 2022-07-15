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
		numRuns                   int
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

var runners []runner

func register(r runner) {
	runners = append(runners, r)
}

var lp *logging.LogProvider

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func (b runnerConfigBuilder) populateConfig() *runnerConfig {

	var enabled bool
	client.PopulateConfigProperty(b.runnerKeyPath+".enabled", func(a any) {
		enabled = a.(bool)
	})

	var numQueues int
	client.PopulateConfigProperty(b.runnerKeyPath+".numQueues", func(a any) {
		numQueues = a.(int)
	})

	var appendQueueIndexToQueueName bool
	client.PopulateConfigProperty(b.runnerKeyPath+".appendQueueIndexToQueueName", func(a any) {
		appendQueueIndexToQueueName = a.(bool)
	})

	var appendClientIdToQueueName bool
	client.PopulateConfigProperty(b.runnerKeyPath+".appendClientIdToQueueName", func(a any) {
		appendClientIdToQueueName = a.(bool)
	})

	var useQueuePrefix bool
	client.PopulateConfigProperty(b.runnerKeyPath+".queuePrefix.enabled", func(a any) {
		useQueuePrefix = a.(bool)
	})

	var queuePrefix string
	client.PopulateConfigProperty(b.runnerKeyPath+".queuePrefix.prefix", func(a any) {
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

	var enabled bool
	client.PopulateConfigProperty(c+".enabled", func(a any) {
		enabled = a.(bool)
	})

	var numRuns int
	client.PopulateConfigProperty(c+".numRuns", func(a any) {
		numRuns = a.(int)
	})

	var batchSizePoll int
	client.PopulateConfigProperty(c+".batchSize", func(a any) {
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
	client.PopulateConfigProperty(keyPath, func(a any) {
		enabled = a.(bool)
	})

	keyPath = configBasePath + ".durationMs"
	var durationMs int
	client.PopulateConfigProperty(keyPath, func(a any) {
		durationMs = a.(int)
	})

	return &sleepConfig{enabled, durationMs}

}

func PopulateConfig(runnerKeyPath string, queueBaseName string) *runnerConfig {

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
		"kind":   logging.InternalStateInfo,
		"client": client.ID(),
	}).Trace(msg)

}
