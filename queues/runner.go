package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
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
		parsedConfig  map[string]interface{}
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

// constants related to general runner configuration
const (
	defaultEnabled                     = true
	defaultNumQueues                   = 10
	defaultAppendQueueIndexToQueueName = false
	defaultAppendClientIdToQueueName   = true
	defaultUseQueuePrefix              = true
	defaultQueuePrefix                 = "ht_"
)

// constants related to put configuration
const (
	defaultEnabledPut                             = true
	defaultNumRunsPut                             = 2000
	defaultBatchSizePut                           = 50
	defaultSleepInitialDelayEnabledPut            = true
	defaultSleepInitialDelayDurationMsPut         = 100
	defaultSleepBetweenActionBatchesEnabledPut    = true
	defaultSleepBetweenActionBatchesDurationMsPut = 100
	defaultSleepBetweenRunsEnabledPut             = true
	defaultSleepBetweenRunsDurationMsPut          = 500
)

// constants related to poll configuration
const (
	defaultEnabledPoll                             = true
	defaultNumRunsPoll                             = 2000
	defaultBatchSizePoll                           = 50
	defaultSleepInitialDelayEnabledPoll            = true
	defaultSleepInitialDelayDurationMsPoll         = 2000
	defaultSleepBetweenActionBatchesEnabledPoll    = true
	defaultSleepBetweenActionBatchesDurationMsPoll = 200
	defaultSleepBetweenRunsEnabledPoll             = true
	defaultSleepBetweenRunsDurationMsPoll          = 1000
)

var lp *logging.LogProvider

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func (b runnerConfigBuilder) populateConfig() *runnerConfig {

	keyPath := b.runnerKeyPath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.parsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".numQueues"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var numQueues int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numQueues = defaultNumQueues
	} else {
		numQueues = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".appendQueueIndexToQueueName"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var appendQueueIndexToQueueName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendQueueIndexToQueueName = defaultAppendQueueIndexToQueueName
	} else {
		appendQueueIndexToQueueName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".appendClientIdToQueueName"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var appendClientIdToQueueName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendClientIdToQueueName = defaultAppendClientIdToQueueName
	} else {
		appendClientIdToQueueName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".queuePrefix.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var useQueuePrefix bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		useQueuePrefix = defaultUseQueuePrefix
	} else {
		useQueuePrefix = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".queuePrefix.prefix"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var queuePrefix string
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		queuePrefix = defaultQueuePrefix
	} else {
		queuePrefix = valueFromConfig.(string)
	}

	return &runnerConfig{
		enabled:                     enabled,
		numQueues:                   numQueues,
		queueBaseName:               b.queueBaseName,
		appendQueueIndexToQueueName: appendQueueIndexToQueueName,
		appendClientIdToQueueName:   appendClientIdToQueueName,
		useQueuePrefix:              useQueuePrefix,
		queuePrefix:                 queuePrefix,
		putConfig:                   b.populateOperationConfig("put", defaultEnabledPut, defaultNumRunsPut, defaultBatchSizePut),
		pollConfig:                  b.populateOperationConfig("poll", defaultEnabledPoll, defaultNumRunsPoll, defaultBatchSizePoll),
	}

}

func (b runnerConfigBuilder) populateOperationConfig(operation string, defaultOperationEnabled bool,
	defaultNumRuns int, defaultBatchSize int) *operationConfig {

	c := b.runnerKeyPath + "." + fmt.Sprintf("%sConfig", operation)
	keyPath := c + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.parsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultOperationEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = c + ".numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var numRuns int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = c + ".batchSize"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var batchSizePoll int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		batchSizePoll = defaultBatchSize
	} else {
		batchSizePoll = valueFromConfig.(int)
	}

	var defaultInitialDelayEnabled bool
	var defaultInitialDelayDurationMs int
	var defaultBetweenActionBatchesEnabled bool
	var defaultBetweenActionBatchesDurationMs int
	var defaultBetweenRunsEnabled bool
	var defaultBetweenRunsDurationMs int

	if operation == "put" {
		defaultInitialDelayEnabled = defaultSleepInitialDelayEnabledPut
		defaultInitialDelayDurationMs = defaultSleepInitialDelayDurationMsPut
		defaultBetweenActionBatchesEnabled = defaultSleepBetweenActionBatchesEnabledPut
		defaultBetweenActionBatchesDurationMs = defaultSleepBetweenActionBatchesDurationMsPut
		defaultBetweenRunsEnabled = defaultSleepBetweenRunsEnabledPut
		defaultBetweenRunsDurationMs = defaultSleepBetweenRunsDurationMsPut
	} else {
		defaultInitialDelayEnabled = defaultSleepInitialDelayEnabledPoll
		defaultInitialDelayDurationMs = defaultSleepInitialDelayDurationMsPoll
		defaultBetweenActionBatchesEnabled = defaultSleepBetweenActionBatchesEnabledPoll
		defaultBetweenActionBatchesDurationMs = defaultSleepBetweenActionBatchesDurationMsPoll
		defaultBetweenRunsEnabled = defaultSleepBetweenRunsEnabledPoll
		defaultBetweenRunsDurationMs = defaultSleepBetweenRunsDurationMsPoll
	}

	return &operationConfig{
		enabled:                   enabled,
		numRuns:                   numRuns,
		batchSize:                 batchSizePoll,
		initialDelay:              b.populateSleepConfig(c+".sleeps.initialDelay", defaultInitialDelayEnabled, defaultInitialDelayDurationMs),
		sleepBetweenActionBatches: b.populateSleepConfig(c+".sleeps.betweenActionBatches", defaultBetweenActionBatchesEnabled, defaultBetweenActionBatchesDurationMs),
		sleepBetweenRuns:          b.populateSleepConfig(c+".sleeps.betweenRuns", defaultBetweenRunsEnabled, defaultBetweenRunsDurationMs),
	}

}

func (b runnerConfigBuilder) populateSleepConfig(configBasePath string, defaultEnabled bool, defaultDurationMs int) *sleepConfig {

	keyPath := configBasePath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.parsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = configBasePath + ".durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var durationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		durationMs = defaultDurationMs
	} else {
		durationMs = valueFromConfig.(int)
	}

	return &sleepConfig{enabled, durationMs}

}

func PopulateConfig(runnerKeyPath string, queueBaseName string) *runnerConfig {

	return runnerConfigBuilder{
		runnerKeyPath,
		queueBaseName,
		config.GetParsedConfig(),
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

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}

func logConfigEvent(configValue string, source string, msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ID(),
	}
	if logLevel == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

}
