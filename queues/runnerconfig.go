package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
)

type RunnerConfig struct {
	Enabled       bool
	NumQueues     int
	QueueBaseName string
	PutConfig     *OperationConfig
	PollConfig    *OperationConfig
}

type OperationConfig struct {
	Enabled                   bool
	NumRuns                   int
	BatchSize                 int
	InitialDelay              *SleepConfig
	SleepBetweenActionBatches *SleepConfig
	SleepBetweenRuns          *SleepConfig
}

type SleepConfig struct {
	Enabled    bool
	DurationMs int
}

type RunnerConfigBuilder struct {
	RunnerKeyPath string
	QueueBaseName string
	ParsedConfig  map[string]interface{}
}

// constants related to general runner configuration
const (
	defaultEnabled   = true
	defaultNumQueues = 10
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

func (b RunnerConfigBuilder) PopulateConfig() *RunnerConfig {

	keyPath := b.RunnerKeyPath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".numQueues"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numQueues int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numQueues = defaultNumQueues
	} else {
		numQueues = valueFromConfig.(int)
	}

	return &RunnerConfig{
		Enabled:       enabled,
		NumQueues:     numQueues,
		QueueBaseName: b.QueueBaseName,
		PutConfig:     b.populateOperationConfig("put", defaultEnabledPut, defaultNumRunsPut, defaultBatchSizePut),
		PollConfig:    b.populateOperationConfig("poll", defaultEnabledPoll, defaultNumRunsPoll, defaultBatchSizePoll),
	}

}

func (b RunnerConfigBuilder) populateOperationConfig(operation string, defaultOperationEnabled bool,
	defaultNumRuns int, defaultBatchSize int) *OperationConfig {

	operationConfig := b.RunnerKeyPath + "." + fmt.Sprintf("%sConfig", operation)
	keyPath := operationConfig + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultOperationEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = operationConfig + ".numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numRuns int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = operationConfig + ".batchSize"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
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

	return &OperationConfig{
		Enabled:                   enabled,
		NumRuns:                   numRuns,
		BatchSize:                 batchSizePoll,
		InitialDelay:              b.populateSleepConfig(operationConfig+".sleeps.initialDelay", defaultInitialDelayEnabled, defaultInitialDelayDurationMs),
		SleepBetweenActionBatches: b.populateSleepConfig(operationConfig+".sleeps.betweenActionBatches", defaultBetweenActionBatchesEnabled, defaultBetweenActionBatchesDurationMs),
		SleepBetweenRuns:          b.populateSleepConfig(operationConfig+".sleeps.betweenRuns", defaultBetweenRunsEnabled, defaultBetweenRunsDurationMs),
	}

}

func (b RunnerConfigBuilder) populateSleepConfig(configBasePath string, defaultEnabled bool, defaultDurationMs int) *SleepConfig {

	keyPath := configBasePath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = configBasePath + ".durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var durationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		durationMs = defaultDurationMs
	} else {
		durationMs = valueFromConfig.(int)
	}

	return &SleepConfig{enabled, durationMs}

}

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}

func logConfigEvent(configValue string, source string, msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}
	if logLevel == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

}
