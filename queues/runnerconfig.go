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
	Enabled   bool
	NumRuns   int
	BatchSize int
}

type RunnerConfigBuilder struct {
	RunnerKeyPath string
	QueueBaseName string
	ParsedConfig  map[string]interface{}
}

const (
	defaultEnabled       = true
	defaultNumQueues     = 10
	defaultPutEnabled    = true
	defaultNumRunsPut    = 5000
	defaultBatchSizePut  = 50
	defaultPollEnabled   = true
	defaultNumRunsPoll   = 10000
	defaultBatchSizePoll = 50
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

	keyPath = b.RunnerKeyPath + ".putConfig.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var putEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		putEnabled = defaultPutEnabled
	} else {
		putEnabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".putConfig.numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numRunsPut int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRunsPut = defaultNumRunsPut
	} else {
		numRunsPut = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".putConfig.batchSize"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var batchSizePut int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		batchSizePut = defaultBatchSizePut
	} else {
		batchSizePut = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".pollConfig.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var pollEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		pollEnabled = defaultPollEnabled
	} else {
		pollEnabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".pollConfig.numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numRunsPoll int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRunsPoll = defaultNumRunsPoll
	} else {
		numRunsPoll = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".pollConfig.batchSize"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var batchSizePoll int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		batchSizePoll = defaultBatchSizePoll
	} else {
		batchSizePoll = valueFromConfig.(int)
	}

	return &RunnerConfig{
		Enabled:       enabled,
		NumQueues:     numQueues,
		QueueBaseName: b.QueueBaseName,
		PutConfig:     &OperationConfig{putEnabled, numRunsPut, batchSizePut},
		PollConfig:    &OperationConfig{pollEnabled, numRunsPoll, batchSizePoll},
	}

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
