package maps

import (
	"fmt"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"

	log "github.com/sirupsen/logrus"
)

type MapRunnerConfig struct {
	Enabled                   bool
	NumMaps                   int
	NumRuns                   int
	MapBaseName               string
	UseMapPrefix              bool
	MapPrefix                 string
	AppendMapIndexToMapName   bool
	AppendClientIdToMapName   bool
	SleepBetweenActionBatches *SleepConfig
	SleepBetweenRuns          *SleepConfig
}

type SleepConfig struct {
	Enabled    bool
	DurationMs int
}

type MapRunnerConfigBuilder struct {
	RunnerKeyPath string
	MapBaseName   string
	ParsedConfig  map[string]interface{}
}

const (
	defaultEnabled                             = true
	defaultNumMaps                             = 10
	defaultAppendMapIndexToMapName             = true
	defaultAppendClientIdToMapName             = false
	defaultNumRuns                             = 10000
	defaultUseMapPrefix                        = true
	defaultMapPrefix                           = "ht_"
	defaultSleepBetweenActionBatchesEnabled    = false
	defaultSleepBetweenActionBatchesDurationMs = 200
	defaultSleepBetweenRunsEnabled             = true
	defaultSleepBetweenRunsDurationMs          = 200
)

func (b MapRunnerConfigBuilder) PopulateConfig() *MapRunnerConfig {

	keyPath := b.RunnerKeyPath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".numMaps"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numMaps int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numMaps = defaultNumMaps
	} else {
		numMaps = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".appendMapIndexToMapName"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var appendMapIndexToMapName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendMapIndexToMapName = defaultAppendMapIndexToMapName
	} else {
		appendMapIndexToMapName = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".appendClientIdToMapName"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var appendClientIdToMapName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendClientIdToMapName = defaultAppendClientIdToMapName
	} else {
		appendClientIdToMapName = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var numRuns int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".mapPrefix.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var useMapPrefix bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		useMapPrefix = defaultUseMapPrefix
	} else {
		useMapPrefix = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".mapPrefix.prefix"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var mapPrefix string
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		mapPrefix = defaultMapPrefix
	} else {
		mapPrefix = valueFromConfig.(string)
	}

	keyPath = b.RunnerKeyPath + ".sleeps.betweenActionBatches.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var sleepBetweenActionBatchesEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenActionBatchesEnabled = defaultSleepBetweenActionBatchesEnabled
	} else {
		sleepBetweenActionBatchesEnabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".sleeps.betweenActionBatches.durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var sleepBetweenActionBatchesDurationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenActionBatchesDurationMs = defaultSleepBetweenActionBatchesDurationMs
	} else {
		sleepBetweenActionBatchesDurationMs = valueFromConfig.(int)
	}

	keyPath = b.RunnerKeyPath + ".sleeps.betweenRuns.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var sleepBetweenRunsEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenRunsEnabled = defaultSleepBetweenRunsEnabled
	} else {
		sleepBetweenRunsEnabled = valueFromConfig.(bool)
	}

	keyPath = b.RunnerKeyPath + ".sleeps.betweenRuns.durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var sleepBetweenRunsDurationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenRunsDurationMs = defaultSleepBetweenRunsDurationMs
	} else {
		sleepBetweenRunsDurationMs = valueFromConfig.(int)
	}

	return &MapRunnerConfig{
		Enabled:                   enabled,
		NumMaps:                   numMaps,
		NumRuns:                   numRuns,
		MapBaseName:               b.MapBaseName,
		UseMapPrefix:              useMapPrefix,
		MapPrefix:                 mapPrefix,
		AppendMapIndexToMapName:   appendMapIndexToMapName,
		AppendClientIdToMapName:   appendClientIdToMapName,
		SleepBetweenActionBatches: &SleepConfig{sleepBetweenActionBatchesEnabled, sleepBetweenActionBatchesDurationMs},
		SleepBetweenRuns:          &SleepConfig{sleepBetweenRunsEnabled, sleepBetweenRunsDurationMs},
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
