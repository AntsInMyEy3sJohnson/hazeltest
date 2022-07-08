package maps

import (
	"hazeltest/client/config"
)

type Runner interface {
	RunMapTests(hzCluster string, hzMembers []string)
}

var Runners []Runner

func Register(r Runner) {
	Runners = append(Runners, r)
}

type RunnerConfig struct {
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

type RunnerConfigBuilder struct {
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

	return &RunnerConfig{
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
