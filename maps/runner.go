package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"sync"
)

type runner interface {
	runMapTests(hzCluster string, hzMembers []string)
}

type runnerConfig struct {
	enabled                   bool
	numMaps                   int
	numRuns                   int
	mapBaseName               string
	useMapPrefix              bool
	mapPrefix                 string
	appendMapIndexToMapName   bool
	appendClientIdToMapName   bool
	sleepBetweenActionBatches *sleepConfig
	sleepBetweenRuns          *sleepConfig
}

type sleepConfig struct {
	enabled    bool
	durationMs int
}

type runnerConfigBuilder struct {
	runnerKeyPath string
	mapBaseName   string
	parsedConfig  map[string]interface{}
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

	keyPath = b.runnerKeyPath + ".numMaps"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var numMaps int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numMaps = defaultNumMaps
	} else {
		numMaps = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".appendMapIndexToMapName"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var appendMapIndexToMapName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendMapIndexToMapName = defaultAppendMapIndexToMapName
	} else {
		appendMapIndexToMapName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".appendClientIdToMapName"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var appendClientIdToMapName bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendClientIdToMapName = defaultAppendClientIdToMapName
	} else {
		appendClientIdToMapName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".numRuns"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var numRuns int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".mapPrefix.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var useMapPrefix bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		useMapPrefix = defaultUseMapPrefix
	} else {
		useMapPrefix = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".mapPrefix.prefix"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var mapPrefix string
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		mapPrefix = defaultMapPrefix
	} else {
		mapPrefix = valueFromConfig.(string)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenActionBatches.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var sleepBetweenActionBatchesEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenActionBatchesEnabled = defaultSleepBetweenActionBatchesEnabled
	} else {
		sleepBetweenActionBatchesEnabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenActionBatches.durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var sleepBetweenActionBatchesDurationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenActionBatchesDurationMs = defaultSleepBetweenActionBatchesDurationMs
	} else {
		sleepBetweenActionBatchesDurationMs = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenRuns.enabled"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var sleepBetweenRunsEnabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenRunsEnabled = defaultSleepBetweenRunsEnabled
	} else {
		sleepBetweenRunsEnabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenRuns.durationMs"
	valueFromConfig, err = config.ExtractConfigValue(b.parsedConfig, keyPath)
	var sleepBetweenRunsDurationMs int
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		sleepBetweenRunsDurationMs = defaultSleepBetweenRunsDurationMs
	} else {
		sleepBetweenRunsDurationMs = valueFromConfig.(int)
	}

	return &runnerConfig{
		enabled:                   enabled,
		numMaps:                   numMaps,
		numRuns:                   numRuns,
		mapBaseName:               b.mapBaseName,
		useMapPrefix:              useMapPrefix,
		mapPrefix:                 mapPrefix,
		appendMapIndexToMapName:   appendMapIndexToMapName,
		appendClientIdToMapName:   appendClientIdToMapName,
		sleepBetweenActionBatches: &sleepConfig{sleepBetweenActionBatchesEnabled, sleepBetweenActionBatchesDurationMs},
		sleepBetweenRuns:          &sleepConfig{sleepBetweenRunsEnabled, sleepBetweenRunsDurationMs},
	}

}

type MapTester struct {
	HzCluster string
	HzMembers []string
}

var runners []runner

func register(r runner) {
	runners = append(runners, r)
}

func (t *MapTester) TestMaps() {

	clientID := client.ClientID()
	logInternalStateEvent(fmt.Sprintf("%s: maptester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := runners[i]
			runner.runMapTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}
