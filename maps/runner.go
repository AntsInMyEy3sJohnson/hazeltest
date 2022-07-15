package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runner interface {
		runMapTests(hzCluster string, hzMembers []string)
	}
	runnerConfig struct {
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
	sleepConfig struct {
		enabled    bool
		durationMs int
	}
	runnerConfigBuilder struct {
		runnerKeyPath string
		mapBaseName   string
	}
	MapTester struct {
		HzCluster string
		HzMembers []string
	}
)

var runners []runner

func register(r runner) {
	runners = append(runners, r)
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

var lp *logging.LogProvider

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func (b runnerConfigBuilder) populateConfig() *runnerConfig {

	keyPath := b.runnerKeyPath + ".enabled"
	valueFromConfig, err := client.RetrieveConfigValue(keyPath)
	var enabled bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".numMaps"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var numMaps int
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		numMaps = defaultNumMaps
	} else {
		numMaps = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".appendMapIndexToMapName"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var appendMapIndexToMapName bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		appendMapIndexToMapName = defaultAppendMapIndexToMapName
	} else {
		appendMapIndexToMapName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".appendClientIdToMapName"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var appendClientIdToMapName bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		appendClientIdToMapName = defaultAppendClientIdToMapName
	} else {
		appendClientIdToMapName = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".numRuns"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var numRuns int
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".mapPrefix.enabled"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var useMapPrefix bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		useMapPrefix = defaultUseMapPrefix
	} else {
		useMapPrefix = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".mapPrefix.prefix"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var mapPrefix string
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		mapPrefix = defaultMapPrefix
	} else {
		mapPrefix = valueFromConfig.(string)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenActionBatches.enabled"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var sleepBetweenActionBatchesEnabled bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		sleepBetweenActionBatchesEnabled = defaultSleepBetweenActionBatchesEnabled
	} else {
		sleepBetweenActionBatchesEnabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenActionBatches.durationMs"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var sleepBetweenActionBatchesDurationMs int
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		sleepBetweenActionBatchesDurationMs = defaultSleepBetweenActionBatchesDurationMs
	} else {
		sleepBetweenActionBatchesDurationMs = valueFromConfig.(int)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenRuns.enabled"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var sleepBetweenRunsEnabled bool
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		sleepBetweenRunsEnabled = defaultSleepBetweenRunsEnabled
	} else {
		sleepBetweenRunsEnabled = valueFromConfig.(bool)
	}

	keyPath = b.runnerKeyPath + ".sleeps.betweenRuns.durationMs"
	valueFromConfig, err = client.RetrieveConfigValue(keyPath)
	var sleepBetweenRunsDurationMs int
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
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

func (t *MapTester) TestMaps() {

	clientID := client.ID()
	lp.LogInternalStateEvent(fmt.Sprintf("%s: maptester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

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
