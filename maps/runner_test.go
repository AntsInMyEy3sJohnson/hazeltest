package maps

import (
	"testing"
)

var (
	testConfig = map[string]interface{}{
		runnerKeyPath + ".enabled":                                true,
		runnerKeyPath + ".numMaps":                                10,
		runnerKeyPath + ".appendMapIndexToMapName":                true,
		runnerKeyPath + ".appendClientIdToMapName":                false,
		runnerKeyPath + ".numRuns":                                1000,
		runnerKeyPath + ".mapPrefix.enabled":                      true,
		runnerKeyPath + ".mapPrefix.prefix":                       mapPrefix,
		runnerKeyPath + ".sleeps.betweenActionBatches.enabled":    true,
		runnerKeyPath + ".sleeps.betweenActionBatches.durationMs": 2000,
		runnerKeyPath + ".sleeps.betweenRuns.enabled":             true,
		runnerKeyPath + ".sleeps.betweenRuns.durationMs":          2500,
	}
)

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the map runner config")
	{
		b := runnerConfigBuilder{runnerKeyPath: runnerKeyPath, mapBaseName: mapBaseName}
		t.Log("\twhen property assignment does not yield an error")
		{
			rc, err := b.populateConfig(testConfigPropertyAssigner{false, b.runnerKeyPath, testConfig})

			msg := "\t\tno error should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig should be returned"
			if rc != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			msg = "\t\tconfig should contain expected values"
			if configValuesAsExpected(rc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen property assignment yields an error")
		{
			_, err := b.populateConfig(testConfigPropertyAssigner{true, b.runnerKeyPath, map[string]interface{}{}})

			msg := "\t\terror should be returned"

			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func configValuesAsExpected(rc *runnerConfig, expected map[string]interface{}) bool {

	return rc.enabled == expected[runnerKeyPath+".enabled"] &&
		rc.numMaps == expected[runnerKeyPath+".numMaps"] &&
		rc.numRuns == expected[runnerKeyPath+".numRuns"] &&
		rc.mapBaseName == mapBaseName &&
		rc.useMapPrefix == expected[runnerKeyPath+".mapPrefix.enabled"] &&
		rc.mapPrefix == expected[runnerKeyPath+".mapPrefix.prefix"] &&
		rc.appendMapIndexToMapName == expected[runnerKeyPath+".appendMapIndexToMapName"] &&
		rc.appendClientIdToMapName == expected[runnerKeyPath+".appendClientIdToMapName"] &&
		rc.sleepBetweenActionBatches.enabled == expected[runnerKeyPath+".sleeps.betweenActionBatches.enabled"] &&
		rc.sleepBetweenActionBatches.durationMs == expected[runnerKeyPath+".sleeps.betweenActionBatches.durationMs"] &&
		rc.sleepBetweenRuns.enabled == expected[runnerKeyPath+".sleeps.betweenRuns.enabled"] &&
		rc.sleepBetweenRuns.durationMs == expected[runnerKeyPath+".sleeps.betweenRuns.durationMs"]

}
