package maps

import (
	"testing"
)

var (
	batchTestConfig = map[string]any{
		runnerKeyPath + ".enabled":                                                     true,
		runnerKeyPath + ".numMaps":                                                     10,
		runnerKeyPath + ".appendMapIndexToMapName":                                     true,
		runnerKeyPath + ".appendClientIdToMapName":                                     false,
		runnerKeyPath + ".numRuns":                                                     1000,
		runnerKeyPath + ".mapPrefix.enabled":                                           true,
		runnerKeyPath + ".mapPrefix.prefix":                                            mapPrefix,
		runnerKeyPath + ".sleeps.betweenRuns.enabled":                                  true,
		runnerKeyPath + ".sleeps.betweenRuns.durationMs":                               2500,
		runnerKeyPath + ".sleeps.betweenRuns.enableRandomness":                         true,
		runnerKeyPath + ".testLoop.type":                                               "boundary",
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enabled":          true,
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.durationMs":       2000,
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enableRandomness": true,
	}
)

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the map runner config")
	{
		b := runnerConfigBuilder{runnerKeyPath: runnerKeyPath, mapBaseName: mapBaseName}
		t.Log("\twhen property assignment does not yield an error")
		{
			assigner := testConfigPropertyAssigner{false, batchTestConfig}
			b.assigner = assigner
			rc, err := b.populateConfig()

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
			if configValuesAsExpected(rc, batchTestConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen property assignment yields an error")
		{
			assigner := testConfigPropertyAssigner{true, map[string]any{}}
			b.assigner = assigner
			_, err := b.populateConfig()

			msg := "\t\terror should be returned"

			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func configValuesAsExpected(rc *runnerConfig, expected map[string]any) bool {

	return rc.enabled == expected[runnerKeyPath+".enabled"] &&
		rc.numMaps == uint16(expected[runnerKeyPath+".numMaps"].(int)) &&
		rc.numRuns == uint32(expected[runnerKeyPath+".numRuns"].(int)) &&
		rc.mapBaseName == mapBaseName &&
		rc.useMapPrefix == expected[runnerKeyPath+".mapPrefix.enabled"] &&
		rc.mapPrefix == expected[runnerKeyPath+".mapPrefix.prefix"] &&
		rc.appendMapIndexToMapName == expected[runnerKeyPath+".appendMapIndexToMapName"] &&
		rc.appendClientIdToMapName == expected[runnerKeyPath+".appendClientIdToMapName"] &&
		rc.batch.sleepBetweenActionBatches.enabled == expected[runnerKeyPath+".sleeps.betweenActionBatches.enabled"] &&
		rc.batch.sleepBetweenActionBatches.durationMs == expected[runnerKeyPath+".sleeps.betweenActionBatches.durationMs"] &&
		rc.batch.sleepBetweenActionBatches.enableRandomness == expected[runnerKeyPath+".sleeps.betweenActionBatches.enableRandomness"] &&
		rc.sleepBetweenRuns.enabled == expected[runnerKeyPath+".sleeps.betweenRuns.enabled"] &&
		rc.sleepBetweenRuns.durationMs == expected[runnerKeyPath+".sleeps.betweenRuns.durationMs"] &&
		rc.sleepBetweenRuns.enableRandomness == expected[runnerKeyPath+".sleeps.betweenRuns.enableRandomness"]

}
