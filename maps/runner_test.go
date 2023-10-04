package maps

import (
	"fmt"
	"testing"
)

var (
	baseTestConfig = map[string]any{
		runnerKeyPath + ".enabled":                             true,
		runnerKeyPath + ".numMaps":                             10,
		runnerKeyPath + ".appendMapIndexToMapName":             true,
		runnerKeyPath + ".appendClientIdToMapName":             false,
		runnerKeyPath + ".numRuns":                             1_000,
		runnerKeyPath + ".mapPrefix.enabled":                   true,
		runnerKeyPath + ".mapPrefix.prefix":                    mapPrefix,
		runnerKeyPath + ".sleeps.betweenRuns.enabled":          true,
		runnerKeyPath + ".sleeps.betweenRuns.durationMs":       2_500,
		runnerKeyPath + ".sleeps.betweenRuns.enableRandomness": true,
	}
	batchTestConfig = map[string]any{
		runnerKeyPath + ".testLoop.type":                                               "batch",
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enabled":          true,
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.durationMs":       2_000,
		runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enableRandomness": true,
	}
	boundaryTestConfig = map[string]any{
		runnerKeyPath + ".testLoop.type":                                                                        "boundary",
		runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.enabled":                              true,
		runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.durationMs":                           2_500,
		runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.enableRandomness":                     true,
		runnerKeyPath + ".testLoop.boundary.operationChain.resetAfterRun":                                       true,
		runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.upper.mapFillPercentage":          0.8,
		runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.upper.enableRandomness":           true,
		runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.lower.mapFillPercentage":          0.2,
		runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.lower.enableRandomness":           true,
		runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.actionTowardsBoundaryProbability": 0.75,
	}
)

func assembleTestConfigForTestLoopType(t runnerLoopType) map[string]any {

	if t == batch {
		return combineMapsInNewMap([]map[string]any{baseTestConfig, batchTestConfig})
	} else if t == boundary {
		return combineMapsInNewMap([]map[string]any{baseTestConfig, boundaryTestConfig})
	}

	return nil

}

func combineMapsInNewMap(newContentMaps []map[string]any) map[string]any {

	var result = map[string]any{}
	for _, m := range newContentMaps {
		for k, v := range m {
			result[k] = v
		}
	}

	return result

}

func TestValidateTestLoopType(t *testing.T) {

	t.Log("given a method to validate a string against the two available map test loop types")
	{
		keyPath := "awesome.key.path"
		t.Log("\twhen valid test loop type is provided")
		{
			msg := "\t\tno error must be returned"
			for _, loopType := range []runnerLoopType{batch, boundary} {

				err := validateTestLoopType(keyPath, string(loopType))

				if err == nil {
					t.Log(msg, checkMark, loopType)
				} else {
					t.Fatal(msg, ballotX, loopType)
				}
			}
		}

		msg := "\t\terror must be returned"
		t.Log("\twhen string representing non-existent loop type is provided")
		{
			err := validateTestLoopType(keyPath, "gandalf")

			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen given string is empty")
		{
			err := validateTestLoopType(keyPath, "")

			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given a map runner config containing properties for both a batch and a boundary test loop")
	{
		b := runnerConfigBuilder{runnerKeyPath: runnerKeyPath, mapBaseName: mapBaseName}
		t.Log("\twhen property assignment does not yield an error")
		{
			for _, lt := range []runnerLoopType{batch, boundary} {
				t.Log(fmt.Sprintf("\t\ttest loop type: %s", lt))
				testConfig := assembleTestConfigForTestLoopType(lt)
				assigner := testConfigPropertyAssigner{false, testConfig}
				b.assigner = assigner
				rc, err := b.populateConfig()

				msg := "\t\t\tno error should be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig should be returned"
				if rc != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig should contain expected values"
				if valid, detail := configValuesAsExpected(rc, testConfig); valid {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}

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
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func configValuesAsExpected(rc *runnerConfig, expected map[string]any) (bool, string) {

	if rc.mapBaseName != mapBaseName {
		return false, "map base name"
	}

	keyPath := runnerKeyPath + ".enabled"
	if rc.enabled != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".numMaps"
	if rc.numMaps != uint16(expected[keyPath].(int)) {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".numRuns"
	if rc.numRuns != uint32(expected[keyPath].(int)) {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".mapPrefix.enabled"
	if rc.useMapPrefix != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".mapPrefix.prefix"
	if rc.mapPrefix != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".appendMapIndexToMapName"
	if rc.appendMapIndexToMapName != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".appendClientIdToMapName"
	if rc.appendClientIdToMapName != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".sleeps.betweenRuns.enabled"
	if rc.sleepBetweenRuns.enabled != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".sleeps.betweenRuns.durationMs"
	if rc.sleepBetweenRuns.durationMs != expected[keyPath] {
		return false, keyPath
	}

	keyPath = runnerKeyPath + ".testLoop.type"
	if string(rc.loopType) != expected[keyPath] {
		return false, keyPath
	}

	if rc.loopType == batch {
		keyPath = runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enabled"
		if rc.batch.sleepBetweenActionBatches.enabled != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.durationMs"
		if rc.batch.sleepBetweenActionBatches.durationMs != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.batch.sleeps.betweenActionBatches.enableRandomness"
		if rc.batch.sleepBetweenActionBatches.enableRandomness != expected[keyPath] {
			return false, keyPath
		}

		if rc.boundary != nil {
			return false, fmt.Sprintf("boundary test loop config must be nil when batch test loop was configured")
		}
	} else if rc.loopType == boundary {
		keyPath = runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.enabled"
		if rc.boundary.sleepBetweenOperationChains.enabled != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.durationMs"
		if rc.boundary.sleepBetweenOperationChains.durationMs != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.sleeps.betweenOperationChains.enableRandomness"
		if rc.boundary.sleepBetweenOperationChains.enableRandomness != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.resetAfterRun"
		if rc.boundary.resetAfterRun != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.upper.mapFillPercentage"
		if rc.boundary.upper.mapFillPercentage != float32(expected[keyPath].(float64)) {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.upper.enableRandomness"
		if rc.boundary.upper.enableRandomness != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.lower.mapFillPercentage"
		if rc.boundary.lower.mapFillPercentage != float32(expected[keyPath].(float64)) {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.lower.enableRandomness"
		if rc.boundary.lower.enableRandomness != expected[keyPath] {
			return false, keyPath
		}

		keyPath = runnerKeyPath + ".testLoop.boundary.operationChain.boundaryDefinition.actionTowardsBoundaryProbability"
		if rc.boundary.actionTowardsBoundaryProbability != float32(expected[keyPath].(float64)) {
			return false, keyPath
		}

		if rc.batch != nil {
			return false, fmt.Sprintf("batch test loop config must be nil when boundary test loop was configured")
		}
	} else {
		return false, fmt.Sprintf("unknown test loop type: %s", rc.loopType)
	}

	return true, ""

}
