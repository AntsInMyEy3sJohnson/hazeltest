package queues

import (
	"errors"
	"hazeltest/client"
	"strings"
	"testing"
)

var (
	testConfig = map[string]any{
		runnerKeyPath + ".enabled":                                                 true,
		runnerKeyPath + ".numQueues":                                               5,
		runnerKeyPath + ".appendQueueIndexToQueueName":                             true,
		runnerKeyPath + ".appendClientIdToQueueName":                               false,
		runnerKeyPath + ".queuePrefix.enabled":                                     true,
		runnerKeyPath + ".queuePrefix.prefix":                                      queuePrefix,
		runnerKeyPath + ".putConfig.enabled":                                       true,
		runnerKeyPath + ".putConfig.numRuns":                                       500,
		runnerKeyPath + ".putConfig.batchSize":                                     50,
		runnerKeyPath + ".putConfig.sleeps.initialDelay.enabled":                   true,
		runnerKeyPath + ".putConfig.sleeps.initialDelay.durationMs":                2000,
		runnerKeyPath + ".putConfig.sleeps.initialDelay.enableRandomness":          true,
		runnerKeyPath + ".putConfig.sleeps.betweenActionBatches.enabled":           true,
		runnerKeyPath + ".putConfig.sleeps.betweenActionBatches.durationMs":        1000,
		runnerKeyPath + ".putConfig.sleeps.betweenActionBatches.enableRandomness":  true,
		runnerKeyPath + ".putConfig.sleeps.betweenRuns.enabled":                    true,
		runnerKeyPath + ".putConfig.sleeps.betweenRuns.durationMs":                 2000,
		runnerKeyPath + ".putConfig.sleeps.betweenRuns.enableRandomness":           true,
		runnerKeyPath + ".pollConfig.enabled":                                      true,
		runnerKeyPath + ".pollConfig.numRuns":                                      500,
		runnerKeyPath + ".pollConfig.batchSize":                                    50,
		runnerKeyPath + ".pollConfig.sleeps.initialDelay.enabled":                  true,
		runnerKeyPath + ".pollConfig.sleeps.initialDelay.durationMs":               12500,
		runnerKeyPath + ".pollConfig.sleeps.initialDelay.enableRandomness":         true,
		runnerKeyPath + ".pollConfig.sleeps.betweenActionBatches.enabled":          true,
		runnerKeyPath + ".pollConfig.sleeps.betweenActionBatches.durationMs":       1000,
		runnerKeyPath + ".pollConfig.sleeps.betweenActionBatches.enableRandomness": true,
		runnerKeyPath + ".pollConfig.sleeps.betweenRuns.enabled":                   true,
		runnerKeyPath + ".pollConfig.sleeps.betweenRuns.durationMs":                2000,
		runnerKeyPath + ".pollConfig.sleeps.betweenRuns.enableRandomness":          true,
	}
)

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the queue runner config")
	{
		b := runnerConfigBuilder{runnerKeyPath: runnerKeyPath, queueBaseName: queueBaseName}
		t.Log("\twhen property assignment does not generate an error")
		{
			propertyAssigner = testConfigPropertyAssigner{returnError: false, dummyConfig: testConfig}
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
			if configValuesAsExpected(rc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen property assigning a property yields an error")
		{
			propertyAssigner = testConfigPropertyAssigner{returnError: true, dummyConfig: map[string]interface{}{}}
			_, err := b.populateConfig()

			msg := "\t\terror should be returned"

			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen property parsing a property yields an error")
		{
			testConfigCopy := copyTestConfig()
			invalidValuePath := runnerKeyPath + ".numQueues"
			testConfigCopy[invalidValuePath] = "boom!"
			propertyAssigner = testConfigPropertyAssigner{returnError: false, dummyConfig: testConfigCopy}

			_, err := b.populateConfig()

			msg := "\t\tcorrect type of error should be returned"

			if err != nil && errors.As(err, &client.FailedParse{}) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror message must contain path of erroneous key"

			if strings.Contains(err.Error(), invalidValuePath) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func copyTestConfig() map[string]interface{} {

	mapCopy := make(map[string]interface{})
	for k, v := range testConfig {
		mapCopy[k] = v
	}

	return mapCopy

}

func configValuesAsExpected(rc *runnerConfig, expected map[string]interface{}) bool {

	var runnerKeyPath = "testQueueRunner"

	return rc.enabled == expected[runnerKeyPath+".enabled"] &&
		rc.numQueues == expected[runnerKeyPath+".numQueues"] &&
		rc.appendQueueIndexToQueueName == expected[runnerKeyPath+".appendQueueIndexToQueueName"] &&
		rc.appendClientIdToQueueName == expected[runnerKeyPath+".appendClientIdToQueueName"] &&
		rc.useQueuePrefix == expected[runnerKeyPath+".queuePrefix.enabled"] &&
		rc.queuePrefix == expected[runnerKeyPath+".queuePrefix.prefix"] &&
		rc.putConfig.enabled == expected[runnerKeyPath+".putConfig.enabled"] &&
		rc.putConfig.numRuns == uint32(expected[runnerKeyPath+".putConfig.numRuns"].(int)) &&
		rc.putConfig.batchSize == expected[runnerKeyPath+".putConfig.batchSize"] &&
		rc.putConfig.initialDelay.enabled == expected[runnerKeyPath+".putConfig.sleeps.initialDelay.enabled"] &&
		rc.putConfig.initialDelay.durationMs == expected[runnerKeyPath+".putConfig.sleeps.initialDelay.durationMs"] &&
		rc.putConfig.initialDelay.enableRandomness == expected[runnerKeyPath+".putConfig.sleeps.initialDelay.enableRandomness"] &&
		rc.putConfig.sleepBetweenActionBatches.enabled == expected[runnerKeyPath+".putConfig.sleeps.betweenActionBatches.enabled"] &&
		rc.putConfig.sleepBetweenActionBatches.durationMs == expected[runnerKeyPath+".putConfig.sleeps.betweenActionBatches.durationMs"] &&
		rc.putConfig.sleepBetweenActionBatches.enableRandomness == expected[runnerKeyPath+".putConfig.sleeps.betweenActionBatches.enableRandomness"] &&
		rc.putConfig.sleepBetweenRuns.enabled == expected[runnerKeyPath+".putConfig.sleeps.betweenRuns.enabled"] &&
		rc.putConfig.sleepBetweenRuns.durationMs == expected[runnerKeyPath+".putConfig.sleeps.betweenRuns.durationMs"] &&
		rc.putConfig.sleepBetweenRuns.enableRandomness == expected[runnerKeyPath+".putConfig.sleeps.betweenRuns.enableRandomness"] &&
		rc.pollConfig.enabled == expected[runnerKeyPath+".pollConfig.enabled"] &&
		rc.pollConfig.numRuns == uint32(expected[runnerKeyPath+".pollConfig.numRuns"].(int)) &&
		rc.pollConfig.batchSize == expected[runnerKeyPath+".pollConfig.batchSize"] &&
		rc.pollConfig.initialDelay.enabled == expected[runnerKeyPath+".pollConfig.sleeps.initialDelay.enabled"] &&
		rc.pollConfig.initialDelay.durationMs == expected[runnerKeyPath+".pollConfig.sleeps.initialDelay.durationMs"] &&
		rc.pollConfig.initialDelay.enableRandomness == expected[runnerKeyPath+".pollConfig.sleeps.initialDelay.enableRandomness"] &&
		rc.pollConfig.sleepBetweenActionBatches.enabled == expected[runnerKeyPath+".pollConfig.sleeps.betweenActionBatches.enabled"] &&
		rc.pollConfig.sleepBetweenActionBatches.durationMs == expected[runnerKeyPath+".pollConfig.sleeps.betweenActionBatches.durationMs"] &&
		rc.pollConfig.sleepBetweenActionBatches.enableRandomness == expected[runnerKeyPath+".pollConfig.sleeps.betweenActionBatches.enableRandomness"] &&
		rc.pollConfig.sleepBetweenRuns.enabled == expected[runnerKeyPath+".pollConfig.sleeps.betweenRuns.enabled"] &&
		rc.pollConfig.sleepBetweenRuns.durationMs == expected[runnerKeyPath+".pollConfig.sleeps.betweenRuns.durationMs"] &&
		rc.pollConfig.sleepBetweenRuns.enableRandomness == expected[runnerKeyPath+".pollConfig.sleeps.betweenRuns.enableRandomness"]

}
