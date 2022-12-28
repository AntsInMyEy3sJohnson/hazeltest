package chaos

import (
	"fmt"
	"strings"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		dummyConfig map[string]any
	}
)

const (
	validChaosProbability   = 0.6
	invalidChaosProbability = -0.1
	validLabelSelector      = "app.kubernetes.io/name=hazelcastimdg"
	invalidLabelSelector    = ""
)

var (
	monkeyKeyPath = "testChaosMonkey"
)

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	} else {
		return fmt.Errorf("test error: unable to find value in dummy config for given key path '%s'", keyPath)
	}

	return nil

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the chaos monkey config")
	{
		b := monkeyConfigBuilder{monkeyKeyPath: monkeyKeyPath}
		t.Log("\twhen k8s ouf-of-cluster access mode is given and no property assignment yields an error")
		{
			testConfig := assembleTestConfig(validChaosProbability, k8sOutOfClusterAccessMode, validLabelSelector)
			propertyAssigner = testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig()

			msg := "\t\tno errors should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tconfig should be returned"
			if mc != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig should contain correct values"
			if configValuesAsExpected(mc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen k8s in-cluster access mode is given and no property assignment yields an error")
		{
			testConfig := assembleTestConfig(validChaosProbability, k8sInClusterAccessMode, validLabelSelector)
			propertyAssigner = testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig()

			msg := "\t\tno errors should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tconfig should be returned"
			if mc != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tconfig should contain correct values"
			if configValuesAsExpected(mc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen top-level property assignment yields an error")
		{
			testConfig := assembleTestConfig(invalidChaosProbability, k8sInClusterAccessMode, validLabelSelector)
			propertyAssigner = testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig()

			msg := "\t\terror should be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig should be nil"
			if mc == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen k8s access mode property assignment yields an error")
		{
			for _, accessMode := range []string{k8sOutOfClusterAccessMode, k8sInClusterAccessMode} {
				t.Logf("\t\t%s", accessMode)
				{
					testConfig := assembleTestConfig(validChaosProbability, accessMode, invalidLabelSelector)
					propertyAssigner = testConfigPropertyAssigner{testConfig}
					mc, err := b.populateConfig()

					msg := "\t\t\terror should be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tconfig should be nil"
					if mc == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
		}

		t.Log("\twhen unknown k8s hazelcast member access mode is given")
		{
			unknownAccessMode := "someUnknownAccessMode"
			testConfig := assembleTestConfig(validChaosProbability, unknownAccessMode, validLabelSelector)
			propertyAssigner = testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig()

			msg := "\t\terror should be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror should contain information on unknown access mode"
			if strings.Contains(err.Error(), unknownAccessMode) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err, unknownAccessMode)
			}

			msg = "\t\tconfig should be nil"
			if mc == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func assembleTestConfig(chaosProbability float64, memberAccessMode, labelSelector string) map[string]any {

	return map[string]any{
		monkeyKeyPath + ".enabled":                                    true,
		monkeyKeyPath + ".stopWhenRunnersFinished":                    true,
		monkeyKeyPath + ".chaosProbability":                           chaosProbability,
		monkeyKeyPath + ".memberAccess.mode":                          memberAccessMode,
		monkeyKeyPath + ".memberAccess.targetOnlyActive":              true,
		monkeyKeyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":    "default",
		monkeyKeyPath + ".memberAccess.k8sOutOfCluster.namespace":     "hazelcastplatform",
		monkeyKeyPath + ".memberAccess.k8sOutOfCluster.labelSelector": labelSelector,
		monkeyKeyPath + ".memberAccess.k8sInCluster.labelSelector":    labelSelector,
		monkeyKeyPath + ".sleep.enabled":                              true,
		monkeyKeyPath + ".sleep.durationSeconds":                      600,
		monkeyKeyPath + ".sleep.enableRandomness":                     false,
		monkeyKeyPath + ".memberGrace.enabled":                        true,
		monkeyKeyPath + ".memberGrace.durationSeconds":                30,
		monkeyKeyPath + ".memberGrace.enableRandomness":               true,
	}

}

func configValuesAsExpected(mc *monkeyConfig, expected map[string]any) bool {

	allButAccessModeAsExpected := mc.enabled == expected[monkeyKeyPath+".enabled"] &&
		mc.stopWhenRunnersFinished == expected[monkeyKeyPath+".stopWhenRunnersFinished"] &&
		mc.chaosProbability == expected[monkeyKeyPath+".chaosProbability"] &&
		mc.accessConfig.memberAccessMode == expected[monkeyKeyPath+".memberAccess.mode"] &&
		mc.accessConfig.targetOnlyActive == expected[monkeyKeyPath+".memberAccess.targetOnlyActive"] &&
		mc.sleep.enabled == expected[monkeyKeyPath+".sleep.enabled"] &&
		mc.sleep.durationSeconds == expected[monkeyKeyPath+".sleep.durationSeconds"] &&
		mc.sleep.enableRandomness == expected[monkeyKeyPath+".sleep.enableRandomness"] &&
		mc.memberGrace.enabled == expected[monkeyKeyPath+".memberGrace.enabled"] &&
		mc.memberGrace.durationSeconds == expected[monkeyKeyPath+".memberGrace.durationSeconds"] &&
		mc.memberGrace.enableRandomness == expected[monkeyKeyPath+".memberGrace.enableRandomness"]

	var accessModeAsExpected bool
	if allButAccessModeAsExpected && mc.accessConfig.memberAccessMode == k8sOutOfClusterAccessMode {
		accessModeAsExpected = mc.accessConfig.k8sOutOfClusterMemberAccess.kubeconfig == expected[monkeyKeyPath+".memberAccess.k8sOutOfCluster.kubeconfig"] &&
			mc.accessConfig.k8sOutOfClusterMemberAccess.namespace == expected[monkeyKeyPath+".memberAccess.k8sOutOfCluster.namespace"] &&
			mc.accessConfig.k8sOutOfClusterMemberAccess.labelSelector == expected[monkeyKeyPath+".memberAccess.k8sOutOfCluster.labelSelector"]
	} else if allButAccessModeAsExpected && mc.accessConfig.memberAccessMode == k8sInClusterAccessMode {
		accessModeAsExpected = mc.accessConfig.k8sInClusterMemberAccess.labelSelector == expected[monkeyKeyPath+".memberAccess.k8sInCluster.labelSelector"]
	} else {
		return false
	}

	return allButAccessModeAsExpected && accessModeAsExpected

}
