package chaos

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type (
	testHzMemberChooser struct {
		returnError bool
		memberID    string
	}
	testHzMemberKiller struct {
		returnError bool
	}
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
	hzMemberID          = "hazelcastimdg-0"
	memberChooser       = &testHzMemberChooser{returnError: false, memberID: hzMemberID}
	errMemberChooser    = &testHzMemberChooser{returnError: true}
	testMonkeyKeyPath   = "testChaosMonkey"
	memberKillerKeyPath = "chaosMonkeys.memberKiller"
)

func (k *testHzMemberKiller) kill(_ hzMember, _ memberAccessConfig, _ sleepConfig) error {

	if k.returnError {
		return errors.New("yet another error that should have been completely impossible")
	}

	return nil

}

func (c *testHzMemberChooser) choose(_ memberAccessConfig) (hzMember, error) {

	if c.returnError {
		return hzMember{}, errors.New("awesome error")
	}

	return hzMember{c.memberID}, nil

}

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

func TestMemberKillerMonkeyCauseChaos(t *testing.T) {

	t.Log("given the need to test the member killer monkey's ability to cause chaos")
	{
		t.Log("\twhen populating the member killer config returns an error")
		{
			propertyAssigner = &testConfigPropertyAssigner{assembleTestConfig(memberKillerKeyPath, invalidChaosProbability, k8sInClusterAccessMode, validLabelSelector)}
			m := memberKillerMonkey{chooser: &testHzMemberChooser{}, killer: &testHzMemberKiller{}}

			m.causeChaos()

			msg := "\t\tstate transitions must contain only start state"
			if len(m.stateList) == 1 && m.stateList[0] == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the chaos monkey config")
	{
		b := monkeyConfigBuilder{monkeyKeyPath: testMonkeyKeyPath}
		t.Log("\twhen k8s ouf-of-cluster access mode is given and no property assignment yields an error")
		{
			testConfig := assembleTestConfig(testMonkeyKeyPath, validChaosProbability, k8sOutOfClusterAccessMode, validLabelSelector)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, validChaosProbability, k8sInClusterAccessMode, validLabelSelector)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, invalidChaosProbability, k8sInClusterAccessMode, validLabelSelector)
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
					testConfig := assembleTestConfig(testMonkeyKeyPath, validChaosProbability, accessMode, invalidLabelSelector)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, validChaosProbability, unknownAccessMode, validLabelSelector)
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

func assembleTestConfig(keyPath string, chaosProbability float64, memberAccessMode, labelSelector string) map[string]any {

	return map[string]any{
		keyPath + ".enabled":                                    true,
		keyPath + ".numRuns":                                    10000,
		keyPath + ".chaosProbability":                           chaosProbability,
		keyPath + ".memberAccess.mode":                          memberAccessMode,
		keyPath + ".memberAccess.targetOnlyActive":              true,
		keyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":    "default",
		keyPath + ".memberAccess.k8sOutOfCluster.namespace":     "hazelcastplatform",
		keyPath + ".memberAccess.k8sOutOfCluster.labelSelector": labelSelector,
		keyPath + ".memberAccess.k8sInCluster.labelSelector":    labelSelector,
		keyPath + ".sleep.enabled":                              true,
		keyPath + ".sleep.durationSeconds":                      600,
		keyPath + ".sleep.enableRandomness":                     false,
		keyPath + ".memberGrace.enabled":                        true,
		keyPath + ".memberGrace.durationSeconds":                30,
		keyPath + ".memberGrace.enableRandomness":               true,
	}

}

func configValuesAsExpected(mc *monkeyConfig, expected map[string]any) bool {

	allButAccessModeAsExpected := mc.enabled == expected[testMonkeyKeyPath+".enabled"] &&
		mc.numRuns == expected[testMonkeyKeyPath+".numRuns"] &&
		mc.chaosProbability == expected[testMonkeyKeyPath+".chaosProbability"] &&
		mc.accessConfig.memberAccessMode == expected[testMonkeyKeyPath+".memberAccess.mode"] &&
		mc.accessConfig.targetOnlyActive == expected[testMonkeyKeyPath+".memberAccess.targetOnlyActive"] &&
		mc.sleep.enabled == expected[testMonkeyKeyPath+".sleep.enabled"] &&
		mc.sleep.durationSeconds == expected[testMonkeyKeyPath+".sleep.durationSeconds"] &&
		mc.sleep.enableRandomness == expected[testMonkeyKeyPath+".sleep.enableRandomness"] &&
		mc.memberGrace.enabled == expected[testMonkeyKeyPath+".memberGrace.enabled"] &&
		mc.memberGrace.durationSeconds == expected[testMonkeyKeyPath+".memberGrace.durationSeconds"] &&
		mc.memberGrace.enableRandomness == expected[testMonkeyKeyPath+".memberGrace.enableRandomness"]

	var accessModeAsExpected bool
	if allButAccessModeAsExpected && mc.accessConfig.memberAccessMode == k8sOutOfClusterAccessMode {
		accessModeAsExpected = mc.accessConfig.k8sOutOfCluster.kubeconfig == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.kubeconfig"] &&
			mc.accessConfig.k8sOutOfCluster.namespace == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.namespace"] &&
			mc.accessConfig.k8sOutOfCluster.labelSelector == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.labelSelector"]
	} else if allButAccessModeAsExpected && mc.accessConfig.memberAccessMode == k8sInClusterAccessMode {
		accessModeAsExpected = mc.accessConfig.k8sInCluster.labelSelector == expected[testMonkeyKeyPath+".memberAccess.k8sInCluster.labelSelector"]
	} else {
		return false
	}

	return allButAccessModeAsExpected && accessModeAsExpected

}
