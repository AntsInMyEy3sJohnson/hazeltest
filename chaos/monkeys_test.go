package chaos

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type (
	testHzMemberChooser struct {
		returnError    bool
		memberID       string
		numInvocations int
	}
	testHzMemberKiller struct {
		returnError    bool
		numInvocations int
		givenHzMember  hzMember
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
	testMonkeyKeyPath    = "testChaosMonkey"
	memberKillerKeyPath  = "chaosMonkeys.memberKiller"
	completeRunStateList = []state{start, populateConfigComplete, checkEnabledComplete, raiseReadyComplete, chaosStart, chaosComplete}
	sleepEnabled         = sleepConfig{
		enabled:          true,
		durationSeconds:  10,
		enableRandomness: false,
	}
	sleepDisabled = sleepConfig{
		enabled:          false,
		durationSeconds:  1,
		enableRandomness: false,
	}
)

func (k *testHzMemberKiller) kill(member hzMember, _ memberAccessConfig, _ sleepConfig) error {

	k.numInvocations++

	if k.returnError {
		return errors.New("yet another error that should have been completely impossible")
	}

	k.givenHzMember = member

	return nil

}

func (c *testHzMemberChooser) choose(_ memberAccessConfig) (hzMember, error) {

	c.numInvocations++

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
			propertyAssigner = &testConfigPropertyAssigner{assembleTestConfig(memberKillerKeyPath, true, invalidChaosProbability, 10, k8sInClusterAccessMode, validLabelSelector, sleepDisabled)}
			m := memberKillerMonkey{chooser: &testHzMemberChooser{}, killer: &testHzMemberKiller{}}

			m.causeChaos()

			msg := "\t\tstate transitions must contain only start state"
			if len(m.stateList) == 1 && m.stateList[0] == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		genericMsg := "\t\tstate transitions must be correct"
		t.Log("\twhen monkey is disabled")
		{
			testConfig := assembleTestConfig(memberKillerKeyPath, false, validChaosProbability, 10, k8sInClusterAccessMode, validLabelSelector, sleepDisabled)
			propertyAssigner = &testConfigPropertyAssigner{testConfig}
			m := memberKillerMonkey{chooser: &testHzMemberChooser{}, killer: &testHzMemberKiller{}}

			m.causeChaos()

			if detail, ok := checkMonkeyStateTransitions([]state{start, populateConfigComplete}, m.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, detail)
			}
		}
		t.Log("\twhen non-zero number of runs is configured")
		{
			numRuns := 9
			propertyAssigner = &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			hzMemberID := "hazelcastimdg-ß"
			chooser := &testHzMemberChooser{memberID: hzMemberID}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{chooser: chooser, killer: killer}

			m.causeChaos()

			if detail, ok := checkMonkeyStateTransitions(completeRunStateList, m.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, detail)
			}

			msg := "\t\tmember chooser must have expected number of invocations"
			if chooser.numInvocations == numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", chooser.numInvocations, numRuns))
			}

			msg = "\t\tmember killer must have expected number of invocations"
			if killer.numInvocations == numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d != %d", killer.numInvocations, numRuns))
			}

			msg = "\t\tkiller's invocation argument must contain previously chosen hazelcast member"
			if killer.givenHzMember.identifier == hzMemberID {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, killer.givenHzMember.identifier)
			}
		}
		t.Log("\twhen chaos probability is given as 0.0")
		{
			propertyAssigner = &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					0.0,
					9,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{chooser: chooser, killer: killer}

			m.causeChaos()

			if detail, ok := checkMonkeyStateTransitions(completeRunStateList, m.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, detail)
			}

			msg := "\t\truns must be skipped"
			if chooser.numInvocations == 0 && killer.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen chooser yields error")
		{
			numRuns := 3
			propertyAssigner = &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{returnError: true}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{chooser: chooser, killer: killer}

			m.causeChaos()

			msg := "\t\tchooser invocation must be re-tried in next run"
			if chooser.numInvocations == numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tkiller must not be invoked"
			if killer.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen killer yields an error")
		{
			numRuns := 3
			propertyAssigner = &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{returnError: true}
			m := memberKillerMonkey{chooser: chooser, killer: killer}

			m.causeChaos()

			msg := "\t\tinvocations of both chooser and killer must be retried in next run"
			if chooser.numInvocations == numRuns && killer.numInvocations == numRuns {
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
			testConfig := assembleTestConfig(
				testMonkeyKeyPath,
				true,
				validChaosProbability,
				10,
				k8sOutOfClusterAccessMode,
				validLabelSelector,
				sleepDisabled,
			)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, true, validChaosProbability, 10, k8sInClusterAccessMode, validLabelSelector, sleepDisabled)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, true, invalidChaosProbability, 10, k8sInClusterAccessMode, validLabelSelector, sleepDisabled)
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
					testConfig := assembleTestConfig(testMonkeyKeyPath, true, validChaosProbability, 10, accessMode, invalidLabelSelector, sleepDisabled)
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
			testConfig := assembleTestConfig(testMonkeyKeyPath, true, validChaosProbability, 10, unknownAccessMode, validLabelSelector, sleepDisabled)
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

func checkMonkeyStateTransitions(expected []state, actual []state) (string, bool) {

	if len(expected) != len(actual) {
		return fmt.Sprintf("expected %d state transition(-s), got %d", len(expected), len(actual)), false
	}

	for i, expectedValue := range expected {
		if actual[i] != expectedValue {
			return fmt.Sprintf("expected '%s' in index '%d', got '%s'", expectedValue, i, actual[i]), false
		}
	}

	return "", true

}

func assembleTestConfig(keyPath string, enabled bool, chaosProbability float64, numRuns int, memberAccessMode, labelSelector string, sleep sleepConfig) map[string]any {

	return map[string]any{
		keyPath + ".enabled":                                    enabled,
		keyPath + ".numRuns":                                    numRuns,
		keyPath + ".chaosProbability":                           chaosProbability,
		keyPath + ".memberAccess.mode":                          memberAccessMode,
		keyPath + ".memberAccess.targetOnlyActive":              true,
		keyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":    "default",
		keyPath + ".memberAccess.k8sOutOfCluster.namespace":     "hazelcastplatform",
		keyPath + ".memberAccess.k8sOutOfCluster.labelSelector": labelSelector,
		keyPath + ".memberAccess.k8sInCluster.labelSelector":    labelSelector,
		keyPath + ".sleep.enabled":                              sleep.enabled,
		keyPath + ".sleep.durationSeconds":                      sleep.durationSeconds,
		keyPath + ".sleep.enableRandomness":                     sleep.enableRandomness,
		keyPath + ".memberGrace.enabled":                        true,
		keyPath + ".memberGrace.durationSeconds":                30,
		keyPath + ".memberGrace.enableRandomness":               true,
	}

}

func configValuesAsExpected(mc *monkeyConfig, expected map[string]any) bool {

	allButAccessModeAsExpected := mc.enabled == expected[testMonkeyKeyPath+".enabled"] &&
		mc.numRuns == uint32(expected[testMonkeyKeyPath+".numRuns"].(int)) &&
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
