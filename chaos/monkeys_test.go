package chaos

import (
	"errors"
	"fmt"
	"hazeltest/status"
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
		testConfig map[string]any
	}
	testSleeper struct {
		secondsSlept int
	}
)

const (
	validChaosProbability                    = 0.6
	invalidChaosProbability                  = -0.1
	validLabelSelector                       = "app.kubernetes.io/name=hazelcastplatform"
	invalidLabelSelector                     = ""
	invalidAbsoluteNumMembersToKill          = -1
	invalidRelativePercentageOfMembersToKill = 1.1
)

const statusKeyFinished = "finished"

var (
	testMonkeyKeyPath    = "testChaosMonkey"
	memberKillerKeyPath  = "chaosMonkeys.memberKiller"
	completeRunStateList = []state{start, populateConfigComplete, checkEnabledComplete, raiseReadyComplete, chaosStart, chaosComplete}
	sleepDisabled        = &sleepConfig{
		enabled:          false,
		durationSeconds:  1,
		enableRandomness: false,
	}
	noOpFunc = func() {}
)

func (s *testSleeper) sleep(sc *sleepConfig, _ evaluateTimeToSleep) {

	if sc.enabled {
		s.secondsSlept += sc.durationSeconds
	}

}

func (k *testHzMemberKiller) kill(members []hzMember, _ *memberAccessConfig, _ *sleepConfig) error {

	k.numInvocations++

	if k.returnError {
		return errors.New("yet another error that should have been completely impossible")
	}

	k.givenHzMember = members[0]

	return nil

}

func (c *testHzMemberChooser) choose(_ *memberAccessConfig, _ *memberSelectionConfig) ([]hzMember, error) {

	c.numInvocations++

	if c.returnError {
		return nil, errors.New("awesome error")
	}

	return []hzMember{
		{c.memberID},
	}, nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if value, ok := a.testConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	} else {
		return fmt.Errorf("test error: unable to find value in test config for given key path '%s'", keyPath)
	}

	return nil

}

func TestDefaultSleeperSleep(t *testing.T) {

	t.Log("given the default sleeper")
	{
		t.Log("\twhen sleep has been disabled")
		{
			s := defaultSleeper{}

			sleepInvoked := false
			s.sleep(sleepDisabled, func(sc *sleepConfig) int {
				sleepInvoked = true
				return 0
			})

			msg := "\t\tsleep function must not have been invoked"
			if !sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen sleep has been enabled")
		{
			s := defaultSleeper{}
			sc := &sleepConfig{
				enabled:          true,
				durationSeconds:  1,
				enableRandomness: false,
			}

			sleepInvoked := false
			s.sleep(sc, func(sc *sleepConfig) int {
				sleepInvoked = true
				return 0
			})

			msg := "\t\tsleep must have been invoked"
			if sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestMemberKillerMonkeyCauseChaos(t *testing.T) {

	t.Log("given a member killer monkey with the ability to kill hazelcast members")
	{
		t.Log("\twhen populating the member killer config returns an error")
		{
			assigner := &testConfigPropertyAssigner{assembleTestConfig(
				memberKillerKeyPath,
				true,
				invalidChaosProbability,
				10,
				relativeMemberSelectionMode,
				false,
				0,
				0.0,
				k8sInClusterAccessMode,
				validLabelSelector,
				sleepDisabled,
			)}
			m := memberKillerMonkey{}

			raiseReadyInvoked := false
			testReadyFunc := func() {
				raiseReadyInvoked = true
			}
			raiseNotReadyInvoked := false
			testNotReadyFunc := func() {
				raiseNotReadyInvoked = true
			}
			m.init(assigner, &testSleeper{}, &testHzMemberChooser{}, &testHzMemberKiller{}, status.NewGatherer(), testReadyFunc, testNotReadyFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

			msg := "\t\tstate transitions must contain only start state"
			if len(m.stateList) == 1 && m.stateList[0] == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), 0, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}

			msg = "\t\tno api status function must have been invoked"
			if !raiseReadyInvoked {
				t.Log(msg, checkMark, "raiseReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseReadyFunc")
			}
			if !raiseNotReadyInvoked {
				t.Log(msg, checkMark, "raiseNotReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseNotReadyFunc")
			}
		}
		genericMsg := "\t\tstate transitions must be correct"
		t.Log("\twhen monkey is disabled")
		{
			testConfig := assembleTestConfig(
				memberKillerKeyPath,
				false,
				validChaosProbability,
				10,
				relativeMemberSelectionMode,
				false,
				0,
				0.0,
				k8sInClusterAccessMode,
				validLabelSelector,
				sleepDisabled,
			)
			assigner := &testConfigPropertyAssigner{testConfig}
			m := memberKillerMonkey{}

			raiseReadyInvoked := false
			testReadyFunc := func() {
				raiseReadyInvoked = true
			}
			raiseNotReadyInvoked := false
			testNotReadyFunc := func() {
				raiseNotReadyInvoked = true
			}
			m.init(assigner, &testSleeper{}, &testHzMemberChooser{}, &testHzMemberKiller{}, status.NewGatherer(), testReadyFunc, testNotReadyFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

			if detail, ok := checkMonkeyStateTransitions([]state{start, populateConfigComplete}, m.stateList); ok {
				t.Log(genericMsg, checkMark)
			} else {
				t.Fatal(genericMsg, ballotX, detail)
			}

			msg := "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), 10, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}

			msg = "\t\tno api status function must have been invoked"
			if !raiseReadyInvoked {
				t.Log(msg, checkMark, "raiseReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseReadyFunc")
			}
			if !raiseNotReadyInvoked {
				t.Log(msg, checkMark, "raiseNotReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseNotReadyFunc")
			}
		}
		t.Log("\twhen non-zero number of runs is configured and chaos probability is 100 %")
		{
			numRuns := 9
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			hzMemberID := "hazelcastplatform-ß"
			chooser := &testHzMemberChooser{memberID: hzMemberID}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{}

			raiseReadyInvoked := false
			testReadyFunc := func() {
				raiseReadyInvoked = true
			}
			raiseNotReadyInvoked := false
			testNotReadyFunc := func() {
				raiseNotReadyInvoked = true
			}
			m.init(assigner, &testSleeper{}, chooser, killer, status.NewGatherer(), testReadyFunc, testNotReadyFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

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

			msg = "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), numRuns, numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}

			msg = "\t\tapi status function must have been invoked"
			if raiseReadyInvoked {
				t.Log(msg, checkMark, "raiseReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseReadyFunc")
			}
			if raiseNotReadyInvoked {
				t.Log(msg, checkMark, "raiseNotReadyFunc")
			} else {
				t.Fatal(msg, ballotX, "raiseNotReadyFunc")
			}
		}
		t.Log("\twhen chaos probability is set to zero")
		{
			numRuns := 9
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					0.0,
					numRuns,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{}

			m.init(assigner, &testSleeper{}, chooser, killer, status.NewGatherer(), noOpFunc, notReadyFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

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

			msg = "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), numRuns, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}
		t.Log("\twhen chooser yields error")
		{
			numRuns := 3
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{returnError: true}
			killer := &testHzMemberKiller{}
			m := memberKillerMonkey{}
			m.init(assigner, &testSleeper{}, chooser, killer, status.NewGatherer(), noOpFunc, noOpFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

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

			msg = "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), numRuns, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}
		t.Log("\twhen killer yields an error")
		{
			numRuns := 3
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{returnError: true}
			m := memberKillerMonkey{}
			m.init(assigner, &testSleeper{}, chooser, killer, status.NewGatherer(), noOpFunc, noOpFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

			msg := "\t\tinvocations of both chooser and killer must be retried in next run"
			if chooser.numInvocations == numRuns && killer.numInvocations == numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmonkey status must contain expected values"
			if ok, key, detail := statusContainsExpectedValues(m.g.AssembleStatusCopy(), numRuns, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}
		t.Log("\twhen sleep has been disabled")
		{
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					10,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sleepDisabled,
				)}
			s := &testSleeper{}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{returnError: true}
			m := memberKillerMonkey{}
			m.init(assigner, s, chooser, killer, status.NewGatherer(), noOpFunc, noOpFunc)

			m.causeChaos()
			waitForStatusGatheringDone(m.g)

			msg := "\t\ttime slept must be zero"
			if s.secondsSlept == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen static sleep has been enabled")
		{
			numRuns := 9
			sc := &sleepConfig{
				enabled:          true,
				durationSeconds:  10,
				enableRandomness: false,
			}
			assigner := &testConfigPropertyAssigner{
				assembleTestConfig(
					memberKillerKeyPath,
					true,
					1.0,
					numRuns,
					relativeMemberSelectionMode,
					false,
					0,
					0.0,
					k8sInClusterAccessMode,
					validLabelSelector,
					sc,
				)}
			s := &testSleeper{}
			chooser := &testHzMemberChooser{}
			killer := &testHzMemberKiller{returnError: true}
			m := memberKillerMonkey{}
			m.init(assigner, s, chooser, killer, status.NewGatherer(), noOpFunc, noOpFunc)

			m.causeChaos()

			msg := "\t\ttime slept must be equal to number of runs into number of seconds given as sleep time"
			if s.secondsSlept == numRuns*sc.durationSeconds {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestPopulateMemberAccessConfig(t *testing.T) {

	t.Log("given the config builder's method to populate the member access config")
	{
		b := monkeyConfigBuilder{monkeyKeyPath: testMonkeyKeyPath}
		for _, accessMode := range []string{k8sOutOfClusterAccessMode, k8sInClusterAccessMode} {
			t.Logf("\twhen access mode '%s' is given", accessMode)
			{
				t.Log("\t\twhen all properties are valid")
				{
					testMemberAccessConfig := assembleTestMemberAccessConfig(testMonkeyKeyPath, accessMode, validLabelSelector)
					assigner := testConfigPropertyAssigner{testMemberAccessConfig}
					ac, err := b.populateMemberAccessConfig(assigner, accessMode)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tconfig must be returned"
					if ac != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tconfig must contain correct values"
					if memberAccessConfigAsExpected(ac, testMemberAccessConfig) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\twhen at least one property is invalid")
				{
					testMemberAccessConfig := assembleTestMemberAccessConfig(testMonkeyKeyPath, accessMode, invalidLabelSelector)
					assigner := testConfigPropertyAssigner{testMemberAccessConfig}
					ac, err := b.populateMemberAccessConfig(assigner, accessMode)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\treturned config must be nil"
					if ac == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
		}
		t.Log("\twhen unknown member access mode is given")
		{
			unknownAccessMode := "someUnknownAccessMode"
			testMemberAccessConfig := assembleTestMemberAccessConfig(testMonkeyKeyPath, unknownAccessMode, validLabelSelector)
			assigner := testConfigPropertyAssigner{testMemberAccessConfig}
			ac, err := b.populateMemberAccessConfig(assigner, unknownAccessMode)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must contain information on unknown access mode"
			if strings.Contains(err.Error(), unknownAccessMode) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err, unknownAccessMode)
			}

			msg = "\t\tconfig must be nil"
			if ac == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given the config builder's method to populate the member killer config")
	{
		b := monkeyConfigBuilder{monkeyKeyPath: testMonkeyKeyPath}
		t.Log("\twhen valid values are provided for all properties and no property assignment yields an error")
		{
			testConfig := assembleTestConfig(
				testMonkeyKeyPath,
				true,
				validChaosProbability,
				42,
				relativeMemberSelectionMode,
				true,
				0,
				0.3,
				k8sOutOfClusterAccessMode,
				validLabelSelector,
				sleepDisabled,
			)
			assigner := testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig(assigner)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig must be returned"
			if mc != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig must contain expected values"
			if configValuesAsExpected(mc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen top-level property assignment yields an error")
		{
			testConfig := assembleTestConfig(
				testMonkeyKeyPath,
				true,
				invalidChaosProbability,
				10,
				relativeMemberSelectionMode,
				false,
				0,
				0.0,
				k8sInClusterAccessMode,
				validLabelSelector,
				sleepDisabled,
			)
			assigner := testConfigPropertyAssigner{testConfig}
			mc, err := b.populateConfig(assigner)

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

		t.Log("\twhen member selection mode property assignment yields an error")
		{
			for _, selectionMode := range []string{relativeMemberSelectionMode, absoluteMemberSelectionMode} {
				t.Logf("\t\t%s", selectionMode)
				{
					testConfig := assembleTestConfig(
						testMonkeyKeyPath,
						true,
						validChaosProbability,
						42,
						selectionMode,
						true,
						invalidAbsoluteNumMembersToKill,
						invalidRelativePercentageOfMembersToKill,
						k8sInClusterAccessMode,
						validLabelSelector,
						sleepDisabled,
					)
					assigner := testConfigPropertyAssigner{testConfig}
					mc, err := b.populateConfig(assigner)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tconfig must be nil"
					if mc == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
		}

		t.Log("\twhen k8s access mode property assignment yields an error")
		{
			for _, accessMode := range []string{k8sOutOfClusterAccessMode, k8sInClusterAccessMode} {
				t.Logf("\t\t%s", accessMode)
				{
					testConfig := assembleTestConfig(
						testMonkeyKeyPath,
						true,
						validChaosProbability,
						10,
						relativeMemberSelectionMode,
						false,
						0,
						0.0,
						accessMode,
						invalidLabelSelector,
						sleepDisabled,
					)
					assigner := testConfigPropertyAssigner{testConfig}
					mc, err := b.populateConfig(assigner)

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
	}

}

func waitForStatusGatheringDone(g status.Gatherer) {

	for {
		if done := g.ListeningStopped(); done {
			break
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

func statusContainsExpectedValues(status map[string]any, expectedNumRuns, expectedNumMembersKilled int,
	expectedMonkeyFinished bool) (bool, string, string) {

	if numRunsFromStatus, ok := status[statusKeyNumRuns]; !ok || numRunsFromStatus != uint32(expectedNumRuns) {
		return false, statusKeyNumRuns, fmt.Sprintf("expected: %d, got: %d", expectedNumRuns, numRunsFromStatus)
	}

	if numMembersKilledFromStatus, ok := status[statusKeyNumMembersKilled]; !ok || numMembersKilledFromStatus != uint32(expectedNumMembersKilled) {
		return false, statusKeyNumMembersKilled, fmt.Sprintf("expected: %d, got: %d", expectedNumMembersKilled, numMembersKilledFromStatus)
	}

	if monkeyFinishedFromStatus, ok := status[statusKeyFinished]; !ok || monkeyFinishedFromStatus != expectedMonkeyFinished {
		return false, statusKeyFinished, fmt.Sprintf("expected: %t; got: %d", expectedMonkeyFinished, monkeyFinishedFromStatus)
	}

	return true, "", ""

}

func assembleTestMemberAccessConfig(keyPath, memberAccessMode, labelSelector string) map[string]any {

	return map[string]any{
		keyPath + ".memberAccess.mode":                          memberAccessMode,
		keyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":    "default",
		keyPath + ".memberAccess.k8sOutOfCluster.namespace":     "hazelcastplatform",
		keyPath + ".memberAccess.k8sOutOfCluster.labelSelector": labelSelector,
		keyPath + ".memberAccess.k8sInCluster.labelSelector":    labelSelector,
	}

}

func assembleTestConfig(
	keyPath string,
	enabled bool,
	chaosProbability float64,
	numRuns int,
	memberSelectionMode string,
	targetOnlyActive bool,
	absoluteNumMembersToKill int,
	relativePercentageOfMembersToKill float32,
	memberAccessMode, labelSelector string,
	sleep *sleepConfig,
) map[string]any {

	return map[string]any{
		keyPath + ".enabled":                                            enabled,
		keyPath + ".numRuns":                                            numRuns,
		keyPath + ".chaosProbability":                                   chaosProbability,
		keyPath + ".memberSelection.mode":                               memberSelectionMode,
		keyPath + ".memberSelection.targetOnlyActive":                   targetOnlyActive,
		keyPath + ".memberSelection.absolute.numMembersToKill":          absoluteNumMembersToKill,
		keyPath + ".memberSelection.relative.percentageOfMembersToKill": relativePercentageOfMembersToKill,
		keyPath + ".memberAccess.mode":                                  memberAccessMode,
		keyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":            "default",
		keyPath + ".memberAccess.k8sOutOfCluster.namespace":             "hazelcastplatform",
		keyPath + ".memberAccess.k8sOutOfCluster.labelSelector":         labelSelector,
		keyPath + ".memberAccess.k8sInCluster.labelSelector":            labelSelector,
		keyPath + ".sleep.enabled":                                      sleep.enabled,
		keyPath + ".sleep.durationSeconds":                              sleep.durationSeconds,
		keyPath + ".sleep.enableRandomness":                             sleep.enableRandomness,
		keyPath + ".memberGrace.enabled":                                true,
		keyPath + ".memberGrace.durationSeconds":                        30,
		keyPath + ".memberGrace.enableRandomness":                       true,
	}

}

func configValuesAsExpected(mc *monkeyConfig, expected map[string]any) bool {

	allExceptSelectionModeAndAccessModeAsExpected := mc.enabled == expected[testMonkeyKeyPath+".enabled"] &&
		mc.numRuns == uint32(expected[testMonkeyKeyPath+".numRuns"].(int)) &&
		mc.chaosProbability == expected[testMonkeyKeyPath+".chaosProbability"] &&
		mc.selectionConfig.selectionMode == expected[testMonkeyKeyPath+".memberSelection.mode"] &&
		mc.selectionConfig.targetOnlyActive == expected[testMonkeyKeyPath+".memberSelection.targetOnlyActive"] &&
		mc.sleep.enabled == expected[testMonkeyKeyPath+".sleep.enabled"] &&
		mc.sleep.durationSeconds == expected[testMonkeyKeyPath+".sleep.durationSeconds"] &&
		mc.sleep.enableRandomness == expected[testMonkeyKeyPath+".sleep.enableRandomness"] &&
		mc.memberGrace.enabled == expected[testMonkeyKeyPath+".memberGrace.enabled"] &&
		mc.memberGrace.durationSeconds == expected[testMonkeyKeyPath+".memberGrace.durationSeconds"] &&
		mc.memberGrace.enableRandomness == expected[testMonkeyKeyPath+".memberGrace.enableRandomness"]

	return allExceptSelectionModeAndAccessModeAsExpected &&
		memberSelectionConfigAsExpected(mc.selectionConfig, expected) &&
		memberAccessConfigAsExpected(mc.accessConfig, expected)

}

func memberSelectionConfigAsExpected(sc *memberSelectionConfig, expected map[string]any) bool {

	modeAsExpected := sc.selectionMode == expected[testMonkeyKeyPath+".memberSelection.mode"]

	if !modeAsExpected {
		return false
	}

	if sc.selectionMode == relativeMemberSelectionMode {
		return sc.relativePercentageOfMembersToKill == expected[testMonkeyKeyPath+".memberSelection.relative.percentageOfMembersToKill"]
	} else if sc.selectionMode == absoluteMemberSelectionMode {
		return sc.absoluteNumMembersToKill == expected[testMonkeyKeyPath+".memberSelection.absolute.numMembersToKill"]
	}

	return false

}

func memberAccessConfigAsExpected(ac *memberAccessConfig, expected map[string]any) bool {

	modeAsExpected := ac.accessMode == expected[testMonkeyKeyPath+".memberAccess.mode"]

	if !modeAsExpected {
		return false
	}

	if ac.accessMode == k8sOutOfClusterAccessMode {
		return ac.k8sOutOfCluster.kubeconfig == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.kubeconfig"] &&
			ac.k8sOutOfCluster.namespace == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.namespace"] &&
			ac.k8sOutOfCluster.labelSelector == expected[testMonkeyKeyPath+".memberAccess.k8sOutOfCluster.labelSelector"]
	} else if ac.accessMode == k8sInClusterAccessMode {
		return ac.k8sInCluster.labelSelector == expected[testMonkeyKeyPath+".memberAccess.k8sInCluster.labelSelector"]
	}

	return false

}
