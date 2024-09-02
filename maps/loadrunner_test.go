package maps

import (
	"context"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"strconv"
	"testing"
)

type (
	testLoadTestLoop struct {
		assignedTestLoopExecution *testLoopExecution[loadElement]
		observations              *testLoadTestLoopObservations
	}
	testLoadTestLoopObservations struct {
		numNewLooperInvocations  int
		numInitLooperInvocations int
		numRunInvocations        int
	}
)

func (d *testLoadTestLoop) init(tle *testLoopExecution[loadElement], _ sleeper, _ *status.Gatherer) {
	d.observations.numInitLooperInvocations++
	d.assignedTestLoopExecution = tle
}

func (d *testLoadTestLoop) run() {
	d.observations.numRunInvocations++
}

func newTestLoadTestLoop() *testLoadTestLoop {
	return &testLoadTestLoop{
		observations: &testLoadTestLoopObservations{},
	}
}

func TestInitializeLoadElementTestLoop(t *testing.T) {

	t.Log("given a function to initialize the test loop from the provided loop type")
	{
		t.Log("\twhen boundary test loop type is provided")
		{
			l, err := newLoadElementTestLoop(&runnerConfig{loopType: boundary})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*boundaryTestLoop[loadElement]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen batch test loop type is provided")
		{
			l, err := newLoadElementTestLoop(&runnerConfig{loopType: batch})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*batchTestLoop[loadElement]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen unknown test loop type is provided")
		{
			l, err := newLoadElementTestLoop(&runnerConfig{loopType: "saruman"})

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlooper must be nil"
			if l == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestRunLoadMapTests(t *testing.T) {

	t.Log("given a load runner to run map test loops")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsgStateTransitions := "\t\tstate transitions must be correct"
		genericMsgLatestStateInGatherer := "\t\tlatest state in gatherer must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				testConfig:  nil,
			}
			l := &testLoadTestLoop{}
			r := loadRunner{
				assigner:   assigner,
				stateList:  []runnerState{},
				hzMapStore: testHzMapStore{},
				providerFuncs: struct {
					mapStore            newMapStoreFunc
					loadElementTestLoop newLoadElementTestLoopFunc
				}{mapStore: newTestMapStore, loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
					return l, nil
				}},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions([]runnerState{start}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(r.gatherer, start) {
				t.Log(genericMsgLatestStateInGatherer, checkMark, start)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, start)
			}

			msg := "\t\tgatherer instance must have been set"
			if gatherer == r.gatherer {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen runner has been disabled")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.load.enabled": false,
				},
			}
			ch := &testHzClientHandler{}
			r := loadRunner{assigner: assigner, stateList: []runnerState{}, hzClientHandler: ch}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			latestState := populateConfigComplete
			if msg, ok := checkRunnerStateTransitions([]runnerState{start, latestState}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(r.gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark, latestState)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}

			msg := "\t\thazelcast client handler must not have initialized hazelcast client"
			if ch.initClientInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.initClientInvocations)
			}

			msg = "\t\tsimilarly, hazelcast client handler must not have performed shutdown on hazelcast client"
			if ch.shutdownInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.shutdownInvocations)
			}
		}
		t.Log("\twhen test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.load.enabled":                      true,
					"mapTests.load.testLoop.type":                "batch",
					"mapTests.load.payload.variableSize.enabled": true,
				},
			}
			ch := &testHzClientHandler{}
			ms := &testHzMapStore{observations: &testHzMapStoreObservations{}}
			l := newTestLoadTestLoop()

			r := loadRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				providerFuncs: struct {
					mapStore            newMapStoreFunc
					loadElementTestLoop newLoadElementTestLoopFunc
				}{mapStore: func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.MapStore {
					ms.observations.numInitInvocations++
					return ms
				}, loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
					l.observations.numNewLooperInvocations++
					return l, nil
				}},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			latestState := expectedStatesForFullRun[len(expectedStatesForFullRun)-1]
			if latestStatePresentInGatherer(r.gatherer, latestState) {
				t.Log(genericMsgLatestStateInGatherer, checkMark, latestState)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, latestState)
			}

			msg := "\t\thazelcast client handler must have initialized hazelcast client once"
			if ch.initClientInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.initClientInvocations)
			}

			msg = "\t\thazelcast client handler must have performed shutdown of hazelcast client once"
			if ch.shutdownInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.shutdownInvocations)
			}

			msg = "\t\tmap store must have been initialized once"
			if ms.observations.numInitInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.observations.numInitInvocations)
			}

			msg = "\t\tlooper must have been created once"
			if l.observations.numNewLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numNewLooperInvocations)
			}

			msg = "\t\tlooper must have been initialized once"
			if l.observations.numInitLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numInitLooperInvocations)
			}

			msg = "\t\tlooper must have been run once"
			if l.observations.numRunInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numRunInvocations)
			}

		}
		t.Log("\twhen test loop cannot be initialized")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.load.enabled":       true,
					"mapTests.load.testLoop.type": "awesome-non-existing-test-loop-type",
				},
			}
			ch := &testHzClientHandler{}
			r := loadRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				gatherer:        status.NewGatherer(),
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions([]runnerState{start}, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)

			if latestStatePresentInGatherer(r.gatherer, start) {
				t.Log(genericMsgLatestStateInGatherer, checkMark, start)
			} else {
				t.Fatal(genericMsgLatestStateInGatherer, ballotX, start)
			}

			msg := "\t\thazelcast client handler must not have initialized hazelcast client"
			if ch.initClientInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.initClientInvocations)
			}

			msg = "\t\tsimilarly, hazelcast client handler must not have performed shutdown on hazelcast client"
			if ch.shutdownInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.shutdownInvocations)
			}
		}
		t.Log("\twhen usage of fixed-size load elements was enabled")
		{
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                   true,
				"mapTests.load.testLoop.type":             string(batch),
				"mapTests.load.payload.fixedSize.enabled": true,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				providerFuncs: struct {
					mapStore            newMapStoreFunc
					loadElementTestLoop newLoadElementTestLoopFunc
				}{mapStore: newTestMapStore, loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
					l.observations.numNewLooperInvocations++
					return l, nil
				}},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			numEntriesPerMap = 9
			fixedPayloadSizeBytes = 3
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			waitForStatusGatheringDone(gatherer)

			msg := "\t\ttest loop must have been created once"
			if l.observations.numNewLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numNewLooperInvocations)
			}

			msg = "\t\ttest loop must have been initialized once"
			if l.observations.numInitLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numInitLooperInvocations)
			}

			msg = "\t\tnumber of generated load elements must be correct"
			elements := l.assignedTestLoopExecution.elements
			if len(elements) == numEntriesPerMap {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(elements))
			}

			msg = "\t\tboth key and payload must have been populated on each generated load element"
			for i, v := range elements {
				if v.Key == strconv.Itoa(i) && len(v.Payload) == fixedPayloadSizeBytes {
					t.Log(msg, checkMark, v.Key)
				} else {
					t.Fatal(msg, ballotX, v.Key)
				}
			}

			msg = "\t\ttest loop must have been run once"
			if l.observations.numRunInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numRunInvocations)
			}
		}
		t.Log("\twhen usage of variable-size load elements was enabled")
		{
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                      true,
				"mapTests.load.testLoop.type":                string(batch),
				"mapTests.load.payload.fixedSize.enabled":    false,
				"mapTests.load.payload.variableSize.enabled": true,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				providerFuncs: struct {
					mapStore            newMapStoreFunc
					loadElementTestLoop newLoadElementTestLoopFunc
				}{mapStore: newTestMapStore, loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
					l.observations.numNewLooperInvocations++
					return l, nil
				}},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			numEntriesPerMap = 9
			fixedPayloadSizeBytes = 3
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			waitForStatusGatheringDone(gatherer)

			msg := "\t\ttest loop must have been created once"
			if l.observations.numNewLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numNewLooperInvocations)
			}

			msg = "\t\ttest loop must have been initialized once"
			if l.observations.numInitLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numInitLooperInvocations)
			}

			msg = "\t\tnumber of generated load elements must be correct"
			elements := l.assignedTestLoopExecution.elements
			if len(elements) == numEntriesPerMap {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(elements))
			}

			msg = "\t\tonly keys of load elements must have been populated"
			for i, v := range elements {
				if v.Key == strconv.Itoa(i) && v.Payload == "" {
					t.Log(msg, checkMark, v.Key)
				} else {
					t.Fatal(msg, ballotX, v.Key)
				}
			}

			msg = "\t\ttest loop must have been run once"
			if l.observations.numRunInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numRunInvocations)
			}
		}
		t.Log("\twhen neither fixed-size nor variable-size load elements were enabled")
		{
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                      true,
				"mapTests.load.testLoop.type":                string(batch),
				"mapTests.load.payload.fixedSize.enabled":    false,
				"mapTests.load.payload.variableSize.enabled": false,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				providerFuncs: struct {
					mapStore            newMapStoreFunc
					loadElementTestLoop newLoadElementTestLoopFunc
				}{mapStore: newTestMapStore, loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
					l.observations.numNewLooperInvocations++
					return l, nil
				}},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			numEntriesPerMap = 9
			fixedPayloadSizeBytes = 3
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			waitForStatusGatheringDone(gatherer)

			msg := "\t\ttest loop must have been created once"
			if l.observations.numNewLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numNewLooperInvocations)
			}

			msg = "\t\ttest loop must not have been initialized"
			if l.observations.numInitLooperInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numInitLooperInvocations)
			}

			msg = "\t\tno test loop execution must have been assigned"
			if l.assignedTestLoopExecution == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loop must not have been run"
			if l.observations.numRunInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numRunInvocations)
			}

			msg = "\t\trunner state list must contain expected state transitions"
			if detail, ok := checkRunnerStateTransitions([]runnerState{start, populateConfigComplete, checkEnabledComplete, assignTestLoopComplete}, r.stateList); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}
	}

}

func TestPopulateLoadConfig(t *testing.T) {

	t.Log("given set of configuration properties to populate the load config from")
	{
		t.Log("\twhen property contains invalid value")
		{
			a := &testConfigPropertyAssigner{
				testConfig: map[string]any{
					testMapRunnerKeyPath + ".numEntriesPerMap": "i find your lack of faith disturbing",
				},
			}

			cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned config must be nil"
			if cfg == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen properties are correct")
		{
			tc := assembleTestConfigForTestLoopType(boundary)
			a := &testConfigPropertyAssigner{testConfig: tc}

			cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tconfig should contain expected values"
			if valid, detail := configValuesAsExpected(cfg, tc); valid {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tconfig values specific to load runner must have been correctly populated, too"
			if numEntriesPerMap == tc[testMapRunnerKeyPath+".numEntriesPerMap"].(int) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			keyPath := testMapRunnerKeyPath + ".payload.fixedSize.enabled"
			if useFixedPayload == tc[keyPath].(bool) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.fixedSize.sizeBytes"
			if fixedPayloadSizeBytes == tc[keyPath].(int) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.variableSize.enabled"
			if useVariablePayload == tc[keyPath].(bool) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.variableSize.lowerBoundaryBytes"
			if variablePayloadSizeLowerBoundaryBytes == tc[keyPath].(int) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.variableSize.upperBoundaryBytes"
			if variablePayloadSizeUpperBoundaryBytes == tc[keyPath].(int) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.variableSize.evaluateNewSizeAfterNumWriteActions"
			if variablePayloadEvaluateNewSizeAfterNumWriteActions == tc[keyPath].(int) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

		}
	}

}
