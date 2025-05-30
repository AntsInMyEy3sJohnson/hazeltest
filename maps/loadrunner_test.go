package maps

import (
	"context"
	"errors"
	"fmt"
	"hazeltest/hazelcastwrapper"
	"hazeltest/loadsupport"
	"hazeltest/status"
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
	testPayloadProvider struct {
		observations *testPayloadProviderObservations
		behavior     *testPayloadProviderBehavior
	}
	testPayloadProviderObservations struct {
		requirementRegistrations map[string]loadsupport.PayloadGenerationRequirement
		payloadRetrievals        map[string]struct{}
	}
	testPayloadProviderBehavior struct {
		returnErrorUponPayloadRetrieval bool
		payloadsToReturn                map[string]*loadsupport.PayloadWrapper
	}
)

func (d *testLoadTestLoop) init(tle *testLoopExecution[loadElement], _ sleeper, _ status.Gatherer) {
	d.observations.numInitLooperInvocations++
	d.assignedTestLoopExecution = tle
}

func (d *testLoadTestLoop) run() {
	d.observations.numRunInvocations++
}

func (p *testPayloadProvider) RegisterPayloadGenerationRequirement(actorBaseName string, r loadsupport.PayloadGenerationRequirement) {
	p.observations.requirementRegistrations[actorBaseName] = r
}

func (p *testPayloadProvider) RetrievePayload(actorName string) (*loadsupport.PayloadWrapper, error) {
	p.observations.payloadRetrievals[actorName] = struct{}{}

	if p.behavior.returnErrorUponPayloadRetrieval {
		return nil, errors.New("the error everybody told you would never happen")
	}

	return p.behavior.payloadsToReturn[actorName], nil
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

func TestLoadRunner_runMapTests(t *testing.T) {

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
				providerFunctions: providerFuncs{
					mapStore: newTestMapStore,
					loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
						return l, nil
					},
					payloads: nil,
				},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen(make(chan struct{}, 1))
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
			go gatherer.Listen(make(chan struct{}, 1))

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
		t.Log("\twhen test loop was executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.load.enabled":                                 true,
					"mapTests.load.testLoop.type":                           "batch",
					"mapTests.load.payload.variableSize.enabled":            true,
					"mapTests.load.payload.variableSize.lowerBoundaryBytes": 42,
					"mapTests.load.payload.variableSize.upperBoundaryBytes": 43,
				},
			}
			ch := &testHzClientHandler{}
			ms := &testHzMapStore{observations: &testHzMapStoreObservations{}}
			l := newTestLoadTestLoop()
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					requirementRegistrations: map[string]loadsupport.PayloadGenerationRequirement{},
				},
			}
			r := loadRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				providerFunctions: providerFuncs{
					mapStore: func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.MapStore {
						ms.observations.numInitInvocations++
						return ms
					},
					loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
						l.observations.numNewLooperInvocations++
						return l, nil
					},
					payloads: func() loadsupport.PayloadProvider {
						return tp
					},
				},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen(make(chan struct{}, 1))

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

			msg = "\t\tget or assemble payload function of test loop execution must be populated"
			if l.assignedTestLoopExecution.getOrAssemblePayload != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tgeneration requirement for variable-size payloads must have been correctly registered"
			registeredRequirements := tp.observations.requirementRegistrations
			registeredForActor, ok := registeredRequirements[mapLoadRunnerName]
			if len(registeredRequirements) == 1 && ok &&
				!registeredForActor.UseFixedSize &&
				registeredForActor.UseVariableSize &&
				registeredForActor.VariableSize.SameSizeStepsLimit == variablePayloadEvaluateNewSizeAfterNumWriteActions &&
				registeredForActor.VariableSize.LowerBoundaryBytes == variablePayloadSizeLowerBoundaryBytes &&
				registeredForActor.VariableSize.UpperBoundaryBytes == variablePayloadSizeUpperBoundaryBytes {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
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
			go gatherer.Listen(make(chan struct{}, 1))
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
			numEntriesPerMap := 9
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                      true,
				"mapTests.load.numEntriesPerMap":             numEntriesPerMap,
				"mapTests.load.testLoop.type":                string(batch),
				"mapTests.load.payload.fixedSize.enabled":    true,
				"mapTests.load.payload.variableSize.enabled": false,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					requirementRegistrations: map[string]loadsupport.PayloadGenerationRequirement{},
				},
			}
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				name:            mapLoadRunnerName,
				providerFunctions: providerFuncs{
					mapStore: newTestMapStore,
					loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
						l.observations.numNewLooperInvocations++
						return l, nil
					},
					payloads: func() loadsupport.PayloadProvider {
						return tp
					},
				},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen(make(chan struct{}, 1))

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

			// Unlike the Pokedex Runner, the Load Runner does not rely on
			// pre-initialized load elements, so the elements slice in the
			// assigned test loop execution must be empty.
			msg = "\t\tnumber of generated load elements must be zero"
			if len(l.assignedTestLoopExecution.elements) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(l.assignedTestLoopExecution.elements))
			}

			msg = "\t\tconfig must have been correctly populated with number of map elements"
			if l.assignedTestLoopExecution.runnerConfig.numEntriesPerMap == uint32(numEntriesPerMap) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\trequirement for generation of fixed-size payload must have been registered"
			registeredRequirements := tp.observations.requirementRegistrations
			registeredForActor, ok := registeredRequirements[mapLoadRunnerName]
			if len(registeredRequirements) == 1 && ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tregistered requirement must be correct"
			if registeredForActor.UseFixedSize &&
				!registeredForActor.UseVariableSize &&
				registeredForActor.FixedSize.SizeBytes == fixedPayloadSizeBytes {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
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
			lowerBoundaryBytes := 6
			upperBoundaryBytes := 1200
			sameSizeSteps := 100

			numEntriesPerMap := 9
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                                                  true,
				"mapTests.load.numEntriesPerMap":                                         numEntriesPerMap,
				"mapTests.load.testLoop.type":                                            string(batch),
				"mapTests.load.payload.fixedSize.enabled":                                false,
				"mapTests.load.payload.variableSize.enabled":                             true,
				"mapTests.load.payload.variableSize.lowerBoundaryBytes":                  lowerBoundaryBytes,
				"mapTests.load.payload.variableSize.upperBoundaryBytes":                  upperBoundaryBytes,
				"mapTests.load.payload.variableSize.evaluateNewSizeAfterNumWriteActions": sameSizeSteps,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					requirementRegistrations: map[string]loadsupport.PayloadGenerationRequirement{},
				},
			}
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				name:            mapLoadRunnerName,
				providerFunctions: providerFuncs{
					mapStore: newTestMapStore,
					loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
						l.observations.numNewLooperInvocations++
						return l, nil
					},
					payloads: func() loadsupport.PayloadProvider {
						return tp
					},
				},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen(make(chan struct{}, 1))

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

			msg = "\t\trequirement for generation of variable-size payloads must have been registered"
			registeredRequirements := tp.observations.requirementRegistrations
			registeredForActor, ok := registeredRequirements[mapLoadRunnerName]
			if len(registeredRequirements) == 1 && ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tregistered requirement must be correct"
			if !registeredForActor.UseFixedSize &&
				registeredForActor.UseVariableSize &&
				registeredForActor.VariableSize.SameSizeStepsLimit == variablePayloadEvaluateNewSizeAfterNumWriteActions &&
				registeredForActor.VariableSize.LowerBoundaryBytes == variablePayloadSizeLowerBoundaryBytes &&
				registeredForActor.VariableSize.UpperBoundaryBytes == variablePayloadSizeUpperBoundaryBytes {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loop must have been initialized once"
			if l.observations.numInitLooperInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, l.observations.numInitLooperInvocations)
			}

			// The Load Runner does not make use of pre-initialized load elements regardless of
			// the configured payload mode, so make sure load elements slice is empty in case
			// of variable-size payloads, too
			msg = "\t\tnumber of generated load elements must be zero"
			if len(l.assignedTestLoopExecution.elements) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(l.assignedTestLoopExecution.elements))
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
			numEntriesPerMap := 9
			a := &testConfigPropertyAssigner{testConfig: map[string]any{
				"mapTests.load.enabled":                      true,
				"mapTests.load.numEntriesPerMap":             numEntriesPerMap,
				"mapTests.load.testLoop.type":                string(batch),
				"mapTests.load.payload.fixedSize.enabled":    false,
				"mapTests.load.payload.variableSize.enabled": false,
			}}
			ch := &testHzClientHandler{}
			l := newTestLoadTestLoop()
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					requirementRegistrations: map[string]loadsupport.PayloadGenerationRequirement{},
				},
			}
			r := loadRunner{
				assigner:        a,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				l:               l,
				providerFunctions: providerFuncs{
					mapStore: newTestMapStore,
					loadElementTestLoop: func(rc *runnerConfig) (looper[loadElement], error) {
						l.observations.numNewLooperInvocations++
						return l, nil
					},
					payloads: func() loadsupport.PayloadProvider {
						return tp
					},
				},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen(make(chan struct{}, 1))

			numEntriesPerMap = 9
			fixedPayloadSizeBytes = 3
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer)
			gatherer.StopListen()

			waitForStatusGatheringDone(gatherer)

			msg := "\t\ttest loop must not have been created"
			if l.observations.numNewLooperInvocations == 0 {
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
			if detail, ok := checkRunnerStateTransitions([]runnerState{start}, r.stateList); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tno payload generation requirement must have been registered"
			if len(tp.observations.requirementRegistrations) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestLoadRunner_getOrAssemblePayload(t *testing.T) {

	t.Log("given map name, map number, and a load element")
	{
		t.Log("\twhen payload retrieval is successful")
		{
			mapName := "awesome-map-name"
			mapNumber := uint16(0)
			actorName := fmt.Sprintf("%s-%s-%d", mapLoadRunnerName, mapName, mapNumber)
			pw := &loadsupport.PayloadWrapper{Payload: []byte("super-awesome-payload")}
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					payloadRetrievals: map[string]struct{}{},
				},
				behavior: &testPayloadProviderBehavior{
					returnErrorUponPayloadRetrieval: false,
					payloadsToReturn: map[string]*loadsupport.PayloadWrapper{
						actorName: pw,
					},
				},
			}
			r := loadRunner{
				payloadProvider: tp,
			}

			p, err := r.getOrAssemblePayload(mapName, mapNumber, "")

			msg := "\t\tpayload retrieval must have been invoked for correct actor"
			payloadRetrievals := tp.observations.payloadRetrievals
			_, ok := payloadRetrievals[actorName]
			if len(payloadRetrievals) == 1 && ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned payload must match the one given in test payload provider"
			if p == pw {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen payload retrieval is unsuccessful")
		{
			tp := &testPayloadProvider{
				observations: &testPayloadProviderObservations{
					payloadRetrievals: map[string]struct{}{},
				},
				behavior: &testPayloadProviderBehavior{
					returnErrorUponPayloadRetrieval: true,
				},
			}
			r := loadRunner{
				payloadProvider: tp,
			}

			mapName := "awesome-map-name"
			mapNumber := uint16(0)
			actorName := fmt.Sprintf("%s-%s-%d", mapLoadRunnerName, mapName, mapNumber)
			p, err := r.getOrAssemblePayload(mapName, mapNumber, "")

			msg := "\t\tpayload retrieval must have been invoked for correct actor"
			payloadRetrievals := tp.observations.payloadRetrievals
			_, ok := payloadRetrievals[actorName]
			if len(payloadRetrievals) == 1 && ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned payload must be nil"
			if p == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestAssertExactlyOnePayloadModeEnabled(t *testing.T) {

	t.Log("given two modes for generating payloads for the map load runner")
	{
		t.Log("\twhen both modes are enabled")
		{
			err := assertExactlyOnePayloadModeEnabled(true, true)

			msg := "\t\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen neither of the two modes is enabled")
		{
			err := assertExactlyOnePayloadModeEnabled(false, false)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen only fixed-size payloads are enabled")
		{
			err := assertExactlyOnePayloadModeEnabled(true, false)

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			}
		}
		t.Log("\twhen only variable-size payloads are enabled")
		{
			err := assertExactlyOnePayloadModeEnabled(false, true)

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
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
			keyPath := testMapRunnerKeyPath + ".numEntriesPerMap"
			expectedNumEntriesPerMap := tc[keyPath]
			if cfg.numEntriesPerMap == uint32(expectedNumEntriesPerMap.(int)) {
				t.Log(msg, checkMark, keyPath)
			} else {
				t.Fatal(msg, ballotX, keyPath)
			}

			keyPath = testMapRunnerKeyPath + ".payload.fixedSize.enabled"
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
		t.Log("\twhen lower and upper boundary for variable-size payloads have not been provided")
		{
			t.Log("\t\twhen variable-size payloads have been enabled")
			{
				t.Log("\t\t\twhen runner has been enabled")
				{
					tc := assembleTestConfigForTestLoopType(boundary)
					tc["testMapRunner.payload.variableSize.upperBoundaryBytes"] = 42
					tc["testMapRunner.payload.variableSize.lowerBoundaryBytes"] = 43

					a := &testConfigPropertyAssigner{testConfig: tc}

					cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tnil config must be returned"
					if cfg == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen runner has been disabled")
				{
					tc := assembleTestConfigForTestLoopType(boundary)
					tc["testMapRunner.enabled"] = false
					tc["testMapRunner.payload.variableSize.upperBoundaryBytes"] = 42
					tc["testMapRunner.payload.variableSize.lowerBoundaryBytes"] = 43

					a := &testConfigPropertyAssigner{testConfig: tc}

					cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tpopulated config must be returned"
					if cfg != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
			t.Log("\t\twhen variable-size payloads have not been enabled")
			{
				tc := assembleTestConfigForTestLoopType(boundary)
				tc["testMapRunner.payload.fixedSize.enabled"] = true
				tc["testMapRunner.payload.variableSize.enabled"] = false
				tc["testMapRunner.payload.variableSize.upperBoundaryBytes"] = 42
				tc["testMapRunner.payload.variableSize.lowerBoundaryBytes"] = 43

				a := &testConfigPropertyAssigner{testConfig: tc}

				cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpopulated config must be returned"
				if cfg != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
		t.Log("\twhen both fixed-size and variable-size payloads have been enabled")
		{
			t.Log("\t\twhen runner has been enabled")
			{
				tc := assembleTestConfigForTestLoopType(boundary)
				tc["testMapRunner.payload.fixedSize.enabled"] = true
				tc["testMapRunner.payload.variableSize.enabled"] = true

				a := &testConfigPropertyAssigner{testConfig: tc}

				cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tnil config must be returned"
				if cfg == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen runner has been disabled")
			{
				tc := assembleTestConfigForTestLoopType(boundary)
				tc["testMapRunner.enabled"] = false
				tc["testMapRunner.payload.fixedSize.enabled"] = true
				tc["testMapRunner.payload.variableSize.enabled"] = true

				a := &testConfigPropertyAssigner{testConfig: tc}

				cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpopulated config must be returned"
				if cfg != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}

		}
		t.Log("\twhen both fixed-size nor variable-size payloads have been disabled")
		{
			t.Log("\t\twhen runner has been enabled")
			{
				a := &testConfigPropertyAssigner{testConfig: map[string]any{
					"testMapRunner.enabled":                      true,
					"testMapRunner.payload.fixedSize.enabled":    false,
					"testMapRunner.payload.variableSize.enabled": false,
				}}

				cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

				msg := "\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\tnil config must be returned"
				if cfg == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen runner has been disabled")
			{

				a := &testConfigPropertyAssigner{testConfig: map[string]any{
					"testMapRunner.enabled":                      false,
					"testMapRunner.payload.fixedSize.enabled":    false,
					"testMapRunner.payload.variableSize.enabled": false,
				}}

				cfg, err := populateLoadConfig(testMapRunnerKeyPath, testMapBaseName, a)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpopulated config must be returned"
				if cfg != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestValidateVariablePayloadSizeBoundaries(t *testing.T) {

	t.Log("given a lower and an upper boundary representing the size in bytes of payloads to be generated")
	{
		t.Log("\twhen lower is less than upper")
		{
			msg := "\t\tno error must be returned"
			if err := validateVariablePayloadSizeBoundaries(10, 1000); err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen lower is equal to upper")
		{
			msg := "\t\terror must be returned"
			if err := validateVariablePayloadSizeBoundaries(10, 10); err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen lower is greater than upper")
		{
			msg := "\t\terror must be returned"
			if err := validateVariablePayloadSizeBoundaries(15000, 1000); err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}
