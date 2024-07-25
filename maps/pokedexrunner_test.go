package maps

import (
	"context"
	"hazeltest/status"
	"testing"
)

type testPokedexTestLoop struct{}

func (d testPokedexTestLoop) init(_ *testLoopExecution[pokemon], _ sleeper, _ *status.Gatherer) {
	// No-op
}

func (d testPokedexTestLoop) run() {
	// No-op
}

func TestInitializePokemonTestLoop(t *testing.T) {

	t.Log("given a function to initialize the test loop from the provided loop type")
	{
		t.Log("\twhen boundary test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: boundary})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*boundaryTestLoop[pokemon]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen batch test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: batch})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tlooper must have expected type"
			if _, ok := l.(*batchTestLoop[pokemon]); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen unknown test loop type is provided")
		{
			l, err := initializePokemonTestLoop(&runnerConfig{loopType: "saruman"})

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

func TestRunPokedexMapTests(t *testing.T) {

	t.Log("given the pokedex runner to run map tests")
	{
		t.Log("\twhen runner configuration cannot be populated")
		genericMsgStateTransitions := "\t\tstate transitions must be correct"
		genericMsgLatestStateInGatherer := "\t\tlatest state in gatherer must be correct"
		{
			assigner := testConfigPropertyAssigner{
				returnError: true,
				testConfig:  nil,
			}
			ch := &testHzClientHandler{}
			r := pokedexRunner{assigner: assigner, stateList: []runnerState{}, hzClientHandler: ch, hzMapStore: testHzMapStore{}, l: testPokedexTestLoop{}}

			gatherer := status.NewGatherer()

			go gatherer.Listen()
			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer, initTestMapStore)
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

			msg := "\t\tgatherer instance must have been assigned"
			if gatherer == r.gatherer {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\thazelcast client handler must not have initialized hazelcast client"
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
		t.Log("\twhen runner has been disabled")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.pokedex.enabled": false,
				},
			}
			ch := &testHzClientHandler{}
			r := pokedexRunner{assigner: assigner, stateList: []runnerState{}, hzClientHandler: ch, hzMapStore: testHzMapStore{}, l: testPokedexTestLoop{}}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer, initTestMapStore)
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
		t.Log("\twhen hazelcast map store has been initialized and test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.pokedex.enabled":       true,
					"mapTests.pokedex.testLoop.type": "batch",
				},
			}
			ch := &testHzClientHandler{}
			r := pokedexRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				hzMapStore:      testHzMapStore{},
				l:               testPokedexTestLoop{},
			}

			gatherer := status.NewGatherer()
			go gatherer.Listen()

			r.runMapTests(context.TODO(), hzCluster, hzMembers, gatherer, initTestMapStore)
			gatherer.StopListen()

			if msg, ok := checkRunnerStateTransitions(expectedStatesForFullRun, r.stateList); ok {
				t.Log(genericMsgStateTransitions, checkMark)
			} else {
				t.Fatal(genericMsgStateTransitions, ballotX, msg)
			}

			waitForStatusGatheringDone(gatherer)
			latestState := r.stateList[len(r.stateList)-1]

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

			msg = "\t\thazelcast client handler must have performed shutdown on hazelcast client once"
			if ch.shutdownInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ch.shutdownInvocations)
			}
		}
	}

}
