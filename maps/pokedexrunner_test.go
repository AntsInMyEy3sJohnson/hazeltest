package maps

import (
	"context"
	"encoding/json"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"strconv"
	"testing"
)

type testPokedexTestLoop struct{}

func (d testPokedexTestLoop) init(_ *testLoopExecution[pokemon], _ sleeper, _ status.Gatherer) {
	// No-op
}

func (d testPokedexTestLoop) run() {
	// No-op
}

func TestReturnPokemonPayload(t *testing.T) {

	t.Log("given a function retrieve the string pointer to an element that might be a pokemon")
	{
		t.Log("\twhen given element is not a pokemon")
		{
			digimon := struct {
				id                            int
				digimonsProbablyAlsoHaveAName string
				andPerhapsAColor              string
				andAnAge                      int
				andMaybeAWeight               int
			}{
				42,
				"Dave-imon",
				"orange",
				153,
				21,
			}

			sp, err := returnPokemonPayload("", 0, strconv.Itoa(digimon.id))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnil pointer must be returned"
			if sp == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen pokedex entries map contains given pokemon id")
		{
			t.Log("\t\twhen pokemon id points to non-nil pokemon entry")
			{
				pokemonElement := pokemon{
					ID:          1,
					Num:         "001",
					Name:        "Bulbasaur",
					Img:         "bulbasaur.png",
					ElementType: []string{"Grass", "Poison"},
					Height:      "0.71 m",
					Weight:      "6.9 kg",
					Candy:       "Bulbasaur Candy",
					CandyCount:  25,
					EggDistance: "2 km",
					SpawnChance: 0.69,
					AvgSpawns:   69.0,
					SpawnTime:   "20:00",
					Multipliers: []float32{1.58},
					Weaknesses:  []string{"Fire", "Ice", "Flying", "Psychic"},
					NextEvolution: []nextEvolution{
						{Num: "002", Name: "Ivysaur"},
						{Num: "003", Name: "Venusaur"},
					},
				}
				pokemonEntries[strconv.Itoa(pokemonElement.ID)] = &pokemonElement

				pw, err := returnPokemonPayload("", 0, strconv.Itoa(pokemonElement.ID))

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				unmarshalledPokemonElement := &pokemon{}
				err = json.Unmarshal(pw.Payload, unmarshalledPokemonElement)

				msg = "\t\t\tno error must be returned upon attempt to unmarshal pokemon"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpointer to string representation of pokemon must be returned"
				if unmarshalledPokemonElement.ID == pokemonElement.ID {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}
			t.Log("\t\twhen pokemon id points to nil entry")
			{
				pokemonID := "42"

				pokemonEntries[pokemonID] = nil

				sp, err := returnPokemonPayload("", uint16(9), pokemonID)

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tnil pointer must be returned"
				if sp == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestInitializePokemonTestLoop(t *testing.T) {

	t.Log("given a function to initialize the test loop from the provided loop type")
	{
		t.Log("\twhen boundary test loop type is provided")
		{
			l, err := initPokedexTestLoop(&runnerConfig{loopType: boundary})

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
			l, err := initPokedexTestLoop(&runnerConfig{loopType: batch})

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
			l, err := initPokedexTestLoop(&runnerConfig{loopType: "saruman"})

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
			r := pokedexRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
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
			r := pokedexRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
			}

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
		t.Log("\twhen test loop has executed")
		{
			assigner := testConfigPropertyAssigner{
				returnError: false,
				testConfig: map[string]any{
					"mapTests.pokedex.enabled":       true,
					"mapTests.pokedex.testLoop.type": "batch",
				},
			}
			ch := &testHzClientHandler{}
			ms := &testHzMapStore{observations: &testHzMapStoreObservations{}}
			r := pokedexRunner{
				assigner:        assigner,
				stateList:       []runnerState{},
				hzClientHandler: ch,
				providerFuncs: struct {
					mapStore        newMapStoreFunc
					pokemonTestLoop newPokemonTestLoopFunc
				}{mapStore: func(_ hazelcastwrapper.HzClientHandler) hazelcastwrapper.MapStore {
					ms.observations.numInitInvocations++
					return ms
				}, pokemonTestLoop: func(rc *runnerConfig) (looper[pokemon], error) {
					return &testPokedexTestLoop{}, nil
				}},
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

			msg = "\t\tmap store must have been initialized once"
			if ms.observations.numInitInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ms.observations.numInitInvocations)
			}
		}
	}

}
