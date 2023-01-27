package maps

import (
	"fmt"
	"github.com/google/uuid"
	"hazeltest/status"
	"sync"
	"testing"
)

const testSource = "theFellowship"

var (
	theFellowship = []string{
		"Aragorn",
		"Gandalf",
		"Legolas",
		"Boromir",
		"Sam",
		"Frodo",
		"Merry",
		"Pippin",
		"Gimli",
	}
	sleepConfigDisabled = &sleepConfig{
		enabled:          false,
		durationMs:       0,
		enableRandomness: false,
	}
)

const statusKeyFinished = "finished"

func fellowshipMemberName(element any) string {

	return element.(string)

}

func deserializeFellowshipMember(_ any) error {

	return nil

}

func TestRun(t *testing.T) {

	t.Log("given the need to test running the maps test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)

			expectedNumSetInvocations := len(theFellowship)
			expectedNumGetInvocations := len(theFellowship)
			expectedNumDestroyInvocations := 1

			msg := "\t\texpected predictable invocations on map must have been executed"

			if expectedNumSetInvocations == ms.m.setInvocations &&
				expectedNumGetInvocations == ms.m.getInvocations &&
				expectedNumDestroyInvocations == ms.m.destroyInvocations {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\texpected invocations based on random element in test loop must have been executed"

			expectedContainsKeyInvocations := expectedNumSetInvocations + ms.m.removeInvocations
			if expectedContainsKeyInvocations == ms.m.containsKeyInvocations {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tvalues in test loop status must be correct"

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, numMaps*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			numMaps, numRuns := 10, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)

			expectedNumSetInvocations := len(theFellowship) * 10
			expectedNumGetInvocations := len(theFellowship) * 10
			expectedNumDestroyInvocations := 10

			msg := "\t\texpected predictable invocations on map must have been executed"

			if expectedNumSetInvocations == ms.m.setInvocations &&
				expectedNumGetInvocations == ms.m.getInvocations &&
				expectedNumDestroyInvocations == ms.m.destroyInvocations {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\texpected invocations based on random element in test loop must have been executed"

			expectedContainsKeyInvocations := expectedNumSetInvocations + ms.m.removeInvocations
			if expectedContainsKeyInvocations == ms.m.containsKeyInvocations {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tvalues in test loop status must be correct"

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, numMaps*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen get map yields error")
		{
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(true, false)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)

			msg := "\t\tno invocations on map must have been attempted"

			if ms.m.containsKeyInvocations == 0 &&
				ms.m.setInvocations == 0 &&
				ms.m.getInvocations == 0 &&
				ms.m.removeInvocations == 0 &&
				ms.m.destroyInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tvalues in test loop status must be correct"

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, numMaps*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen only one run is executed an error is thrown during read all")
		{
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, true)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)

			msg := "\t\tno remove invocations must have been attempted"

			if ms.m.removeInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdata must remain in map since no remove was executed"

			if numElementsInSyncMap(ms.m.data) == len(theFellowship) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tvalues in test loop status must be correct"

			expectedRuns := numMaps * numRuns
			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, expectedRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen no map goroutine is launched because the configured number of maps is zero")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numMaps, numRuns := 0, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)

			msg := "\t\tinitial status must contain correct values anyway"

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}
		t.Log("\twhen sleep configs for sleep between runs and sleep between action batches are disabled")
		{
			scBetweenRuns := &sleepConfig{}
			scBetweenActionBatches := &sleepConfig{}
			rc := assembleRunnerConfig(1, 20, scBetweenRuns, scBetweenActionBatches)
			tl := assembleTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false), &rc)

			numInvocationsBetweenRuns := 0
			numInvocationsBetweenActionBatches := 0
			sleepTimeFunc = func(sc *sleepConfig) int {
				if sc == scBetweenRuns {
					numInvocationsBetweenRuns++
				} else {
					numInvocationsBetweenActionBatches++
				}
				return 0
			}

			tl.run()

			msg := "\t\tnumber of sleeps between runs must zero"
			if numInvocationsBetweenRuns == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of sleeps between action batches must be zero"
			if numInvocationsBetweenActionBatches == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen sleep configs for sleep between runs and sleep between action batches are enabled")
		{
			numRuns := 20
			scBetweenRuns := &sleepConfig{enabled: true}
			scBetweenActionsBatches := &sleepConfig{enabled: true}
			rc := assembleRunnerConfig(1, numRuns, scBetweenRuns, scBetweenActionsBatches)
			tl := assembleTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false), &rc)

			numInvocationsBetweenRuns := 0
			numInvocationsBetweenActionBatches := 0
			sleepTimeFunc = func(sc *sleepConfig) int {
				if sc == scBetweenRuns {
					numInvocationsBetweenRuns++
				} else {
					numInvocationsBetweenActionBatches++
				}
				return 0
			}

			tl.run()

			msg := "\t\tnumber of sleeps between runs must be equal to number of runs"
			if numInvocationsBetweenRuns == numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of sleeps between action batches must be equal to two times the number of runs"
			if numInvocationsBetweenActionBatches == 2*numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func waitForStatusGatheringDone(g *status.Gatherer) {

	for {
		if done := g.ListeningStopped(); done {
			return
		}
	}

}

func statusContainsExpectedValues(status map[string]any, expectedNumMaps, expectedNumRuns, expectedTotalRuns int, expectedRunnerFinished bool) (bool, string, string) {

	if numMapsFromStatus, ok := status[statusKeyNumMaps]; !ok || numMapsFromStatus != expectedNumMaps {
		return false, statusKeyNumMaps, fmt.Sprintf("want: %d; got: %d", expectedNumMaps, numMapsFromStatus)
	}

	if numRunsFromStatus, ok := status[statusKeyNumRuns]; !ok || numRunsFromStatus != uint32(expectedNumRuns) {
		return false, statusKeyNumRuns, fmt.Sprintf("want: %d; got: %d", expectedNumRuns, numRunsFromStatus)
	}

	if totalRunsFromStatus, ok := status[statusKeyTotalNumRuns]; !ok || totalRunsFromStatus != uint32(expectedTotalRuns) {
		return false, statusKeyTotalNumRuns, fmt.Sprintf("want: %d; got: %d", expectedTotalRuns, totalRunsFromStatus)
	}

	if runnerFinishedFromStatus, ok := status[statusKeyFinished]; !ok || runnerFinishedFromStatus != expectedRunnerFinished {
		return false, statusKeyFinished, fmt.Sprintf("want: %t; got: %t", expectedRunnerFinished, runnerFinishedFromStatus)
	}

	return true, "", ""

}

func numElementsInSyncMap(data *sync.Map) int {

	i := 0
	data.Range(func(key, value any) bool {
		i++
		return true
	})

	return i

}

func assembleTestLoop(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) testLoop[string] {

	tlc := assembleTestLoopConfig(id, source, rc, ms)
	tl := testLoop[string]{}
	tl.init(&tlc, &defaultSleeper{}, status.NewGatherer())

	return tl

}

func assembleTestLoopConfig(id uuid.UUID, source string, rc *runnerConfig, ms hzMapStore) testLoopConfig[string] {

	return testLoopConfig[string]{
		id:                     id,
		source:                 source,
		mapStore:               ms,
		runnerConfig:           rc,
		elements:               theFellowship,
		ctx:                    nil,
		getElementIdFunc:       fellowshipMemberName,
		deserializeElementFunc: deserializeFellowshipMember,
	}

}

func assembleDummyMapStore(returnErrorUponGetMap, returnErrorUponGet bool) dummyHzMapStore {

	dummyBackend := &sync.Map{}

	return dummyHzMapStore{
		m:                     &dummyHzMap{data: dummyBackend, returnErrorUponGet: returnErrorUponGet},
		returnErrorUponGetMap: returnErrorUponGetMap,
	}

}

func assembleRunnerConfig(numMaps, numRuns int, sleepBetweenRuns *sleepConfig, sleepBetweenActionBatches *sleepConfig) runnerConfig {

	return runnerConfig{
		enabled:                   true,
		numMaps:                   numMaps,
		numRuns:                   uint32(numRuns),
		mapBaseName:               "test",
		useMapPrefix:              true,
		mapPrefix:                 "ht_",
		appendMapIndexToMapName:   false,
		appendClientIdToMapName:   false,
		sleepBetweenActionBatches: sleepBetweenActionBatches,
		sleepBetweenRuns:          sleepBetweenRuns,
	}

}
