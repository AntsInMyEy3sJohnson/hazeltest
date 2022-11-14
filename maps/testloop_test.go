package maps

import (
	"fmt"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

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
	sleepDurationMs     = 10
	sleepConfigDisabled = &sleepConfig{
		enabled:          false,
		durationMs:       0,
		enableRandomness: false,
	}
	sleepConfigEnabled = &sleepConfig{
		enabled:          true,
		durationMs:       sleepDurationMs,
		enableRandomness: false,
	}
	sleepConfigEnabledWithEnabledRandomness = &sleepConfig{
		enabled:          true,
		durationMs:       sleepDurationMs,
		enableRandomness: true,
	}
)

func fellowshipMemberName(element interface{}) string {

	return element.(string)

}

func deserializeFellowshipMember(_ interface{}) error {

	return nil

}

func TestRun(t *testing.T) {

	testSource := "theFellowship"

	t.Log("given the need to test running the maps test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			tl.run()

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

			expectedRuns := numMaps * numRuns
			if ok, key, detail := statusContainsExpectedValues(tl.sg.getStatus(), numMaps, numRuns, expectedRuns, expectedRuns); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			numMaps, numRuns := 10, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()
			// Give status gatherer time to process all elements that might still be in channel
			time.Sleep(500 * time.Millisecond)

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

			expectedRuns := numMaps * numRuns
			if ok, key, detail := statusContainsExpectedValues(tl.sg.getStatus(), numMaps, numRuns, expectedRuns, expectedRuns); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen get map yields error")
		{
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled)
			ms := assembleDummyMapStore(true, false)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()

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

			if ok, key, detail := statusContainsExpectedValues(tl.sg.getStatus(), numMaps, numRuns, numMaps*numRuns, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen only one run is executed an error is thrown during read all")
		{
			numMaps, numRuns := 1, 1
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, true)
			tl := assembleTestLoop(uuid.New(), testSource, ms, &rc)

			tl.run()

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
			if ok, key, detail := statusContainsExpectedValues(tl.sg.getStatus(), numMaps, numRuns, expectedRuns, expectedRuns); ok {
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
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			tl.run()

			msg := "\t\tinitial status must contain correct values anyway"

			if ok, key, detail := statusContainsExpectedValues(tl.sg.getStatus(), numMaps, numRuns, 0, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen an enabled sleep config is provided for sleep between runs")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numRuns := 20
			rc := assembleRunnerConfig(1, numRuns, sleepConfigEnabled)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			start := time.Now()
			tl.run()
			elapsedMs := time.Since(start).Milliseconds()

			msg := "\t\ttest run execution time must be at least the number of runs into the milliseconds slept after each run"
			if elapsedMs >= int64(numRuns*sleepDurationMs) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen an enabled sleep config with enabled randomness is provided for sleep between runs")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numRuns := 20
			rc := assembleRunnerConfig(1, numRuns, sleepConfigEnabledWithEnabledRandomness)
			tl := assembleTestLoop(id, testSource, ms, &rc)

			start := time.Now()
			tl.run()
			elapsedMs := time.Since(start).Milliseconds()

			msg := "\t\ttest run execution time must be less than the number of runs into the given number of " +
				"milliseconds to sleep due to the random factor reducing the actual time slept"
			if elapsedMs < int64(numRuns*sleepDurationMs) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

	}

}

func statusContainsExpectedValues(status *sync.Map, expectedNumMaps, expectedNumRuns, expectedTotalRuns, expectedTotalRunsFinished int) (bool, string, string) {

	if numMapsFromStatus, ok := status.Load(statusKeyNumMaps); !ok || numMapsFromStatus != expectedNumMaps {
		return false, statusKeyNumMaps, fmt.Sprintf("want: %d; got: %d", expectedNumMaps, numMapsFromStatus)
	}

	if numRunsFromStatus, ok := status.Load(statusKeyNumRuns); !ok || numRunsFromStatus != uint32(expectedNumRuns) {
		return false, statusKeyNumRuns, fmt.Sprintf("want: %d; got: %d", expectedNumRuns, numRunsFromStatus)
	}

	if totalRunsFromStatus, ok := status.Load(statusKeyTotalRuns); !ok || totalRunsFromStatus != uint32(expectedTotalRuns) {
		return false, statusKeyTotalRuns, fmt.Sprintf("want: %d; got: %d", expectedTotalRuns, totalRunsFromStatus)
	}

	if totalRunsFinishedFromStatus, ok := status.Load(statusKeyTotalRunsFinished); !ok || totalRunsFinishedFromStatus != uint32(expectedTotalRunsFinished) {
		return false, statusKeyTotalRunsFinished, fmt.Sprintf("want: %d; got: %d", expectedTotalRunsFinished, totalRunsFinishedFromStatus)
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
	sg := assembleStatusGatherer()
	tl.init(&tlc, &sg)

	return tl

}

func assembleStatusGatherer() statusGatherer {

	return statusGatherer{
		status:   sync.Map{},
		elements: make(chan statusElement),
	}

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

func assembleRunnerConfig(numMaps, numRuns int, sleepBetweenRuns *sleepConfig) runnerConfig {

	return runnerConfig{
		enabled:                   true,
		numMaps:                   numMaps,
		numRuns:                   uint32(numRuns),
		mapBaseName:               "test",
		useMapPrefix:              true,
		mapPrefix:                 "ht_",
		appendMapIndexToMapName:   false,
		appendClientIdToMapName:   false,
		sleepBetweenActionBatches: sleepConfigDisabled,
		sleepBetweenRuns:          sleepBetweenRuns,
	}

}
