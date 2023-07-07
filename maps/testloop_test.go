package maps

import (
	"fmt"
	"github.com/google/uuid"
	"hazeltest/status"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
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

func TestExecuteMapAction(t *testing.T) {

	t.Log("given the need to test executing a map action")
	{
		t.Log("\twhen next action is insert")
		{
			ms := assembleDummyMapStore(false, false)
			rc := assembleRunnerConfig(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, &rc)
			tl.nextAction = insert

			mapNumber := 0
			mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
			err := tl.executeMapAction(ms.m, mapName, uint16(mapNumber))

			msg := "\t\tno error must be returned"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tcontains key check must have been executed once"
			if ms.m.containsKeyInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
			}

			msg = "\t\tset operation must have been executed once"
			if ms.m.setInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.setInvocations))
			}

			msg = "\t\tmap must contain one element"
			count := 0
			ms.m.data.Range(func(_, _ any) bool {
				count++
				return true
			})
			if count == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 element, got %d", count))
			}

		}
	}

}

func TestDetermineNextMapAction(t *testing.T) {

	t.Log("given the need to test determining the next map action")
	{
		t.Log("\twhen last action was insert or remove")
		{
			nextMapAction := determineNextMapAction(fill, insert, 0.5)

			msg := "\t\taction after insert must be read"
			if nextMapAction == read {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			nextMapAction = determineNextMapAction(fill, remove, 0.5)

			msg = "\t\taction after remove must be read"

			if nextMapAction == read {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen last action was read and current mode is fill")
		{
			numInvocations := 10_000
			actionProbability := 0.75
			insertCount, removeCount, otherCount := generateMapActionResults(fill, numInvocations, actionProbability)

			msg := "\t\tonly insert and remove are valid next map actions"
			if otherCount == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d invocations resulted in map action that was neither insert nor remove", otherCount))
			}

			actionHitsCorrect, nonActionHitsCorrect := checkMapActionResults(fill, numInvocations, insertCount, removeCount, actionProbability)

			msg = fmt.Sprintf("\t\twith action probability of %d%%, must have roughly %2.f inserts for %d invocations",
				int(actionProbability*100), float64(numInvocations)*actionProbability, numInvocations)
			if actionHitsCorrect {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, insertCount)
			}

			msg = fmt.Sprintf("\t\tremaining %d%% must correspond to roughly %2.f deletes for %d invocations",
				int((1-actionProbability)*100), float64(numInvocations)*(1-actionProbability), numInvocations)
			if nonActionHitsCorrect {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, removeCount)
			}
		}

		t.Log("\twhen last action was read and current mode is drain")
		{
			numInvocations := 10_000
			actionProbability := 0.60
			insertCount, removeCount, otherCount := generateMapActionResults(drain, numInvocations, actionProbability)

			msg := "\t\tonly insert and remove are valid next map actions"

			if otherCount == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%d invocations resulted in map action that was neither insert nor remove", otherCount))
			}

			actionHitsCorrect, nonActionHitsCorrect := checkMapActionResults(drain, numInvocations, insertCount, removeCount, actionProbability)

			msg = fmt.Sprintf("\t\twith action probability of %d%%, must have roughly %2.f removes for %d invocations",
				int(actionProbability*100), float64(numInvocations)*actionProbability, numInvocations)
			if actionHitsCorrect {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, insertCount)
			}

			msg = fmt.Sprintf("\t\tremaining %d%% must correspond to roughly %2.f inserts for %d invocations",
				int((1-actionProbability)*100), float64(numInvocations)*(1-actionProbability), numInvocations)
			if nonActionHitsCorrect {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, removeCount)
			}

		}

		t.Log("\twhen unknown mode is provided")
		{
			var unknownMode actionMode = "awesomeActionMode"
			lastAction := read
			nextAction := determineNextMapAction(unknownMode, lastAction, 0.0)

			msg := "\t\tnext action must be equal to last action"
			if nextAction == lastAction {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%s', got '%s'", lastAction, nextAction))
			}
		}
	}
}

func checkMapActionResults(currentMode actionMode, numInvocations, insertCount, removeCount int, actionProbability float64) (bool, bool) {

	numInvocationsAsFloat := float64(numInvocations)

	expectedNumberOfInserts := 0
	expectedNumberOfDeletes := 0
	if currentMode == fill {
		expectedNumberOfInserts = int(numInvocationsAsFloat * actionProbability)
		expectedNumberOfDeletes = int(numInvocationsAsFloat * (1 - actionProbability))
	} else {
		expectedNumberOfDeletes = int(numInvocationsAsFloat * actionProbability)
		expectedNumberOfInserts = int(numInvocationsAsFloat * (1 - actionProbability))
	}

	tolerance := 0.05
	actionHitsCorrect := math.Abs(float64(expectedNumberOfInserts)-float64(insertCount)) < float64(numInvocations)*tolerance
	nonActionHitsCorrect := math.Abs(float64(expectedNumberOfDeletes)-float64(removeCount)) < float64(numInvocations)*tolerance

	return actionHitsCorrect, nonActionHitsCorrect

}

func generateMapActionResults(currentMode actionMode, numInvocations int, actionProbability float64) (int, int, int) {

	// Same seed as in main function
	rand.Seed(time.Now().UnixNano())

	insertCount := 0
	removeCount := 0
	otherCount := 0
	for i := 0; i < numInvocations; i++ {
		action := determineNextMapAction(currentMode, read, float32(actionProbability))
		if action == insert {
			insertCount++
		} else if action == remove {
			removeCount++
		} else {
			otherCount++
		}
	}

	return insertCount, removeCount, otherCount

}

func TestCheckForModeChange(t *testing.T) {

	t.Log("given the need to test checking for a mode change")
	{
		t.Log("\twhen the currently stored number of elements is less than the lower boundary")
		{
			nextMode := checkForModeChange(0.8, 0.2, 100, 19, drain)

			msg := "\t\tmode check must yield fill as next mode"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

		t.Log("\twhen the currently stored number of elements is equal to lower boundary")
		{
			currentMode := drain
			nextMode := checkForModeChange(0.8, 0.2, 100, 20, currentMode)

			msg := "\t\tmode check must return current mode"

			if nextMode == currentMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

		t.Log("\twhen the currently stored number of elements is in between the lower and the upper boundary")
		{
			currentMode := drain
			nextMode := checkForModeChange(0.8, 0.2, 100, 50, currentMode)

			msg := "\t\tmode check must return current mode"

			if nextMode == currentMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

		t.Log("\twhen the currently stored number of elements is equal to the upper boundary")
		{
			currentMode := fill
			nextMode := checkForModeChange(0.8, 0.2, 100, 80, currentMode)

			msg := "\t\tmode check must return current mode"

			if nextMode == currentMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

		t.Log("\twhen the currently stored number of elements is greater than the upper boundary")
		{
			nextMode := checkForModeChange(0.8, 0.2, 100, 81, fill)

			msg := "\t\tmode check must return drain as next mode"

			if nextMode == drain {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

	}

}

func TestRunWithBoundaryTestLoop(t *testing.T) {

	t.Log("given the need to test running the boundary test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			numMaps, numRuns := uint16(1), uint32(1)

			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false)
			tl := assembleBoundaryTestLoop(id, testSource, ms, &rc)

			tl.run()
			waitForStatusGatheringDone(tl.g)
		}
	}

}

func TestRunWithBatchTestLoop(t *testing.T) {

	t.Log("given the need to test running the maps batch test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false)
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, &rc)

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

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			numMaps, numRuns := uint16(10), uint32(1)
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, &rc)

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

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen get map yields error")
		{
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(true, false)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, &rc)

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

			if ok, key, detail := statusContainsExpectedValues(tl.g.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}

		t.Log("\twhen only one run is executed an error is thrown during read all")
		{
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, true)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, &rc)

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

			expectedRuns := uint32(numMaps) * numRuns
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
			numMaps, numRuns := uint16(0), uint32(1)
			rc := assembleRunnerConfig(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, &rc)

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
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false), &rc)

			numInvocationsBetweenRuns := 0
			numInvocationsBetweenActionBatches := 0
			sleepTimeFunc = func(sc *sleepConfig) int {
				if sc == scBetweenRuns {
					numInvocationsBetweenRuns++
				} else if sc == scBetweenActionBatches {
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
			numRuns := uint32(20)
			scBetweenRuns := &sleepConfig{enabled: true}
			scBetweenActionsBatches := &sleepConfig{enabled: true}
			rc := assembleRunnerConfig(1, numRuns, scBetweenRuns, scBetweenActionsBatches)
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false), &rc)

			numInvocationsBetweenRuns := uint32(0)
			numInvocationsBetweenActionBatches := uint32(0)
			sleepTimeFunc = func(sc *sleepConfig) int {
				if sc == scBetweenRuns {
					numInvocationsBetweenRuns++
				} else if sc == scBetweenActionsBatches {
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

func statusContainsExpectedValues(status map[string]any, expectedNumMaps uint16, expectedNumRuns uint32, expectedTotalRuns uint32, expectedRunnerFinished bool) (bool, string, string) {

	if numMapsFromStatus, ok := status[statusKeyNumMaps]; !ok || numMapsFromStatus != expectedNumMaps {
		return false, statusKeyNumMaps, fmt.Sprintf("want: %d; got: %d", expectedNumMaps, numMapsFromStatus)
	}

	if numRunsFromStatus, ok := status[statusKeyNumRuns]; !ok || numRunsFromStatus != expectedNumRuns {
		return false, statusKeyNumRuns, fmt.Sprintf("want: %d; got: %d", expectedNumRuns, numRunsFromStatus)
	}

	if totalRunsFromStatus, ok := status[statusKeyTotalNumRuns]; !ok || totalRunsFromStatus != expectedTotalRuns {
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

func assembleBoundaryTestLoop(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) boundaryTestLoop[string] {

	tlc := assembleTestLoopConfig(id, source, rc, ms)
	tl := boundaryTestLoop[string]{}
	tl.init(&tlc, &defaultSleeper{}, status.NewGatherer())

	return tl
}

func assembleBatchTestLoop(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) batchTestLoop[string] {

	tlc := assembleTestLoopConfig(id, source, rc, ms)
	tl := batchTestLoop[string]{}
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

func assembleRunnerConfig(numMaps uint16, numRuns uint32, sleepBetweenRuns *sleepConfig, sleepBetweenActionBatches *sleepConfig) runnerConfig {

	return runnerConfig{
		enabled:                   true,
		numMaps:                   numMaps,
		numRuns:                   numRuns,
		mapBaseName:               "test",
		useMapPrefix:              true,
		mapPrefix:                 "ht_",
		appendMapIndexToMapName:   false,
		appendClientIdToMapName:   false,
		sleepBetweenActionBatches: sleepBetweenActionBatches,
		sleepBetweenRuns:          sleepBetweenRuns,
	}

}
