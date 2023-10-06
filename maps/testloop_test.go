package maps

import (
	"context"
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

func populateDummyHzMapStore(ms *dummyHzMapStore) {

	mapNumber := uint16(0)
	// Store all elements from dummy data source in map
	for _, value := range theFellowship {
		key := assembleMapKey(mapNumber, value)

		ms.m.data.Store(key, value)
	}

}

func TestPopulateLocalCache(t *testing.T) {

	t.Log("given a method to populate the boundary test loop's local key cache")
	{
		mapName := "ht_" + mapBaseName
		t.Log("\twhen retrieving the key set is successful")
		{
			ms := assembleDummyMapStore(false, false, false, false, false, false)
			populateDummyHzMapStore(&ms)

			keys, err := queryRemoteMapKeys(context.TODO(), ms.m, mapName, 0)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tresult must contain keys from map"
			if len(keys) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen retrieved key set is empty")
		{
			ms := assembleDummyMapStore(false, false, false, false, false, false)

			keys, err := queryRemoteMapKeys(context.TODO(), ms.m, mapName, 0)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\treturned map must be empty, too"
			if len(keys) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen key set retrieval fails")
		{
			ms := assembleDummyMapStore(false, false, false, false, false, true)

			keys, err := queryRemoteMapKeys(context.TODO(), ms.m, mapName, 0)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned map must be empty"
			if len(keys) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestExecuteMapAction(t *testing.T) {

	t.Log("given various actions to execute on maps")
	{
		t.Log("\twhen next action is insert")
		{
			t.Log("\t\twhen check for contains key yields error")
			{
				ms := assembleDummyMapStore(false, false, false, true, false, false)
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = insert

				actionExecuted, err := tl.executeMapAction(ms.m, "some-map-name", 0, theFellowship[0])

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen target map does not contain key yet")
			{
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = insert

				mapNumber := 0
				mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
				actionExecuted, err := tl.executeMapAction(ms.m, mapName, uint16(mapNumber), theFellowship[0])

				msg := "\t\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = fmt.Sprintf("\t\t\taction '%s' must be reported as executed", tl.nextAction)
				if actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tcontains key check must have been executed once"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tset operation must have been executed once"
				if ms.m.setInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.setInvocations))
				}

				msg = "\t\t\tmap must contain one element"
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

			t.Log("\t\twhen target map does not contain key yet and set yields error")
			{
				ms := assembleDummyMapStore(false, false, true, false, false, false)
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = insert

				actionExecuted, err := tl.executeMapAction(ms.m, "awesome-map-name", 0, theFellowship[0])

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen target map already contains key")
			{
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = insert

				mapNumber := uint16(0)
				populateDummyHzMapStore(&ms)
				mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)

				actionExecuted, err := tl.executeMapAction(ms.m, mapName, mapNumber, theFellowship[0])

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = fmt.Sprintf("\t\t\taction '%s' must be reported as not executed", tl.nextAction)
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno set invocation must have been attempted"
				if ms.m.setInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected no invocations, got %d", ms.m.setInvocations))
				}

			}
		}
		t.Log("\twhen next action is remove")
		{
			t.Log("\t\twhen target map does not contain key")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = remove

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tno remove must have been attempted"
				if ms.m.removeInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 remove invocations, got %d", ms.m.removeInvocations))
				}
			}

			t.Log("\t\twhen target map contains key and remove yields error")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, false, false, false, true, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = remove

				populateDummyHzMapStore(&ms)

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tone remove attempt must have been made"
				if ms.m.removeInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.removeInvocations))
				}

			}

			t.Log("\t\twhen target map contains key and remove does not yield error")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = remove

				populateDummyHzMapStore(&ms)

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as executed"
				if actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tone remove attempt must have been made"
				if ms.m.removeInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.removeInvocations))
				}

			}
		}
		t.Log("\twhen next action is read")
		{
			t.Log("\t\twhen target map does not contain key")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = read

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tno read must have been attempted"
				if ms.m.getInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.getInvocations))
				}
			}

			t.Log("\t\twhen target map contains key and get yields error")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, true, false, false, false, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = read

				populateDummyHzMapStore(&ms)

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as not executed"
				if !actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tone read must have been attempted"
				if ms.m.getInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.getInvocations))
				}

			}

			t.Log("\t\twhen target map contains key and get does not yield error")
			{
				rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
				ms := assembleDummyMapStore(false, false, false, false, false, false)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				tl.nextAction = read

				populateDummyHzMapStore(&ms)

				actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\taction must be reported as executed"
				if actionExecuted {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tone check for key must have been performed"
				if ms.m.containsKeyInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tone read must have been attempted"
				if ms.m.getInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.getInvocations))
				}

			}
		}
		t.Log("\twhen unknown action is provided")
		{
			rc := assembleRunnerConfigForBatchTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false, false, false, false, false)
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
			var unknownAction mapAction = "yeeeehaw"
			tl.nextAction = unknownAction

			actionExecuted, err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0])

			msg := "\t\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\t\taction must be reported as not executed"
			if !actionExecuted {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			// There are only three operations available, and it's highly unlikely anyone will ever invoke this
			// method with the next action set to an action other than these three. Therefore, it's fine not to verify
			// in the method if the next action is one of insert, remove, or delete before performing the contains key
			// check.
			msg = "\t\t\tone check for key must have been performed"
			if ms.m.containsKeyInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
			}

			msg = "\t\t\tno insert must have been executed"
			if ms.m.setInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.setInvocations))
			}

			msg = "\t\t\tno remove must have been executed"
			if ms.m.removeInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.removeInvocations))
			}

			msg = "\t\t\tno read must have been executed"
			if ms.m.getInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.getInvocations))
			}
		}
	}

}

func TestDetermineNextMapAction(t *testing.T) {

	t.Log("given a function to determine the next map action to be executed")
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

	t.Log("given a function that determines whether the map action mode should be changed")
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

		t.Log("\twhen map is empty and current mode is unset")
		{
			nextMode := checkForModeChange(1.0, 0.0, 1_000, 0, "")

			msg := "\t\tnext mode must be fill"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}
		}

	}

}

func TestRunWithBoundaryTestLoop(t *testing.T) {

	t.Log("given the boundary test loop")
	{
		t.Log("\twhen target map is empty")
		{
			t.Log("\t\twhen lower boundary is 0 %, upper boundary is 100 %")
			{
				t.Log("\t\t\twhen probability for action towards boundary is 100 %")
				{
					id := uuid.New()
					numMaps, numRuns := uint16(1), uint32(1)

					rc := assembleRunnerConfigForBoundaryTestLoop(
						numMaps,
						numRuns,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						1.0,
						1_000,
					)
					ms := assembleDummyMapStore(false, false, false, false, false, false)
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

					tl.run()
					waitForStatusGatheringDone(tl.g)

					msg := "\t\t\t\tall elements must be inserted in map"
					if ms.m.setInvocations == len(theFellowship) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

					msg = "\t\t\t\tnumber of invocations to query size of map must be equal to number of gets plus number of sets"
					if ms.m.sizeInvocations == ms.m.setInvocations+ms.m.getInvocations {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.sizeInvocations)
					}

					msg = "\t\t\t\tnumber of get invocations must be equal to number of set invocations minus one"
					if ms.m.getInvocations == ms.m.setInvocations-1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.getInvocations)
					}
				}

				t.Log("\t\t\twhen probability for action towards boundary is 0 %")
				{
					id := uuid.New()
					numMaps, numRuns := uint16(1), uint32(1)

					rc := assembleRunnerConfigForBoundaryTestLoop(
						numMaps,
						numRuns,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.0,
						1_000,
					)
					ms := assembleDummyMapStore(false, false, false, false, false, false)
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

					tl.run()
					waitForStatusGatheringDone(tl.g)

					msg := "\t\t\t\tnumber of insert invocations must be zero"
					if ms.m.setInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

					msg = "\t\t\t\tnumber of contains key invocations must be equal to number of items in data set"
					if ms.m.containsKeyInvocations == len(theFellowship) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.containsKeyInvocations)
					}

					// Contains key check prior to executing remove will correctly indicate
					// the target map does not contain the key in question, thus returning
					// before a remove can be attempted --> Number of remove invocations
					// must be zero
					msg = "\t\t\t\tnumber of remove invocations must be zero"
					if ms.m.removeInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.removeInvocations)
					}

					// The remove attempts will fail because the map does not contain any data,
					// hence the logic under test should never conclude the remove was successful
					// and thus determine a read to be carried out next
					msg = "\t\t\t\tnumber of read invocations must be zero"
					if ms.m.getInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.getInvocations)
					}

				}

				t.Log("\t\t\twhen probability for action towards boundary is 50 %")
				{
					id := uuid.New()
					numMaps, numRuns := uint16(1), uint32(1)

					rc := assembleRunnerConfigForBoundaryTestLoop(
						numMaps,
						numRuns,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.5,
						1_000,
					)
					ms := assembleDummyMapStore(false, false, false, false, false, false)
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

					tl.run()
					waitForStatusGatheringDone(tl.g)

					msg := "\t\t\t\tnumber of set invocations must be equal to the number of items in the source data set, divided by two"
					if ms.m.setInvocations == (len(theFellowship)+1)/2 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

				}
			}

		}
	}

}

func TestRunWithBatchTestLoop(t *testing.T) {

	t.Log("given the maps batch test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(false, false, false, false, false, false)
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, rc)

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
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, false, false, false, false, false)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(true, false, false, false, false, false)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(false, true, false, false, false, false)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
			ms := assembleDummyMapStore(false, false, false, false, false, false)
			numMaps, numRuns := uint16(0), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, rc)

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
			rc := assembleRunnerConfigForBatchTestLoop(1, 20, scBetweenRuns, scBetweenActionBatches)
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false, false, false, false, false), rc)

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
			rc := assembleRunnerConfigForBatchTestLoop(1, numRuns, scBetweenRuns, scBetweenActionsBatches)
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(false, false, false, false, false, false), rc)

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

func assembleTestLoopConfig(id uuid.UUID, source string, rc *runnerConfig, ms hzMapStore) testLoopExecution[string] {

	return testLoopExecution[string]{
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

func assembleDummyMapStore(returnErrorUponGetMap, returnErrorUponGet, returnErrorUponSet, returnErrorUponContainsKey, returnErrorUponRemove, returnErrorUponGetKeySet bool) dummyHzMapStore {

	dummyBackend := &sync.Map{}

	return dummyHzMapStore{
		m: &dummyHzMap{
			data:                       dummyBackend,
			returnErrorUponGet:         returnErrorUponGet,
			returnErrorUponSet:         returnErrorUponSet,
			returnErrorUponContainsKey: returnErrorUponContainsKey,
			returnErrorUponRemove:      returnErrorUponRemove,
			returnErrorUponGetKeySet:   returnErrorUponGetKeySet,
		},
		returnErrorUponGetMap: returnErrorUponGetMap,
	}

}

func assembleRunnerConfigForBoundaryTestLoop(
	numMaps uint16,
	numRuns uint32,
	sleepBetweenRuns *sleepConfig,
	sleepBetweenOperationBatches *sleepConfig,
	upperBoundaryMapFillPercentage, lowerBoundaryMapFillPercentage, actionTowardsBoundaryProbability float32,
	operationChainLength int,
) *runnerConfig {

	c := assembleBaseRunnerConfig(numMaps, numRuns, sleepBetweenRuns)
	c.boundary = &boundaryTestLoopConfig{
		sleepBetweenOperationChains: sleepBetweenOperationBatches,
		chainLength:                 operationChainLength,
		resetAfterChain:             false,
		upper: &boundaryDefinition{
			mapFillPercentage: upperBoundaryMapFillPercentage,
			enableRandomness:  false,
		},
		lower: &boundaryDefinition{
			mapFillPercentage: lowerBoundaryMapFillPercentage,
			enableRandomness:  false,
		},
		actionTowardsBoundaryProbability: actionTowardsBoundaryProbability,
	}

	return c

}

func assembleRunnerConfigForBatchTestLoop(
	numMaps uint16,
	numRuns uint32,
	sleepBetweenRuns *sleepConfig,
	sleepBetweenActionBatches *sleepConfig,
) *runnerConfig {

	c := assembleBaseRunnerConfig(numMaps, numRuns, sleepBetweenRuns)
	c.batch = &batchTestLoopConfig{sleepBetweenActionBatches}

	return c

}

func assembleBaseRunnerConfig(
	numMaps uint16,
	numRuns uint32,
	sleepBetweenRuns *sleepConfig,
) *runnerConfig {

	return &runnerConfig{
		enabled:                 true,
		numMaps:                 numMaps,
		numRuns:                 numRuns,
		mapBaseName:             "test",
		useMapPrefix:            true,
		mapPrefix:               "ht_",
		appendMapIndexToMapName: false,
		appendClientIdToMapName: false,
		sleepBetweenRuns:        sleepBetweenRuns,
		loopType:                boundary,
		batch:                   nil,
		boundary:                nil,
	}

}
