package maps

import (
	"github.com/google/uuid"
	"hazeltest/api"
	"sync"
	"testing"
)

var theFellowShip = []string{
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
			ms := assembleDummyMapStore(false)
			rc := assembleRunnerConfig(1)
			tl := assembleTestLoop(id, testSource, &rc, ms)

			tl.run()

			expectedNumSetInvocations := len(theFellowShip)
			expectedNumGetInvocations := len(theFellowShip)
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

			msg = "\t\ttest loop must have told api package about status"

			if _, ok := api.Loops[id]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			msg = "\t\tstatus at end of test run must be correct"

			testLoopStatus := api.Loops[id]

			if testLoopStatus.Source == testSource &&
				testLoopStatus.NumRuns == rc.numRuns &&
				testLoopStatus.NumMaps == rc.numMaps &&
				testLoopStatus.TotalRuns == rc.numRuns*uint32(rc.numMaps) &&
				testLoopStatus.TotalRunsFinished == rc.numRuns {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			numMaps := 10
			rc := assembleRunnerConfig(numMaps)
			ms := assembleDummyMapStore(false)
			tl := assembleTestLoop(uuid.New(), testSource, &rc, ms)

			tl.run()

			expectedNumSetInvocations := len(theFellowShip) * 10
			expectedNumGetInvocations := len(theFellowShip) * 10
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
		}

		t.Log("\twhen get map yields an error")
		{
			rc := assembleRunnerConfig(1)
			ms := assembleDummyMapStore(true)
			tl := assembleTestLoop(uuid.New(), testSource, &rc, ms)

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

		}

	}

}

func assembleTestLoop(id uuid.UUID, source string, rc *runnerConfig, ms hzMapStore) testLoop[string] {

	tlc := assembleTestLoopConfig(id, source, ms, rc)
	return testLoop[string]{
		config: &tlc,
	}

}

func assembleTestLoopConfig(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) testLoopConfig[string] {

	return testLoopConfig[string]{
		id:                     id,
		source:                 source,
		mapStore:               ms,
		runnerConfig:           rc,
		elements:               theFellowShip,
		ctx:                    nil,
		getElementIdFunc:       fellowshipMemberName,
		deserializeElementFunc: deserializeFellowshipMember,
	}

}

func assembleDummyMapStore(returnErrorUponGetMap bool) dummyHzMapStore {

	dummyBackend := &sync.Map{}

	return dummyHzMapStore{
		m:                     &dummyHzMap{data: dummyBackend},
		returnErrorUponGetMap: returnErrorUponGetMap,
	}

}

func assembleRunnerConfig(numMaps int) runnerConfig {

	return runnerConfig{
		enabled:                 true,
		numMaps:                 numMaps,
		numRuns:                 1,
		mapBaseName:             "test",
		useMapPrefix:            true,
		mapPrefix:               "ht_",
		appendMapIndexToMapName: false,
		appendClientIdToMapName: false,
		sleepBetweenActionBatches: &sleepConfig{
			enabled:    false,
			durationMs: 0,
		},
		sleepBetweenRuns: &sleepConfig{
			enabled:    false,
			durationMs: 0,
		},
	}

}
