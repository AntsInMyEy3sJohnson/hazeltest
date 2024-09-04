package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/state"
	"hazeltest/status"
	"math"
	"strings"
	"sync"
	"testing"
)

type (
	testMapStoreBehavior struct {
		returnErrorUponGetMap, returnErrorUponGet, returnErrorUponSet, returnErrorUponContainsKey, returnErrorUponRemove, returnErrorUponRemoveAll, returnErrorUponEvictAll bool
	}
	runnerProperties struct {
		numMaps               uint16
		numRuns               uint32
		cleanMapsPriorToRun   bool
		mapCleanErrorBehavior state.ErrorDuringCleanBehavior
		sleepBetweenRuns      *sleepConfig
	}
	testSingleMapCleanerBuilder struct {
		mapCleanerToReturn state.SingleCleaner
	}
	testSingleMapCleanerBehavior struct {
		numElementsCleanedReturnValue int
		returnErrorUponClean          bool
	}
	testSingleMapCleanerObservations struct {
		cleanInvocations int
	}
	testSingleMapCleaner struct {
		behavior     *testSingleMapCleanerBehavior
		observations *testSingleMapCleanerObservations
	}
	getOrAssemblePayloadFunctionBehavior struct {
		returnError bool
	}
	getOrAssemblePayloadFunctionObservations struct {
		numInvocations int
	}
	testSleeper struct {
		sleepInvoked bool
	}
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
	rpOneMapOneRunNoEvictionScDisabled = &runnerProperties{
		numMaps:             1,
		numRuns:             1,
		cleanMapsPriorToRun: false,
		sleepBetweenRuns:    sleepConfigDisabled,
	}
	getOrAssembleBehavior     = getOrAssemblePayloadFunctionBehavior{}
	getOrAssembleObservations = getOrAssemblePayloadFunctionObservations{}
	getOrAssemblePayloadError = errors.New("the error everyone told you not to worry about")
)

func (b *testSingleMapCleanerBuilder) Build(_ context.Context, _ hazelcastwrapper.MapStore, _ state.CleanedTracker, _ state.LastCleanedInfoHandler) (state.SingleCleaner, string) {

	return b.mapCleanerToReturn, "hz:impl:mapService"

}

func (c *testSingleMapCleaner) Clean(_ string) (int, error) {

	c.observations.cleanInvocations++

	if c.behavior.returnErrorUponClean {
		return 0, fmt.Errorf("something somewhere went terribly wrong")
	}

	return c.behavior.numElementsCleanedReturnValue, nil

}

func TestMapTestLoopCountersTrackerInit(t *testing.T) {

	t.Log("given the tracker's init function")
	{
		t.Log("\twhen init method is invoked")
		{
			ct := &mapTestLoopCountersTracker{}
			g := status.NewGatherer()

			go g.Listen()
			ct.init(g)
			g.StopListen()

			msg := "\t\tgatherer must have been assigned"
			if ct.gatherer == g {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tall status keys must have been inserted into status record"
			initialCounterValue := 0
			for _, v := range counters {
				if counter, ok := ct.counters[v]; ok && counter == initialCounterValue {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}

			waitForStatusGatheringDone(g)
			msg = "\t\tgatherer must have received all status keys with initial values"
			statusCopy := g.AssembleStatusCopy()

			for _, v := range counters {
				if ok, detail := expectedStatusPresent(statusCopy, v, initialCounterValue); ok {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}
		}
	}

}

func TestMapTestLoopCountersTrackerIncreaseCounter(t *testing.T) {

	t.Log("given a method for increasing the value of a specific counter")
	{
		t.Log("\twhen method is not invoked concurrently")
		{
			ct := &mapTestLoopCountersTracker{
				counters: make(map[statusKey]int),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			for _, v := range counters {
				t.Log(fmt.Sprintf("\t\twhen initial value is zero for counter '%s' and increase method is invoked", v))
				{
					ct.counters[v] = 0
					ct.increaseCounter(v)

					msg := "\t\t\tcounter increase must be reflected in counter tracker's state"
					if ct.counters[v] == 1 {
						t.Log(msg, checkMark, v)
					} else {
						t.Fatal(msg, ballotX, v)
					}

					msg = "\t\t\tcorresponding update must have been sent to status gatherer"
					update := <-ct.gatherer.Updates
					if update.Key == string(v) && update.Value == 1 {
						t.Log(msg, checkMark, v)
					} else {
						t.Fatal(msg, ballotX, v)
					}
				}
			}
		}
		t.Log("\twhen multiple goroutines want to increase a counter")
		{
			wg := sync.WaitGroup{}
			ct := &mapTestLoopCountersTracker{
				counters: make(map[statusKey]int),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			go ct.gatherer.Listen()
			ct.counters[statusKeyNumFailedInserts] = 0
			numInvokingGoroutines := 100
			for i := 0; i < numInvokingGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ct.increaseCounter(statusKeyNumFailedInserts)
				}()
			}
			wg.Wait()
			ct.gatherer.StopListen()

			msg := "\t\tfinal counter value must be equal to number of invoking goroutines"

			if ct.counters[statusKeyNumFailedInserts] == numInvokingGoroutines {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestChooseRandomElementFromSourceData(t *testing.T) {

	t.Log("given a populated source data as part of the test loop execution's runnerState")
	{
		t.Log("\twhen caller desires random element from source data")
		{
			tle := testLoopExecution[pokemon]{
				elements: []pokemon{
					{Name: "Charmander"},
				},
			}
			tl := boundaryTestLoop[pokemon]{
				tle: &tle,
			}
			selectedElement := tl.chooseRandomElementFromSourceData()

			msg := "\t\telement from source data must be selected"
			if selectedElement.Name == tle.elements[0].Name {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestChooseNextMapElement(t *testing.T) {

	t.Log("given a set of possible map actions and a cache mirroring the current state of the corresponding map in hazelcast")
	{
		t.Log("\twhen map action is insert")
		{
			t.Log("\t\twhen source data contains elements, but not all of them have been stored in the cache yet")
			{
				tl := boundaryTestLoop[string]{
					tle: &testLoopExecution[string]{
						elements: theFellowship,
						getElementID: func(element any) string {
							return element.(string)
						},
					},
				}

				cache := make(map[string]struct{}, len(theFellowship)-1)
				mapNumber := uint16(0)
				for i := 0; i < len(theFellowship)-1; i++ {
					key := assembleMapKey(mapNumber, theFellowship[i])
					cache[key] = struct{}{}
				}
				elementNotYetInCache := theFellowship[len(theFellowship)-1]
				selectedElement, err := tl.chooseNextMapElement(insert, cache, 0)

				msg := "\t\t\tselected element must be only element not yet stored in cache"
				if selectedElement == elementNotYetInCache {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, elementNotYetInCache))
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}
			}

			t.Log("\t\twhen all elements of source data have been stored in cache")
			{
				tl := boundaryTestLoop[string]{
					tle: &testLoopExecution[string]{
						elements: []string{
							theFellowship[0],
						},
						getElementID: func(element any) string {
							return element.(string)
						},
					},
				}

				// Simulate "all elements" by passing in a single element both in cache and in source data
				// --> Has same effect in loop, but easier this way to test whether random selection was
				// invoked because with only one element in source data, random selection must yield
				// precisely this element
				mapNumber := uint16(0)
				cache := map[string]struct{}{
					assembleMapKey(mapNumber, theFellowship[0]): {},
				}
				selectedElement, err := tl.chooseNextMapElement(insert, cache, mapNumber)

				msg := "\t\t\tselected element must be equal to only element in source data"
				if selectedElement == theFellowship[0] {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, theFellowship[0]))
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}
			}
		}

		for _, action := range []mapAction{read, remove} {
			t.Log(fmt.Sprintf("\twhen map action is %s", action))
			{
				t.Log("\t\twhen cache is empty")
				{
					elementInSourceData := theFellowship[0]
					tl := boundaryTestLoop[string]{
						tle: &testLoopExecution[string]{
							elements: []string{
								elementInSourceData,
							},
						},
					}

					selectedElement, err := tl.chooseNextMapElement(action, map[string]struct{}{}, 0)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					// Can't simply return nil as t
					msg = "\t\t\treturned element must be first element from source data"
					if selectedElement == elementInSourceData {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, elementInSourceData))
					}
				}

				t.Log("\t\twhen cache contains at least one element")
				{
					elementInSourceData := theFellowship[0]
					tl := boundaryTestLoop[string]{
						tle: &testLoopExecution[string]{
							elements: []string{
								elementInSourceData,
							},
							getElementID: func(element any) string {
								return element.(string)
							},
						},
					}

					mapNumber := uint16(0)
					cache := map[string]struct{}{
						assembleMapKey(mapNumber, elementInSourceData): {},
					}

					selectedElement, err := tl.chooseNextMapElement(action, cache, mapNumber)

					msg := "\t\t\tselected element must be equal to element stored in cache"
					if selectedElement == elementInSourceData {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, elementInSourceData))
					}

					msg = "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

				}

				t.Log("\t\twhen mismatch between source data and state in cache has occurred")
				{
					tl := boundaryTestLoop[string]{
						tle: &testLoopExecution[string]{
							elements: theFellowship,
							getElementID: func(element any) string {
								return "So you have chosen... death."
							},
						},
					}

					cache := map[string]struct{}{
						"You shall not pass!": {},
					}
					selectedElement, err := tl.chooseNextMapElement(action, cache, 0)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tselected element must be equal to first element in source data"
					elementFromSourceData := theFellowship[0]
					if selectedElement == elementFromSourceData {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, elementFromSourceData))
					}
				}
			}

		}

		t.Log("\twhen unknown map action is provided")
		{
			tl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}

			selectedElement, err := tl.chooseNextMapElement("awesomeNonExistingMapAction", map[string]struct{}{}, 0)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tselected element must be first element of source data"
			if selectedElement == theFellowship[0] {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", selectedElement, theFellowship[0]))
			}
		}
	}

}

func TestAssemblePredicate(t *testing.T) {

	t.Log("given a function to assemble a predicate for filtering keys in a hazelcast map")
	{
		t.Log("\twhen client id and map number are provided")
		{
			clientID := uuid.New()
			mapNumber := uint16(0)
			predicate := assemblePredicate(clientID, mapNumber)

			msg := "\t\tpredicate must be returned"
			if predicate != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			predicateString := predicate.String()
			msg = "\t\tpredicate must be sql predicate"
			if strings.HasPrefix(predicateString, "SQL") {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			actualFilter := strings.ReplaceAll(predicateString, "SQL", "")
			expectedFilter := fmt.Sprintf("(__key like %s-%d%%)", clientID, mapNumber)
			msg = "\t\tpredicate must contain expected filter"
			if actualFilter == expectedFilter {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%s', got '%s'", expectedFilter, actualFilter))
			}

		}
	}

}

func TestChooseRandomKeyFromCache(t *testing.T) {

	t.Log("given a cache of keys values have to be selected from")
	{
		t.Log("\twhen empty cache is provided")
		{
			selectedKey, err := chooseRandomKeyFromCache(map[string]struct{}{})

			msg := "\t\tselected key must be empty string"
			if selectedKey == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, msg)
			}

			msg = "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}

		t.Log("\twhen cache contains element")
		{
			key := "gandalf"
			cache := map[string]struct{}{
				key: {},
			}

			selectedKey, err := chooseRandomKeyFromCache(cache)
			msg := "\t\tkey must be selected"

			if selectedKey == key {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, selectedKey)
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}
		}
	}

}

func TestRunWrapper(t *testing.T) {

	t.Log("given a wrapper function to kick off both boundary and batch test loops")
	{
		t.Log("\twhen at least one map is provided")
		{
			numMaps := uint16(1)
			numRuns := uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             numMaps,
					numRuns:             numRuns,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
			go tl.gatherer.Listen()
			runWrapper(tl.tle, tl.gatherer, func(config *runnerConfig, u uint16) string {
				return "banana"
			}, func(h hazelcastwrapper.Map, s string, u uint16) {
				// No-op
			})
			tl.gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			sc := tl.gatherer.AssembleStatusCopy()

			msg := "\t\tstatus gatherer must contain initial runner state: %s"
			if sc[string(statusKeyNumMaps)].(uint16) == numMaps {
				t.Log(fmt.Sprintf(msg, statusKeyNumMaps), checkMark)
			} else {
				t.Fatal(fmt.Sprintf(msg, statusKeyNumMaps), ballotX)
			}

			if sc[string(statusKeyNumRuns)].(uint32) == numRuns {
				t.Log(fmt.Sprintf(msg, statusKeyNumRuns), checkMark)
			} else {
				t.Fatal(fmt.Sprintf(msg, statusKeyNumRuns), ballotX)
			}

			if sc[string(statusKeyTotalNumRuns)].(uint32) == uint32(numMaps)*numRuns {
				t.Log(fmt.Sprintf(msg, statusKeyTotalNumRuns), checkMark)
			} else {
				t.Fatal(fmt.Sprintf(msg, statusKeyTotalNumRuns), ballotX)
			}

			msg = "\t\tstatus gatherer must contain initial test loop state: %s"
			for _, v := range []statusKey{statusKeyNumFailedInserts, statusKeyNumFailedReads, statusKeyNumFailedRemoves, statusKeyNumNilReads} {
				if ok, detail := expectedStatusPresent(sc, v, 0); ok {
					t.Log(fmt.Sprintf(msg, v), checkMark)
				} else {
					t.Fatal(fmt.Sprintf(msg, v), ballotX, detail)
				}
			}
		}

		t.Log("\twhen clean target map prior to execution is disabled")
		{
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             1,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			cleaner := &testSingleMapCleaner{
				observations: &testSingleMapCleanerObservations{},
			}
			tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

			go tl.gatherer.Listen()

			runFuncCalled := false
			runWrapper(tl.tle, tl.gatherer, func(config *runnerConfig, u uint16) string {
				return "yeeehaw"
			}, func(h hazelcastwrapper.Map, s string, u uint16) {
				runFuncCalled = true
			})

			tl.gatherer.StopListen()
			waitForStatusGatheringDone(tl.gatherer)

			msg := "\t\tthere must have been no invocations on single map cleaner"
			if cleaner.observations.cleanInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, cleaner.observations.cleanInvocations)
			}

			msg = "\t\ttest loop's run function must have been invoked"
			if runFuncCalled {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen clean target map prior to execution is enabled")
		{
			t.Log("\t\twhen clean is successful and at least one element was cleaned from target map")
			{
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             1,
						cleanMapsPriorToRun: true,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				cleaner := &testSingleMapCleaner{
					behavior: &testSingleMapCleanerBehavior{
						numElementsCleanedReturnValue: 1,
					},
					observations: &testSingleMapCleanerObservations{},
				}
				tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

				populateTestHzMapStore(&ms)

				go tl.gatherer.Listen()
				runFuncCalled := false
				runWrapper(tl.tle, tl.gatherer, func(config *runnerConfig, u uint16) string {
					return "banana"
				}, func(h hazelcastwrapper.Map, s string, u uint16) {
					runFuncCalled = true
				})
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

				msg := "\t\t\tsingle cleaner must have been invoked once"
				if cleaner.observations.cleanInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cleaner.observations.cleanInvocations)
				}

				msg = "\t\t\ttest loop's run function must have been invoked once"
				if runFuncCalled {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}

			t.Log("\t\twhen evict all is unsuccessful and configured error behavior advises to ignore the error")
			{
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:               1,
						numRuns:               1,
						cleanMapsPriorToRun:   true,
						mapCleanErrorBehavior: state.Ignore,
						sleepBetweenRuns:      sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				cleaner := &testSingleMapCleaner{
					behavior: &testSingleMapCleanerBehavior{
						returnErrorUponClean: true,
					},
					observations: &testSingleMapCleanerObservations{},
				}
				tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

				runFuncCalled := false
				runWrapper(tl.tle, tl.gatherer, func(config *runnerConfig, u uint16) string {
					return "blubbi"
				}, func(_ hazelcastwrapper.Map, _ string, _ uint16) {
					runFuncCalled = true
				})

				msg := "\t\t\tsingle cleaner must have been invoked once"
				if cleaner.observations.cleanInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, cleaner.observations.cleanInvocations)
				}

				msg = "\t\t\ttest loop's run function must have been invoked despite error during pre-run clean"
				if runFuncCalled {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}

			t.Log("\t\twhen evict all is unsuccessful and configured error behavior advises to fail")
			{
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:               1,
						numRuns:               1,
						cleanMapsPriorToRun:   true,
						mapCleanErrorBehavior: state.Fail,
						sleepBetweenRuns:      sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				cleaner := &testSingleMapCleaner{
					behavior:     &testSingleMapCleanerBehavior{returnErrorUponClean: true},
					observations: &testSingleMapCleanerObservations{},
				}
				tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

				populateTestHzMapStore(&ms)

				go tl.gatherer.Listen()
				runFuncCalled := false
				runWrapper(tl.tle, tl.gatherer, func(config *runnerConfig, u uint16) string {
					return "banana"
				}, func(h hazelcastwrapper.Map, s string, u uint16) {
					runFuncCalled = true
				})
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

				msg := "\t\t\tsingle map cleaner must have been invoked once"
				if cleaner.observations.cleanInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.m.evictAllInvocations)
				}

				msg = "\t\t\trun func must not have been invoked"
				if !runFuncCalled {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestExecuteMapAction(t *testing.T) {

	t.Log("given various actions to execute on maps")
	{
		t.Log("\twhen next action is insert")
		{
			t.Log("\t\twhen target map does not contain key yet")
			{
				func() {
					defer resetGetOrAssemblePayloadTestSetup()

					ms := assembleTestMapStore(&testMapStoreBehavior{})
					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := 0
					mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
					go tl.gatherer.Listen()
					err := tl.executeMapAction(ms.m, mapName, uint16(mapNumber), theFellowship[0], action)
					tl.gatherer.StopListen()

					msg := "\t\t\tno error must be returned"

					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tno contains key check must have been executed"
					if ms.m.containsKeyInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.containsKeyInvocations))
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

					waitForStatusGatheringDone(tl.gatherer)

					msg = "\t\t\tstatus gatherer must indicate zero failed insert operations"
					if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedInserts, 0); ok {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, detail)
					}

					msg = "\t\t\tget or assemble payload function must have been invoked once"
					if getOrAssembleObservations.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}()
			}
			t.Log("\t\twhen target map does not contain key yet and set yields error")
			{
				func() {
					defer resetGetOrAssemblePayloadTestSetup()

					ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponSet: true})
					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

					go tl.gatherer.Listen()
					err := tl.executeMapAction(ms.m, "awesome-map-name", 0, theFellowship[0], insert)
					tl.gatherer.StopListen()

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					waitForStatusGatheringDone(tl.gatherer)

					msg = "\t\t\ttest loop must have informed status gatherer about error"
					statusCopy := tl.gatherer.AssembleStatusCopy()
					if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedInserts, 1); ok {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, detail)
					}

					msg = "\t\t\tget or assemble payload function must have been invoked nonetheless"
					if getOrAssembleObservations.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}()

			}
			t.Log("\t\twhen target map already contains key")
			{
				func() {
					defer resetGetOrAssemblePayloadTestSetup()

					ms := assembleTestMapStore(&testMapStoreBehavior{})
					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := uint16(0)
					populateTestHzMapStore(&ms)
					mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)

					err := tl.executeMapAction(ms.m, mapName, mapNumber, theFellowship[0], action)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tset invocation must have been attempted anyway"
					if ms.m.setInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("expected no invocations, got %d", ms.m.setInvocations))
					}

					msg = "\t\t\tget or assemble payload function must have been invoked anyway"
					if getOrAssembleObservations.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}()

			}
			t.Log("\t\twhen get or assemble payload function yields error")
			{
				func() {
					defer resetGetOrAssemblePayloadTestSetup()

					getOrAssembleBehavior = getOrAssemblePayloadFunctionBehavior{returnError: true}

					ms := assembleTestMapStore(&testMapStoreBehavior{})
					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := 0
					mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
					go tl.gatherer.Listen()
					err := tl.executeMapAction(ms.m, mapName, uint16(mapNumber), theFellowship[0], action)
					tl.gatherer.StopListen()

					msg := "\t\t\terror must be returned"
					if errors.Is(err, getOrAssemblePayloadError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tno set on map must have been attempted"
					if ms.m.setInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

					msg = "\t\t\tget or assemble payload function must have been invoked only once"
					if getOrAssembleObservations.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, getOrAssembleObservations.numInvocations)
					}
				}()
			}
		}
		t.Log("\twhen next action is remove")
		{
			t.Log("\t\twhen target map does not contain key")
			{
				func() {
					defer resetGetOrAssemblePayloadTestSetup()

					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

					go tl.gatherer.Listen()
					err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], remove)
					tl.gatherer.StopListen()

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tno check for key must have been performed"
					if ms.m.containsKeyInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
					}

					msg = "\t\t\tremove must have been attempted anyway"
					if ms.m.removeInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 remove invocations, got %d", ms.m.removeInvocations))
					}

					waitForStatusGatheringDone(tl.gatherer)

					msg = "\t\t\tstatus gatherer must indicate zero failed remove attempts"
					if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedRemoves, 0); ok {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, detail)
					}

					msg = "\t\t\tget or assemble payload function must not have been invoked"
					if getOrAssembleObservations.numInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}()
			}

			t.Log("\t\twhen target map contains key and remove yields error")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					1.0,
					0.0,
					0.5,
					42,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponRemove: true})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(&ms)

				go tl.gatherer.Listen()
				err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], remove)
				tl.gatherer.StopListen()

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno check for key must have been performed"
				if ms.m.containsKeyInvocations == 0 {
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

				waitForStatusGatheringDone(tl.gatherer)

				statusCopy := tl.gatherer.AssembleStatusCopy()
				msg = "\t\t\ttest loop must have informed status gatherer about error"

				if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedRemoves, 1); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}

			}

			t.Log("\t\twhen target map contains key and remove does not yield error")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					1.0,
					0.0,
					0.5,
					42,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(&ms)

				err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], remove)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno check for key must have been performed"
				if ms.m.containsKeyInvocations == 0 {
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
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					1.0,
					0.0,
					0.5,
					42,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], read)
				tl.gatherer.StopListen()

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno check for key must have been performed"
				if ms.m.containsKeyInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.containsKeyInvocations))
				}

				msg = "\t\t\tread must have been attempted anyway"
				if ms.m.getInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 invocations, got %d", ms.m.getInvocations))
				}

				waitForStatusGatheringDone(tl.gatherer)

				msg = "\t\t\tstatus record must inform about one nil read"
				statusCopy := tl.gatherer.AssembleStatusCopy()
				if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumNilReads, 1); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}

			t.Log("\t\twhen target map contains key and get yields error")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					1.0,
					0.0,
					0.5,
					42,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponGet: true})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(&ms)

				go tl.gatherer.Listen()
				err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], read)
				tl.gatherer.StopListen()

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno check for key must have been performed"
				if ms.m.containsKeyInvocations == 0 {
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

				waitForStatusGatheringDone(tl.gatherer)

				msg = "\t\t\ttest loop must have informed status gatherer about error"
				statusCopy := tl.gatherer.AssembleStatusCopy()
				if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedReads, 1); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}

			}

			t.Log("\t\twhen target map contains key and get does not yield error")
			{
				func() {

					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						0.5,
						42,
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

					populateTestHzMapStore(&ms)

					err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], read)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tno check for key must have been performed"
					if ms.m.containsKeyInvocations == 0 {
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

					msg = "\t\t\tget or assemble payload function must not have been invoked"
					if getOrAssembleObservations.numInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}()

			}
		}
		t.Log("\twhen unknown action is provided")
		{
			rc := assembleRunnerConfigForBoundaryTestLoop(
				rpOneMapOneRunNoEvictionScDisabled,
				sleepConfigDisabled,
				sleepConfigDisabled,
				1.0,
				0.0,
				0.5,
				42,
				true,
			)
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
			var unknownAction mapAction = "yeeeehaw"

			err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], unknownAction)

			msg := "\t\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\t\tno check for key must have been performed"
			if ms.m.containsKeyInvocations == 0 {
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
		t.Log("\twhen cache is empty")
		{
			nextMapAction := determineNextMapAction(&modeCache{current: fill}, insert, 0.5, 0)

			msg := "\t\tnext action must be insert"
			if nextMapAction == insert {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMapAction)
			}
		}
		t.Log("\twhen last action was insert or remove")
		{
			nextMapAction := determineNextMapAction(&modeCache{current: fill}, insert, 0.5, 1)

			msg := "\t\taction after insert must be read"
			if nextMapAction == read {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			nextMapAction = determineNextMapAction(&modeCache{current: fill}, remove, 0.5, 1)

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
			nextAction := determineNextMapAction(&modeCache{current: unknownMode}, lastAction, 0.0, 1)

			msg := "\t\tnext action must be equal to last action"
			if nextAction == lastAction {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%s', got '%s'", lastAction, nextAction))
			}
		}

		t.Log("\twhen cache is empty and probability for action towards boundary is zero")
		{
			nextAction := determineNextMapAction(&modeCache{current: ""}, "", 0, 0)

			msg := "\t\tnext action must be special no-op action"
			if nextAction == noop {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextAction)
			}

		}
	}
}

func TestCheckForModeChange(t *testing.T) {

	t.Log("given a function that determines whether the map action mode should be changed")
	{
		t.Log("\twhen currently stored number of elements is less than lower boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			lowerBoundary := float32(0.5)
			currentCacheSize := uint32(lowerBoundary*float32(len(theFellowship)) - 1)
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, lowerBoundary, currentCacheSize, drain)

			msg := "\t\tmode check must yield fill as next mode"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards mode must be enforced"
			if forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen currently stored number of elements matches lower boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := drain
			lowerBoundary := float32(0.3)
			currentCacheSize := uint32(math.Round(float64(len(theFellowship)) * float64(lowerBoundary)))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, lowerBoundary, currentCacheSize, currentMode)

			msg := "\t\tmode check must switch mode"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards mode must be enforced"
			if forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen the currently stored number of elements is in between the lower and the upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := drain
			upperBoundary := float32(0.8)
			currentCacheSize := uint32(float32(len(theFellowship))*upperBoundary) - 1
			nextMode, forceActionTowardsMode := btl.checkForModeChange(upperBoundary, 0.2, currentCacheSize, currentMode)

			msg := "\t\tmode check must return current mode"

			if nextMode == currentMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards node does not need to be enforced"
			if !forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen the currently stored number of elements is equal to the upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := fill
			upperBoundary := float32(0.8)
			currentCacheSize := uint32(math.Round(float64(len(theFellowship)) * float64(upperBoundary)))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(upperBoundary, 0.2, currentCacheSize, currentMode)

			msg := "\t\tmode check must switch mode"

			if nextMode == drain {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards mode must be enforced"
			if forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen the currently stored number of elements is greater than the upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, 0.2, uint32(len(theFellowship)), fill)

			msg := "\t\tmode check must return drain as next mode"

			if nextMode == drain {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards mode must be enforced"
			if forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen the currently stored number of elements is zero and the current mode is unset")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			nextMode, forceActionTowardsMode := btl.checkForModeChange(1.0, 0.0, 0, "")

			msg := "\t\tnext mode must be fill"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards node does not need to be enforced"
			if !forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen currently stored number of elements is greater than zero and current mode is unset")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			nextMode, forceActionTowardsMode := btl.checkForModeChange(1.0, 0.0, 500, "")

			msg := "\t\tnext mode must be fill"

			if nextMode == fill {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, nextMode)
			}

			msg = "\t\taction towards node does not need to be enforced"
			if !forceActionTowardsMode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
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
						&runnerProperties{
							numMaps:             numMaps,
							numRuns:             numRuns,
							cleanMapsPriorToRun: false,
							sleepBetweenRuns:    sleepConfigDisabled,
						},
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						1.0,
						// Set operation chain length to length of source data for this set of tests
						len(theFellowship),
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, &testHzClientHandler{}, ms, rc)

					tl.run()

					msg := "\t\t\t\tall elements must be inserted in map"
					if ms.m.setInvocations == len(theFellowship) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
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
						&runnerProperties{
							numMaps:             numMaps,
							numRuns:             numRuns,
							cleanMapsPriorToRun: false,
							sleepBetweenRuns:    sleepConfigDisabled,
						},
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.0,
						len(theFellowship),
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, &testHzClientHandler{}, ms, rc)

					tl.run()

					msg := "\t\t\t\tnumber of insert invocations must be zero"
					if ms.m.setInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
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

					chainLength := 10 * len(theFellowship)
					rc := assembleRunnerConfigForBoundaryTestLoop(
						&runnerProperties{
							numMaps:             numMaps,
							numRuns:             numRuns,
							cleanMapsPriorToRun: false,
							sleepBetweenRuns:    sleepConfigDisabled,
						},
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.5,
						chainLength,
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, &testHzClientHandler{}, ms, rc)

					tl.run()

					msg := "\t\t\t\tnumber of set invocations must be roughly equal to half the chain length"
					if math.Abs(float64(ms.m.setInvocations-chainLength/2)) < 5 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

				}

				t.Log("\t\t\twhen reset after chain has not been activated")
				{
					rc := assembleRunnerConfigForBoundaryTestLoop(
						rpOneMapOneRunNoEvictionScDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.5,
						len(theFellowship)-1,
						false,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

					tl.run()

					msg := "\t\t\t\tremove all must not have been invoked"
					if ms.m.removeAllInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, "expected zero invocations, got %d", ms.m.removeAllInvocations)
					}

				}
			}

		}
	}

}

func TestResetAfterOperationChain(t *testing.T) {

	t.Log("given a method to reset both local and remote runnerState after execution of an operation chain has finished")
	{
		t.Log("\twhen runnerState exists both locally and remotely")
		{
			t.Log("\t\twhen remove all on remote map does not yield error")
			{
				mapNumber := uint16(0)
				tl := boundaryTestLoop[string]{
					tle: &testLoopExecution[string]{
						ctx: context.TODO(),
					},
				}

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				populateTestHzMapStore(&ms)

				mc := &modeCache{current: fill, forceActionTowardsMode: true}
				ac := &actionCache{last: insert, next: read}
				keysCache := map[string]struct{}{}
				ms.m.data.Range(func(key, _ any) bool {
					keysCache[key.(string)] = struct{}{}
					return true
				})
				tl.resetAfterOperationChain(ms.m, "", mapNumber, &keysCache, mc, ac)

				msg := "\t\t\tafter reset, local mode cache for given map number must be cleared"
				if mc.current == "" && !mc.forceActionTowardsMode {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tlocal action cache for given map number must be cleared"
				if ac.last == "" && ac.next == "" {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tremove all must have been invoked once"
				if ms.m.removeAllInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 1 invocation, got %d", ms.m.removeAllInvocations))
				}

				msg = "\t\t\tremove all must have been invoked with correct predicate"
				expectedPredicateFilter := fmt.Sprintf("%s-%d", client.ID(), mapNumber)
				if ms.m.lastPredicateFilterForRemoveAllInvocation == expectedPredicateFilter {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected '%s', got '%s'", expectedPredicateFilter, ms.m.lastPredicateFilterForRemoveAllInvocation))
				}

				msg = "\t\t\tlocal keys cache must have been cleared"
				if len(keysCache) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("contains %d element/-s", len(keysCache)))
				}

			}

			t.Log("\t\twhen remove all on map yields error")
			{
				mapNumber := uint16(0)
				tl := boundaryTestLoop[string]{
					tle: &testLoopExecution[string]{
						ctx: context.TODO(),
					},
				}

				ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponRemoveAll: true})
				populateTestHzMapStore(&ms)

				keysCache := map[string]struct{}{}
				ms.m.data.Range(func(key, _ any) bool {
					keysCache[key.(string)] = struct{}{}
					return true
				})
				mc := &modeCache{current: fill}
				ac := &actionCache{last: insert, next: read}
				tl.resetAfterOperationChain(ms.m, "", mapNumber, &keysCache, mc, ac)

				msg := "\t\t\tlocal mode cache for given map number must be cleared anyway"
				if mc.current == "" {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tlocal action cache for given map number must be cleared anyway"
				if ac.last == "" && ac.next == "" {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tkeys cache must have remained unmodified"
				if len(keysCache) == len(theFellowship) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d element/-s, got %d", len(theFellowship), len(keysCache)))
				}
			}
		}
	}

}

func TestEvaluateMapFillBoundaries(t *testing.T) {

	t.Log("given a function to evaluate the upper and lower map fill boundaries from a boundary test loop config")
	{
		t.Log("\twhen randomness was enabled for both upper and lower boundary")
		{
			configuredUpper := float32(0.7)
			configuredLower := float32(0.5)
			bc := &boundaryTestLoopConfig{
				upper: &boundaryDefinition{
					mapFillPercentage: configuredUpper,
					enableRandomness:  true,
				},
				lower: &boundaryDefinition{
					mapFillPercentage: configuredLower,
					enableRandomness:  true,
				},
			}

			msgUpper := "\t\tevaluated upper must be less than one and greater than or equal to configured upper"
			msgLower := "\t\tevaluated lower must be less than configured lower and greater than or equal to zero"

			upperOk := true
			lowerOk := true
			for i := 0; i < 100; i++ {
				evaluatedUpper, evaluatedLower := evaluateMapFillBoundaries(bc)
				if evaluatedUpper == 1 || evaluatedUpper < configuredUpper {
					upperOk = false
				}

				if evaluatedLower >= configuredLower || evaluatedLower < 0 {
					lowerOk = false
				}
			}

			if upperOk {
				t.Log(msgUpper, checkMark)
			} else {
				t.Fatal(msgUpper, ballotX)
			}

			if lowerOk {
				t.Log(msgLower, checkMark)
			} else {
				t.Fatal(msgLower, ballotX)
			}
		}

		t.Log("\twhen randomness was enabled only for upper boundary")
		{
			configuredUpper := float32(0.7)
			configuredLower := float32(0.5)
			bc := &boundaryTestLoopConfig{
				upper: &boundaryDefinition{
					mapFillPercentage: configuredUpper,
					enableRandomness:  true,
				},
				lower: &boundaryDefinition{
					mapFillPercentage: configuredLower,
					enableRandomness:  false,
				},
			}

			msgUpper := "\t\tevaluated upper must be less than one and greater than or equal to configured upper"
			msgLower := "\t\tevaluated lower must be equal to configured lower"

			upperOk := true
			lowerOk := true
			for i := 0; i < 100; i++ {
				evaluatedUpper, evaluatedLower := evaluateMapFillBoundaries(bc)
				if evaluatedUpper == 1 || evaluatedUpper < configuredUpper {
					upperOk = false
				}

				if evaluatedLower != configuredLower {
					lowerOk = false
				}
			}

			if upperOk {
				t.Log(msgUpper, checkMark)
			} else {
				t.Fatal(msgUpper, ballotX)
			}

			if lowerOk {
				t.Log(msgLower, checkMark)
			} else {
				t.Fatal(msgLower, ballotX)
			}
		}

		t.Log("\twhen randomness was enabled only for lower boundary")
		{
			configuredUpper := float32(0.7)
			configuredLower := float32(0.5)
			bc := &boundaryTestLoopConfig{
				upper: &boundaryDefinition{
					mapFillPercentage: configuredUpper,
					enableRandomness:  false,
				},
				lower: &boundaryDefinition{
					mapFillPercentage: configuredLower,
					enableRandomness:  true,
				},
			}

			msgUpper := "\t\tevaluated upper must be equal to configured upper"
			msgLower := "\t\tevaluated lower must be less than configured lower and greater than or equal to zero"

			upperOk := true
			lowerOk := true
			for i := 0; i < 100; i++ {
				evaluatedUpper, evaluatedLower := evaluateMapFillBoundaries(bc)
				if evaluatedUpper != configuredUpper {
					upperOk = false
				}

				if evaluatedLower >= configuredLower || evaluatedLower < 0 {
					lowerOk = false
				}
			}

			if upperOk {
				t.Log(msgUpper, checkMark)
			} else {
				t.Fatal(msgUpper, ballotX)
			}

			if lowerOk {
				t.Log(msgLower, checkMark)
			} else {
				t.Fatal(msgLower, ballotX)
			}
		}

		t.Log("\twhen randomness was disabled for both upper and lower boundary")
		{
			configuredUpper := float32(0.7)
			configuredLower := float32(0.5)
			bc := &boundaryTestLoopConfig{
				upper: &boundaryDefinition{
					mapFillPercentage: configuredUpper,
					enableRandomness:  false,
				},
				lower: &boundaryDefinition{
					mapFillPercentage: configuredLower,
					enableRandomness:  false,
				},
			}

			msgUpper := "\t\tevaluated upper must be equal to configured upper"
			msgLower := "\t\tevaluated lower must be equal to configured lower"

			upperOk := true
			lowerOk := true
			for i := 0; i < 100; i++ {
				evaluatedUpper, evaluatedLower := evaluateMapFillBoundaries(bc)
				if evaluatedUpper != configuredUpper {
					upperOk = false
				}

				if evaluatedLower != configuredLower {
					lowerOk = false
				}
			}

			if upperOk {
				t.Log(msgUpper, checkMark)
			} else {
				t.Fatal(msgUpper, ballotX)
			}

			if lowerOk {
				t.Log(msgLower, checkMark)
			} else {
				t.Fatal(msgLower, ballotX)
			}
		}
	}

}

func TestRunOperationChain(t *testing.T) {

	t.Log("given the boundary test loop's method for running an operation chain")
	{
		t.Log("\twhen the chain length is zero")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBoundaryTestLoop(
				rpOneMapOneRunNoEvictionScDisabled,
				sleepConfigDisabled,
				sleepConfigDisabled,
				1.0,
				0.0,
				1.0,
				0,
				true,
			)
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			mc := &modeCache{}
			ac := &actionCache{}
			keysCache := map[string]struct{}{}

			err := tl.runOperationChain(0, ms.m, mc, ac, "awesome-map", 0, keysCache)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmode cache must be in initial runnerState"
			if mc.current == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tactions cache must be in initial runnerState"
			if ac.last == "" && ac.next == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap action count must be zero for insert"
			if ms.m.setInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap action count must be zero for read"
			if ms.m.getInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap action count must be zero for remove"
			if ms.m.removeInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tkeys cache must be empty"
			if len(keysCache) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen chain length is greater than zero")
		{
			t.Log("\t\twhen upper boundary is 100 %, lower boundary is 0 %, and probability for action towards boundary is 100 %")
			{
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				chainLength := 10 * len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					1.0,
					0.0,
					1.0,
					chainLength,
					true,
				)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				mc := &modeCache{}
				ac := &actionCache{}
				keysCache := map[string]struct{}{}

				err := tl.runOperationChain(0, ms.m, mc, ac, "awesome-map", 0, keysCache)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				/*
						Number of elements in source data: 9
						Chain length: 90

						With both upper boundary and probability for action towards boundary set to 100 % as well as
						lower boundary set to 0 %, the operation chain will move back and forth between completely filling
						the map, then completely erasing the map, then going back to filling, etc.

						A chain length of 90 covers 10 times fully filling or fully erasing the map if the source data
						contains 9 elements. Thus, for each kind of the two state-changing operations, we expect them
						to be invoked 10 / 2 * 9 times --> 45 inserts and 45 removes.

						The number of reads must be equal to <number of completed full iterations on map> * len(<source data>) - <number of times a read was skipped>.
					    A read will be skipped if one of the following is true:
						(1) The cache is currently empty, meaning there is nothing to be read --> Happens whenever the mode changes from "drain" back to "fill" when
					        the map's lower boundary is 0 %
					    (2) Loop counter reached maximum --> Happens when loop counter is <chain length - 1> -- even when the last operation was a state-changing
					        operation, the next operation, which would necessarily be a read, will not be executed anymore

						This corresponds to 10 * 9 - 5 = 85 read operations.
				*/
				msg = "\t\t\tnumber of inserts performed must be five times the number of elements in the source data"
				if ms.m.setInvocations == 5*len(theFellowship) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d invocations, got %d", 10*len(theFellowship), ms.m.setInvocations))
				}

				msg = "\t\t\tnumber of removes performed must be five times the number of elements in the source data, too"
				if ms.m.removeInvocations == 5*len(theFellowship) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d invocations, got %d", 10*len(theFellowship), ms.m.removeInvocations))
				}

				msg = "\t\t\tnumber of reads performed must be ten times the number of elements in the source data minus the number of times the read had to be skipped"
				if ms.m.getInvocations == 10*len(theFellowship)-5 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d invocations, got %d", 10*len(theFellowship)-5, ms.m.getInvocations))
				}

			}

			t.Log("\t\twhen upper boundary is less than 100 %, lower boundary is greater than 0 %, and probability for action towards boundary is less than 100 %")
			{
				upperBoundary := 0.8
				lowerBoundary := 0.2
				ms := assembleTestMapStoreWithBoundaryMonitoring(&testMapStoreBehavior{}, &boundaryMonitoring{
					upperBoundaryNumElements: int(math.Round(float64(len(theFellowship)) * upperBoundary)),
					lowerBoundaryNumElements: int(math.Round(float64(len(theFellowship)) * lowerBoundary)),
				})
				chainLength := 1_000 * len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					float32(upperBoundary),
					float32(lowerBoundary),
					0.6,
					chainLength,
					true,
				)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				mc := &modeCache{}
				ac := &actionCache{}
				keysCache := map[string]struct{}{}

				err := tl.runOperationChain(0, ms.m, mc, ac, "awesome-map", 0, keysCache)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tthresholds defined by upper and lower boundaries must not be exceeded"
				if !ms.m.bm.upperBoundaryThresholdViolated {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("upper threshold is %d, was violated with map size %d", ms.m.bm.upperBoundaryNumElements, ms.m.bm.upperBoundaryViolationValue))
				}

				if !ms.m.bm.lowerBoundaryThresholdViolated {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("lower threshold is %d, was violated with map size %d", ms.m.bm.lowerBoundaryNumElements, ms.m.bm.lowerBoundaryViolationValue))
				}

			}
		}

		t.Log("\twhen sleep between operation chains has been enabled")
		{
			rc := assembleRunnerConfigForBoundaryTestLoop(
				rpOneMapOneRunNoEvictionScDisabled,
				&sleepConfig{true, 1_000, false},
				sleepConfigDisabled,
				1.0,
				0.0,
				1.0,
				1,
				true,
			)
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			ts := &testSleeper{}
			tl.s = ts

			_ = tl.runOperationChain(42, ms.m, &modeCache{}, &actionCache{}, "awesome-map", 42, map[string]struct{}{})

			msg := "\t\tsleep must have been invoked"

			if ts.sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen sleep upon mode change has been enabled")
		{
			rc := assembleRunnerConfigForBoundaryTestLoop(
				rpOneMapOneRunNoEvictionScDisabled,
				sleepConfigDisabled,
				&sleepConfig{true, 1_000, false},
				1.0,
				0.0,
				1.0,
				1,
				true,
			)
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			ts := &testSleeper{}
			tl.s = ts

			_ = tl.runOperationChain(3, ms.m, &modeCache{}, &actionCache{}, "awesome-map", 12, map[string]struct{}{})

			msg := "\t\tsleep must have been invoked"

			if ts.sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func (s *testSleeper) sleep(_ *sleepConfig, _ evaluateTimeToSleep, _ string) {

	s.sleepInvoked = true

}

func TestRunWithBatchTestLoop(t *testing.T) {

	t.Log("given the maps batch test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				id := uuid.New()
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				numMaps, numRuns := uint16(1), uint32(1)
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             numMaps,
						numRuns:             numRuns,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(id, testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				tl.run()
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

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

				if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, key, detail)
				}
			}()
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				numMaps, numRuns := uint16(10), uint32(1)
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             numMaps,
						numRuns:             numRuns,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{})
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				tl.run()
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

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

				if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, key, detail)
				}
			}()
		}

		t.Log("\twhen get map yields error")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				numMaps, numRuns := uint16(1), uint32(1)
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             numMaps,
						numRuns:             numRuns,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponGetMap: true})
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				tl.run()
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

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

				if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), numMaps, numRuns, uint32(numMaps)*numRuns, true); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, key, detail)
				}
			}()
		}

		t.Log("\twhen only one run is executed an error is thrown during read all")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				numMaps, numRuns := uint16(1), uint32(1)
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             numMaps,
						numRuns:             numRuns,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponGet: true})
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				tl.run()
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

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
				if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), numMaps, numRuns, expectedRuns, true); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, key, detail)
				}
			}()
		}

		t.Log("\twhen no map goroutine is launched because the configured number of maps is zero")
		{
			id := uuid.New()
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			numMaps, numRuns := uint16(0), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             numMaps,
					numRuns:             numRuns,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(id, testSource, &testHzClientHandler{}, ms, rc)

			go tl.gatherer.Listen()
			tl.run()
			tl.gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			msg := "\t\tinitial status must contain correct values anyway"

			if ok, key, detail := statusContainsExpectedValues(tl.gatherer.AssembleStatusCopy(), numMaps, numRuns, 0, true); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, key, detail)
			}
		}
		t.Log("\twhen sleep configs for sleep between runs and sleep between action batches are disabled")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				scBetweenRuns := &sleepConfig{}
				scBetweenActionBatches := &sleepConfig{}
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             20,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    scBetweenRuns,
					},
					sleepConfigDisabled,
					scBetweenActionBatches,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, assembleTestMapStore(&testMapStoreBehavior{}), rc)

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
			}()
		}

		t.Log("\twhen sleep configs for sleep between runs and sleep between action batches are enabled")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				numRuns := uint32(20)
				scBetweenRuns := &sleepConfig{enabled: true}
				scBetweenActionsBatches := &sleepConfig{enabled: true}
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             numRuns,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    scBetweenRuns,
					},
					sleepConfigDisabled,
					scBetweenActionsBatches,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, assembleTestMapStore(&testMapStoreBehavior{}), rc)

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
			}()
		}
	}

}

func TestIngestAll(t *testing.T) {

	t.Log("given a hazelcast map and elements for insertion")
	{
		t.Log("\twhen target map does not contain key yet and set does not yield error")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				statusRecord := map[statusKey]any{
					statusKeyNumFailedInserts: 0,
				}
				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\tnumber of contains key invocations must be equal to number of elements in source data"
				expected := len(tl.tle.elements)
				actual := ms.m.containsKeyInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = "\t\tnumber of set invocations must be equal to number of elements in source data, too"
				actual = ms.m.setInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = "\t\tstatus record must indicate there have been zero failed insert attempts"
				expected = 0
				actual = statusRecord[statusKeyNumFailedInserts].(int)
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = "\t\tnumber of get or assemble payload invocations must be equal to number of elements in source data"
				if getOrAssembleObservations.numInvocations == len(tl.tle.elements) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, getOrAssembleObservations.numInvocations)
				}
			}()
		}
		t.Log("\twhen target map contains all keys")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
				populateTestHzMapStore(&ms)

				statusRecord := map[statusKey]any{
					statusKeyNumFailedInserts: 0,
				}
				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))
				msg := "\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\tnumber of contains key invocations must be equal to number of elements in source data"
				expected := len(theFellowship)
				actual := ms.m.containsKeyInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = "\t\tnumber of inserts must be zero"
				expected = 0
				actual = ms.m.setInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = "\t\tstatus record must indicate zero failed inserts"
				actual = statusRecord[statusKeyNumFailedInserts].(int)
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}
			}()
		}
		t.Log("\twhen target map contains no keys and insert yields error")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				ms := assembleTestMapStore(&testMapStoreBehavior{
					returnErrorUponSet: true,
				})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))
				tl.gatherer.StopListen()

				msg := "\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				expected := 1
				msg = fmt.Sprintf("\t\tnumber of contains key checks must be %d", expected)
				actual := ms.m.containsKeyInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				msg = fmt.Sprintf("\t\tnumber of inserts must be %d", expected)
				actual = ms.m.setInvocations
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}

				waitForStatusGatheringDone(tl.gatherer)

				msg = "\t\tstatus gatherer must have been informed about one failed insert attempt"
				if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedInserts, 1); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}()

		}
		t.Log("\twhen contains key check yields error")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				ms := assembleTestMapStore(&testMapStoreBehavior{
					returnErrorUponContainsKey: true,
				})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen()
				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))
				tl.gatherer.StopListen()

				msg := "\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\tstatus gatherer must have been informed about one failed contains key check"
				waitForStatusGatheringDone(tl.gatherer)
				if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedKeyChecks, 1); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}()

		}
		t.Log("\twhen sleep after batch action is enabled")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					&sleepConfig{
						enabled: true,
					},
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
				s := &testSleeper{}
				tl.s = s

				go tl.gatherer.Listen()
				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))
				tl.gatherer.StopListen()

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\tsleeper must have been invoked"
				if s.sleepInvoked {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}()
		}
		t.Log("\twhen invocation of get or assemble payload yields error")
		{
			func() {
				defer resetGetOrAssemblePayloadTestSetup()

				getOrAssembleBehavior = getOrAssemblePayloadFunctionBehavior{returnError: true}

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             9,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    sleepConfigDisabled,
					},
					sleepConfigDisabled,
					sleepConfigDisabled,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

				err := tl.ingestAll(ms.m, "awesome-map", uint16(0))

				msg := "\t\terror must be returned"
				if errors.Is(err, getOrAssemblePayloadError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\tget or assemble payload function must have been invoked only once"
				if getOrAssembleObservations.numInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, getOrAssembleObservations.numInvocations)
				}

				msg = "\t\tthere must have been no attempt of inserting a key-value pair into the target map"
				if ms.m.setInvocations == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}()
		}
	}

}

func TestReadAll(t *testing.T) {

	t.Log("given a hazelcast map")
	{
		t.Log("\twhen map contains all elements expected based on data source and no access operation fails")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             12,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(&ms)

			go tl.gatherer.Listen()
			err := tl.readAll(ms.m, testMapBaseName, 0)
			tl.gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			msg := "\t\tstatus gatherer must indicate zero failed operations"
			statusCopy := tl.gatherer.AssembleStatusCopy()
			for _, v := range counters {
				if ok, detail := expectedStatusPresent(statusCopy, v, 0); ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, detail)
				}
			}

			msg = "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tnumber of get invocations must be equal to number of elements in source data"
			expected := len(theFellowship)
			actual := ms.m.getInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
			}
		}

		t.Log("\twhen get invocation yields error")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponGet: true})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			go tl.gatherer.Listen()
			err := tl.readAll(ms.m, testMapBaseName, 0)
			tl.gatherer.StopListen()

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			msg = "\t\tstatus gatherer must have been informed about one failed read"
			if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedReads, 1); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}

		t.Log("\twhen map contains elements and get yields no error, but retrieved value is nil")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			ms.m.data.Store(assembleMapKey(0, "legolas"), nil)

			go tl.gatherer.Listen()
			err := tl.readAll(ms.m, testMapBaseName, 0)
			tl.gatherer.StopListen()

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			statusCopy := tl.gatherer.AssembleStatusCopy()

			msg = "\t\tstatus gatherer must indicate zero failed reads"
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumFailedReads, 0); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tstatus gatherer must indicate one nil read"
			if ok, detail := expectedStatusPresent(statusCopy, statusKeyNumNilReads, 1); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}

		t.Log("\twhen sleep after batch action is enabled")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				&sleepConfig{
					enabled: true,
				},
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(&ms)

			s := &testSleeper{}
			tl.s = s

			go tl.gatherer.Listen()
			err := tl.readAll(ms.m, "yet-another-awesome-map", uint16(0))
			tl.gatherer.StopListen()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tsleeper must have been invoked"
			if s.sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}
}

func TestRemoveSome(t *testing.T) {

	t.Log("given a hazelcast map containing state")
	{
		t.Log("\twhen remove operation does not yield error")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			populateTestHzMapStore(&ms)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedRemoves: 0,
			}
			err := tl.removeSome(ms.m, testMapBaseName, uint16(0))

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tstatus record must indicate zero failed remove invocations"
			expected := 0
			actual := statusRecord[statusKeyNumFailedRemoves].(int)

			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
			}
		}

		t.Log("\twhen remove operation yields error")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{returnErrorUponRemove: true})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)

			populateTestHzMapStore(&ms)

			go tl.gatherer.Listen()
			err := tl.removeSome(ms.m, testMapBaseName, uint16(0))
			tl.gatherer.StopListen()

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			msg = "\t\tstatus gatherer must have been informed about one failed remove invocation"
			if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedRemoves, 1); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}
		t.Log("\twhen sleep after batch action is enabled")
		{
			ms := assembleTestMapStore(&testMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				&runnerProperties{
					numMaps:             1,
					numRuns:             9,
					cleanMapsPriorToRun: false,
					sleepBetweenRuns:    sleepConfigDisabled,
				},
				&sleepConfig{
					enabled: true,
				},
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(&ms)

			s := &testSleeper{}
			tl.s = s

			go tl.gatherer.Listen()
			err := tl.removeSome(ms.m, "yet-another-awesome-map", uint16(0))
			tl.gatherer.StopListen()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tsleeper must have been invoked"
			if s.sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func resetGetOrAssemblePayloadTestSetup() {

	getOrAssembleObservations = getOrAssemblePayloadFunctionObservations{}
	getOrAssembleBehavior = getOrAssemblePayloadFunctionBehavior{}

}

func expectedStatusPresent(statusCopy map[string]any, expectedKey statusKey, expectedValue int) (bool, string) {

	recordedValue := statusCopy[string(expectedKey)].(int)

	if recordedValue == expectedValue {
		return true, ""
	} else {
		return false, fmt.Sprintf("expected %d, got %d", expectedValue, recordedValue)
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

	insertCount := 0
	removeCount := 0
	otherCount := 0
	for i := 0; i < numInvocations; i++ {
		action := determineNextMapAction(&modeCache{currentMode, false}, read, float32(actionProbability), 1)
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

func statusContainsExpectedValues(status map[string]any, expectedNumMaps uint16, expectedNumRuns uint32, expectedTotalRuns uint32, expectedRunnerFinished bool) (bool, statusKey, string) {

	if numMapsFromStatus, ok := status[string(statusKeyNumMaps)]; !ok || numMapsFromStatus != expectedNumMaps {
		return false, statusKeyNumMaps, fmt.Sprintf("want: %d; got: %d", expectedNumMaps, numMapsFromStatus)
	}

	if numRunsFromStatus, ok := status[string(statusKeyNumRuns)]; !ok || numRunsFromStatus != expectedNumRuns {
		return false, statusKeyNumRuns, fmt.Sprintf("want: %d; got: %d", expectedNumRuns, numRunsFromStatus)
	}

	if totalRunsFromStatus, ok := status[string(statusKeyTotalNumRuns)]; !ok || totalRunsFromStatus != expectedTotalRuns {
		return false, statusKeyTotalNumRuns, fmt.Sprintf("want: %d; got: %d", expectedTotalRuns, totalRunsFromStatus)
	}

	if runnerFinishedFromStatus, ok := status[string(statusKeyFinished)]; !ok || runnerFinishedFromStatus != expectedRunnerFinished {
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

func assembleBoundaryTestLoop(id uuid.UUID, source string, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore, rc *runnerConfig) boundaryTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, rc, ch, ms)
	tl := boundaryTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl
}

func assembleBatchTestLoop(id uuid.UUID, source string, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore, rc *runnerConfig) batchTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, rc, ch, ms)
	tl := batchTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl

}

func assembleTestLoopExecution(id uuid.UUID, source string, rc *runnerConfig, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore) testLoopExecution[string] {

	return testLoopExecution[string]{
		id:                   id,
		source:               source,
		hzClientHandler:      ch,
		hzMapStore:           ms,
		runnerConfig:         rc,
		elements:             theFellowship,
		ctx:                  nil,
		getElementID:         fellowshipMemberName,
		getOrAssemblePayload: returnFellowshipMemberName,
	}

}

func returnFellowshipMemberName(_ string, _ uint16, element any) (any, error) {
	getOrAssembleObservations.numInvocations++

	if getOrAssembleBehavior.returnError {
		return "", getOrAssemblePayloadError
	}
	return element, nil
}

func assembleTestMapStoreWithBoundaryMonitoring(b *testMapStoreBehavior, bm *boundaryMonitoring) testHzMapStore {

	ms := assembleTestMapStore(b)
	ms.m.bm = bm

	return ms

}

func assembleTestMapStore(b *testMapStoreBehavior) testHzMapStore {

	testBackend := &sync.Map{}

	return testHzMapStore{
		m: &testHzMap{
			data:                       testBackend,
			returnErrorUponGet:         b.returnErrorUponGet,
			returnErrorUponSet:         b.returnErrorUponSet,
			returnErrorUponContainsKey: b.returnErrorUponContainsKey,
			returnErrorUponRemove:      b.returnErrorUponRemove,
			returnErrorUponRemoveAll:   b.returnErrorUponRemoveAll,
			returnErrorUponEvictAll:    b.returnErrorUponEvictAll,
		},
		returnErrorUponGetMap: b.returnErrorUponGetMap,
	}

}

func assembleRunnerConfigForBoundaryTestLoop(
	rp *runnerProperties,
	sleepBetweenOperationChains *sleepConfig,
	sleepUponModeChange *sleepConfig,
	upperBoundaryMapFillPercentage, lowerBoundaryMapFillPercentage, actionTowardsBoundaryProbability float32,
	operationChainLength int,
	resetAfterChain bool,
) *runnerConfig {

	c := assembleBaseRunnerConfig(rp)
	c.boundary = &boundaryTestLoopConfig{
		sleepBetweenOperationChains: sleepBetweenOperationChains,
		sleepAfterChainAction:       sleepConfigDisabled,
		sleepUponModeChange:         sleepUponModeChange,
		chainLength:                 operationChainLength,
		resetAfterChain:             resetAfterChain,
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
	rp *runnerProperties,
	sleepAfterChainAction *sleepConfig,
	sleepBetweenActionBatches *sleepConfig,
) *runnerConfig {

	c := assembleBaseRunnerConfig(rp)
	c.batch = &batchTestLoopConfig{sleepAfterChainAction, sleepBetweenActionBatches}

	return c

}

func fellowshipMemberName(element any) string {

	return element.(string)

}

func populateTestHzMapStore(ms *testHzMapStore) {

	mapNumber := uint16(0)
	// Store all elements from test data source in map
	for _, value := range theFellowship {
		key := assembleMapKey(mapNumber, value)

		ms.m.data.Store(key, value)
	}

}

func assembleBaseRunnerConfig(rp *runnerProperties) *runnerConfig {

	return &runnerConfig{
		enabled:                 true,
		numMaps:                 rp.numMaps,
		numRuns:                 rp.numRuns,
		mapBaseName:             "test",
		useMapPrefix:            true,
		mapPrefix:               "ht_",
		appendMapIndexToMapName: false,
		appendClientIdToMapName: false,
		preRunClean: &preRunCleanConfig{
			enabled:       rp.cleanMapsPriorToRun,
			errorBehavior: rp.mapCleanErrorBehavior,
		},
		sleepBetweenRuns: rp.sleepBetweenRuns,
		loopType:         boundary,
		batch:            nil,
		boundary:         nil,
	}

}
