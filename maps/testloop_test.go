package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/loadsupport"
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
		numEntriesPerMap      uint32
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
	defaultTestMapName        = "awesome-map"
	defaultTestMapNumber      = uint16(0)
)

func (b *testSingleMapCleanerBuilder) Build(_ *state.SingleMapCleanerBuildValues) (state.SingleCleaner, string) {

	return b.mapCleanerToReturn, "hz:impl:mapService"

}

func (c *testSingleMapCleaner) Clean(_ string) state.SingleCleanResult {

	c.observations.cleanInvocations++

	if c.behavior.returnErrorUponClean {
		return state.SingleCleanResult{Err: fmt.Errorf("something somewhere went terribly wrong")}
	}

	return state.SingleCleanResult{NumCleanedItems: c.behavior.numElementsCleanedReturnValue}

}

func BenchmarkRunOperationChain(b *testing.B) {

	ms := assembleTestMapStore(&testMapStoreBehavior{})
	chainLength := 100 * len(theFellowship)
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
	tl := assembleBoundaryTestLoopForBenchmark(uuid.New(), testSource, 1_000, &testHzClientHandler{}, ms, rc)

	mc := &modeCache{}
	ac := &actionCache{}
	ic := &indexCache{}
	elementsInserted := make(map[string]struct{})

	availableElements := &availableElementsWrapper{
		maxNum: 0,
		pool:   make(map[string]struct{}),
	}
	for i := 0; i < b.N; i++ {
		err := tl.runOperationChain(0, ms.m, mc, ac, ic, "awesome-map-name", 0, elementsInserted, availableElements)

		if err != nil {
			b.Fatal("encountered error in scope of running operation chain: ", err)
		}
	}

}

func TestPopulateElementsAvailableForInsertion(t *testing.T) {

	t.Log("given source data elements that are available for insertion as long as they haven't been inserted yet")
	{
		t.Log("\twhen test loop execution's source data contains at least one element")
		{
			tl := &boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
					getElementID: func(element any) string {
						return element.(string)
					},
				},
			}

			availableForInsertion := tl.populateElementsAvailableForInsertion()

			msg := "\t\telements available for insertion must have been populated"
			if len(availableForInsertion) == len(theFellowship) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\telements available for insertion must contain all elements from source data"
			for _, v := range theFellowship {
				key := fellowshipMemberName(v)
				if _, ok := availableForInsertion[key]; ok {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}
		}
		t.Log("\twhen test loop execution's source data contains zero elements")
		{
			tl := &boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: []string{},
				},
			}

			availableForInsertion := tl.populateElementsAvailableForInsertion()

			msg := "\t\treturned map must be empty"
			if len(availableForInsertion) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestMapTestLoopCountersTrackerInit(t *testing.T) {

	t.Log("given the tracker's init function")
	{
		t.Log("\twhen init method is invoked")
		{
			ct := &mapTestLoopCountersTracker{}
			g := status.NewGatherer()

			go g.Listen(make(chan struct{}, 1))
			ct.init(g)
			g.StopListen()

			msg := "\t\tgatherer must have been assigned"
			if ct.gatherer == g {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tall status keys must have been inserted into status record"
			initialCounterValue := uint64(0)
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
				if ok, detail := expectedCounterValuePresent(statusCopy, v, initialCounterValue, eq); ok {
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
				counters: make(map[statusKey]uint64),
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
					g := ct.gatherer.(*status.DefaultGatherer)
					update := <-g.Updates
					if update.Key == string(v) && update.Value == uint64(1) {
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
				counters: make(map[statusKey]uint64),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			go ct.gatherer.Listen(make(chan struct{}, 1))
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

			if ct.counters[statusKeyNumFailedInserts] == uint64(numInvokingGoroutines) {
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

				elementsInserted := make(map[string]struct{}, len(theFellowship)-1)
				for i := 0; i < len(theFellowship)-1; i++ {
					elementsInserted[fellowshipMemberName(theFellowship[i])] = struct{}{}
				}

				elementNotInserted := theFellowship[len(theFellowship)-1]
				keyOfNotInsertedElement := fellowshipMemberName(elementNotInserted)
				elementsAvailableForInsertion := map[string]struct{}{
					keyOfNotInsertedElement: {},
				}
				chosenKey, err := tl.chooseNextMapElementKey(insert, elementsInserted, elementsAvailableForInsertion)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tchosen key must be only one not yet stored in cache"
				if chosenKey == keyOfNotInsertedElement {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", chosenKey, elementsAvailableForInsertion))
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
				cache := map[string]struct{}{
					fellowshipMemberName(theFellowship[0]): {},
				}

				chosenKey, err := tl.chooseNextMapElementKey(insert, cache, make(map[string]struct{}))

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, err)
				}

				msg = "\t\t\tchosen key must be equal to only one available in source data"
				if chosenKey == theFellowship[0] {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", chosenKey, theFellowship[0]))
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

					// When the cache of keys representing the values that have been written to the
					// target Hazelcast map is empty, then that means all keys (and their associated values)
					// must be available for insertion.
					availableForInsertion := make(map[string]struct{}, len(theFellowship))
					for i := 0; i < len(theFellowship); i++ {
						key := fellowshipMemberName(theFellowship[i])
						availableForInsertion[key] = struct{}{}
					}
					chosenKey, err := tl.chooseNextMapElementKey(action, make(map[string]struct{}), availableForInsertion)

					msg := "\t\t\terror must be returned"
					if err != nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\tchosen key element must be empty string"
					if chosenKey == "" {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
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

					cache := map[string]struct{}{
						fellowshipMemberName(elementInSourceData): {},
					}

					elementsAvailableForInsertion := make(map[string]struct{}, len(theFellowship)-1)
					for i := 1; i < len(theFellowship); i++ {
						key := fellowshipMemberName(elementInSourceData)
						elementsAvailableForInsertion[key] = struct{}{}
					}

					chosenKey, err := tl.chooseNextMapElementKey(action, cache, elementsAvailableForInsertion)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tchosen key must be equal to key stored in cache"
					if chosenKey == elementInSourceData {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", chosenKey, elementInSourceData))
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

			chosenKey, err := tl.chooseNextMapElementKey("awesomeNonExistingMapAction", make(map[string]struct{}), make(map[string]struct{}))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tchosen key must be empty string"
			if chosenKey == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
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
			predicate := assemblePredicate(clientID, defaultTestMapName, defaultTestMapNumber)

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
			expectedFilter := fmt.Sprintf("(__key like %s-%s-%d%%)", clientID, defaultTestMapName, defaultTestMapNumber)
			msg = "\t\tpredicate must contain expected filter"
			if actualFilter == expectedFilter {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%s', got '%s'", expectedFilter, actualFilter))
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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
			go tl.gatherer.Listen(make(chan struct{}, 1))
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
				if ok, detail := expectedCounterValuePresent(sc, v, 0, eq); ok {
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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			cleaner := &testSingleMapCleaner{
				observations: &testSingleMapCleanerObservations{},
			}
			tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

			go tl.gatherer.Listen(make(chan struct{}, 1))

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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				cleaner := &testSingleMapCleaner{
					behavior: &testSingleMapCleanerBehavior{
						numElementsCleanedReturnValue: 1,
					},
					observations: &testSingleMapCleanerObservations{},
				}
				tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				cleaner := &testSingleMapCleaner{
					behavior:     &testSingleMapCleanerBehavior{returnErrorUponClean: true},
					observations: &testSingleMapCleanerObservations{},
				}
				tl.tle.stateCleanerBuilder = &testSingleMapCleanerBuilder{mapCleanerToReturn: cleaner}

				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := 0
					mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
					go tl.gatherer.Listen(make(chan struct{}, 1))
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
					if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedInserts, 0, eq); ok {
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

					go tl.gatherer.Listen(make(chan struct{}, 1))
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
					if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumFailedInserts, 1, eq); ok {
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := uint16(0)
					populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
					action := insert

					mapNumber := 0
					mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
					go tl.gatherer.Listen(make(chan struct{}, 1))
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

					go tl.gatherer.Listen(make(chan struct{}, 1))
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
					if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedRemoves, 0, eq); ok {
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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

				if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumFailedRemoves, 1, eq); ok {
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumNilReads, 1, eq); ok {
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumFailedReads, 1, eq); ok {
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

					populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

					err := tl.executeMapAction(ms.m, defaultTestMapName, defaultTestMapNumber, theFellowship[0], read)

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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
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
		t.Log("\twhen index position indicating currently stored elements is less than lower boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			lowerBoundary := float32(0.5)
			currentIndex := uint32(lowerBoundary*float32(len(theFellowship)) - 1)
			maxNumElements := uint32(len(theFellowship))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, lowerBoundary, currentIndex, maxNumElements, drain)

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

		t.Log("\twhen index indicating currently stored number of elements matches lower boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := drain
			lowerBoundary := float32(0.3)
			currentIndex := uint32(math.Round(float64(len(theFellowship)) * float64(lowerBoundary)))
			maxNumElements := uint32(len(theFellowship))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, lowerBoundary, currentIndex, maxNumElements, currentMode)

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

		t.Log("\twhen index indicating currently stored number of elements is between lower and upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := drain
			upperBoundary := float32(0.8)
			currentIndex := uint32(float32(len(theFellowship))*upperBoundary) - 1
			maxNumElements := uint32(len(theFellowship))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(upperBoundary, 0.2, currentIndex, maxNumElements, currentMode)

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

		t.Log("\twhen index indicating currently stored number of elements is equal to upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			currentMode := fill
			upperBoundary := float32(0.8)
			currentIndex := uint32(math.Round(float64(len(theFellowship)) * float64(upperBoundary)))
			maxNumElements := uint32(len(theFellowship))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(upperBoundary, 0.2, currentIndex, maxNumElements, currentMode)

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

		t.Log("\twhen index indicating currently stored number of elements is greater than upper boundary")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			maxNumElements := uint32(len(theFellowship))
			nextMode, forceActionTowardsMode := btl.checkForModeChange(0.8, 0.2, maxNumElements, maxNumElements, fill)

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

		t.Log("\twhen index indicating currently stored number of elements is zero and current mode is unset")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			nextMode, forceActionTowardsMode := btl.checkForModeChange(1.0, 0.0, 0, uint32(len(theFellowship)), "")

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

		t.Log("\twhen index indicating currently stored number of elements is greater than zero and current mode is unset")
		{
			btl := boundaryTestLoop[string]{
				tle: &testLoopExecution[string]{
					elements: theFellowship,
				},
			}
			nextMode, forceActionTowardsMode := btl.checkForModeChange(1.0, 0.0, 500, 501, "")

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

	t.Log("given the boundary test loop in use pre-initialized elements mode")
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
							numEntriesPerMap:    uint32(len(theFellowship)),
							cleanMapsPriorToRun: false,
							sleepBetweenRuns:    sleepConfigDisabled,
						},
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						1.0,
						// Since the operation chain is not reset by one step for a read operation, we now need at least
						// two times the length of the data set for all the elements to be inserted in one run of the
						// chain
						2*len(theFellowship),
						true,
					)
					ms := assembleTestMapStore(&testMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, true, &testHzClientHandler{}, ms, rc)

					tl.run()

					msg := "\t\t\t\tall elements must be inserted in map"
					if ms.m.setInvocations == len(theFellowship) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

					// Because the operation chain doesn't get reset by one step after a read anymore and because
					// we provide two times the number of elements in the source data as the length of the operation,
					// we now have space in the operation chain for 9 modifying operations and a full 9 read operations
					msg = "\t\t\t\tnumber of get invocations must be equal to number of set invocations"
					if ms.m.getInvocations == ms.m.setInvocations {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.getInvocations)
					}

					msg = "\t\t\t\tnumber of remove invocations must be zero"
					if ms.m.removeInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.removeInvocations)
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
					tl := assembleBoundaryTestLoop(id, testSource, true, &testHzClientHandler{}, ms, rc)

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
							numEntriesPerMap:    uint32(len(theFellowship)),
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
					tl := assembleBoundaryTestLoop(id, testSource, true, &testHzClientHandler{}, ms, rc)

					tl.run()

					// With an action-towards-boundary-probability of 50 %, the test loop will execute inserts, reads,
					// and removes roughly the same number of times due to the fact that after a remove on a cache of
					// size 1, the next action has to be an insert rather than a read. Hence, the usual rule of "do a
					// read after each modifying action" does not apply here.
					msg := "\t\t\t\tnumber of set invocations must be roughly one third of the chain length"
					if math.Abs(float64(ms.m.setInvocations-chainLength/3)) < 5 {
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
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

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
				tl := boundaryTestLoop[string]{
					tle: &testLoopExecution[string]{
						ctx:          context.TODO(),
						elements:     theFellowship,
						runnerConfig: assembleBaseRunnerConfig(&runnerProperties{numEntriesPerMap: uint32(42)}),
						getElementID: func(element any) string {
							return element.(string)
						},
						usePreInitializedElements: true,
					},
				}

				ms := assembleTestMapStore(&testMapStoreBehavior{})
				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				mc := &modeCache{current: fill, forceActionTowardsMode: true}
				ac := &actionCache{last: insert, next: read}
				keysCache := make(map[string]struct{})
				numKeysStored := 0
				ms.m.data.Range(func(key, value any) bool {
					keysCache[key.(string)] = struct{}{}
					numKeysStored++
					return true
				})
				ic := &indexCache{current: uint32(numKeysStored)}
				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   make(map[string]struct{}, len(theFellowship)),
				}
				tl.resetAfterOperationChain(ms.m, defaultTestMapName, defaultTestMapNumber, &keysCache, available, mc, ac, ic)

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

				msg = "\t\t\tindex cache must have been reset"
				if ic.current == 0 {
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
				expectedPredicateFilter := fmt.Sprintf("%s-%s-%d", client.ID(), defaultTestMapName, defaultTestMapNumber)
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

				msg = "\t\t\tstore representing elements available for insertion must have been re-populated"
				if len(available.pool) == len(theFellowship) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d element/-s, got %d", len(theFellowship), len(available.pool)))
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
				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

				keysCache := make(map[string]struct{})
				numKeysStored := 0
				ms.m.data.Range(func(key, value any) bool {
					keysCache[key.(string)] = struct{}{}
					numKeysStored++
					return true
				})
				mc := &modeCache{current: fill}
				ac := &actionCache{last: insert, next: read}
				ic := &indexCache{current: uint32(numKeysStored)}

				elementsAvailableForInsertion := make(map[string]struct{}, len(theFellowship))
				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   elementsAvailableForInsertion,
				}
				tl.resetAfterOperationChain(ms.m, "", mapNumber, &keysCache, available, mc, ac, ic)

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

				msg = "\t\t\tindex cache must have remained unmodified, too"
				if ic.current == uint32(numKeysStored) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tstore representing set of elements available for insertion must remain unmodified, too"
				if len(elementsAvailableForInsertion) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 elements, got %d", len(elementsAvailableForInsertion)))
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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			mc := &modeCache{}
			ac := &actionCache{}
			ic := &indexCache{}
			keysCache := make(map[string]struct{})
			available := &availableElementsWrapper{}

			err := tl.runOperationChain(0, ms.m, mc, ac, ic, "awesome-map", 0, keysCache, available)

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
				/*
					Set up chain length so the chain will run both three complete fills and three complete drains.
					One complete fill-drain cycle takes 35 steps rather than 36 on a source data pool of 9 elements
					because the 36th operation would be a read, but at this point, the cache is already empty,
					so the 36th operation instead becomes the first operation of the next iteration. Thus, if we wish
					to set up the operation chain for three complete fill-and-drain iterations without starting
					the next iteration, the chain needs to have 3*36-3 or 12*9-3 steps.
				*/
				chainLength := 12*len(theFellowship) - 3
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, false, &testHzClientHandler{}, ms, rc)

				mc := &modeCache{}
				ac := &actionCache{}
				ic := &indexCache{}
				keysCache := make(map[string]struct{})

				mapName := "awesome-map"
				mapNumber := uint16(0)

				availableForInsertion := populateElementsAvailableForInsertion(mapName, mapNumber, theFellowship)
				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   availableForInsertion,
				}
				err := tl.runOperationChain(0, ms.m, mc, ac, ic, mapName, mapNumber, keysCache, available)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				/*
					Number of elements in source data: 9
					Chain length: 105

					With both upper boundary and probability for action towards boundary set to 100 % as well as
					lower boundary set to 0 %, the operation chain will move back and forth between completely filling
					and draining the map by always executing state-altering operations in the corresponding "direction",
					i.e. fill mode will only execute inserts and reads, and drain mode will only run removes and reads.

					Within this framework, the operation chain requires 35 operations for one complete fill-drain cycle
					(35 because, as mentioned above when defining the chain length, the last read of iteration n
					becomes the first insert of iteration n+1 because that last read would otherwise be executed
					on an empty cache), and the operation counts accumulate as follows:

					Chain position (when either 9 or zero is last hit during this iteration) -> index -> operation counts
					(start: fill)		0 	-> 	0 	-> 0 sets, 	0 gets, 0 removes
					(result of fill) 	17 	-> 	9 	-> 9 sets, 	9 gets, 0 removes 	-- +9 of primary, +9 gets
					(result of drain) 	34 	-> 	0 	-> 9 sets, 	17 gets, 9 removes 	-- +9 of primary, +8 gets
					(result of fill) 	52 	-> 	9 	-> 18 sets, 26 gets, 9 removes 	-- +9 of primary, +9 gets
					(result of drain) 	69 	-> 	0 	-> 18 sets, 34 gets, 18 removes -- +9 of primary, +8 gets
					(result of fill) 	87 	-> 	9 	-> 27 sets, 43 gets, 18 removes -- +9 of primary, +9 gets
					(result of drain) 	104 ->	0 	-> 27 sets, 51 gets, 27 removes -- +9 of primary, +8 gets
				*/
				expectedNumStateAlteringInvocations := 12 * len(theFellowship) / 4
				expectedNumGetInvocations := 12*len(theFellowship)/2 - 3
				msg = "\t\t\tnumber of inserts performed must be five times the number of elements in the source data"
				if ms.m.setInvocations == expectedNumStateAlteringInvocations {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d invocations, got %d", 10*len(theFellowship), ms.m.setInvocations))
				}

				msg = "\t\t\tnumber of removes performed must be five times the number of elements in the source data, too"
				if ms.m.removeInvocations == expectedNumStateAlteringInvocations {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d invocations, got %d", 10*len(theFellowship), ms.m.removeInvocations))
				}

				msg = "\t\t\tnumber of reads performed must be ten times the number of elements in the source data minus the number of times the read had to be skipped"
				if ms.m.getInvocations == expectedNumGetInvocations {
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
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				mc := &modeCache{}
				ac := &actionCache{}
				ic := &indexCache{}
				keysCache := make(map[string]struct{})

				mapName := "awesome-map"
				mapNumber := uint16(0)

				availableForInsertion := populateElementsAvailableForInsertion(mapName, mapNumber, theFellowship)
				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   availableForInsertion,
				}

				err := tl.runOperationChain(0, ms.m, mc, ac, ic, mapName, mapNumber, keysCache, available)

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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			ts := &testSleeper{}
			tl.s = ts

			_ = tl.runOperationChain(42, ms.m, &modeCache{}, &actionCache{}, &indexCache{}, "awesome-map", 42, make(map[string]struct{}), &availableElementsWrapper{})

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
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			ts := &testSleeper{}
			tl.s = ts

			_ = tl.runOperationChain(3, ms.m, &modeCache{}, &actionCache{}, &indexCache{}, "awesome-map", 12, make(map[string]struct{}), &availableElementsWrapper{})

			msg := "\t\tsleep must have been invoked"

			if ts.sleepInvoked {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen action in chain yields error")
		{
			t.Log("\t\twhen first insert yields error")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					&sleepConfig{true, 1_000, false},
					1.0,
					0.0,
					1.0,
					3,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{
					returnErrorUponSet: true,
				})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				ts := &testSleeper{}
				tl.s = ts

				ac := &actionCache{}
				ic := &indexCache{}
				kc := make(map[string]struct{})

				mapName := "awesome-map"
				mapNumber := uint16(12)

				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   populateElementsAvailableForInsertion(mapName, mapNumber, theFellowship),
				}
				_ = tl.runOperationChain(3, ms.m, &modeCache{}, ac, ic, mapName, mapNumber, kc, available)

				msg := "\t\t\taction must be tracked anyway"
				// We can check whether the action was tracked by verifying the action cache's last action is populated
				if ac.last != "" {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tkeys cache must not be updated"
				if len(kc) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\telement must not have been removed from elements available for insertion"
				if len(available.pool) == len(theFellowship) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				// The local cache keeping track of the contents of the remote map hasn't been updated (meaning it
				// will still contain zero elements), hence 'determineNextMapAction' will return insert as next action
				// even if it was also the last, because insert is the only action that makes sense on an empty cache
				msg = "\t\t\tinsert must have been retried"
				if ms.m.setInvocations == 3 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tindex tracking number of inserted elements must not have been increased"
				if ic.current == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen read fails after successful inserts")
			{
				operationChainLength := len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					&sleepConfig{true, 1_000, false},
					1.0,
					0.0,
					1.0,
					operationChainLength,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{
					returnErrorUponGet: true,
				})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				ts := &testSleeper{}
				tl.s = ts

				ac := &actionCache{}
				ic := &indexCache{}
				kc := make(map[string]struct{})
				go tl.gatherer.Listen(make(chan struct{}, 1))

				mapName := "awesome-map"
				mapNumber := uint16(12)

				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   populateElementsAvailableForInsertion(mapName, mapNumber, theFellowship),
				}
				_ = tl.runOperationChain(3, ms.m, &modeCache{}, ac, ic, mapName, mapNumber, kc, available)
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

				msg := "\t\t\toperation chain must commence"
				/*
					We can verify the operation chain continued by checking the number of inserts on the map store.

					For a chain length of 9 (as set up above), we expect 5 set invocations (each set is followed by a read,
					hence 9 steps will yield 5 sets and 4 reads).
				*/
				numExpectedInserts := 5
				if ms.m.setInvocations == numExpectedInserts {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d invocations", operationChainLength, ms.m.setInvocations))
				}

				msg = "\t\t\treads must have been retried"
				if ms.m.getInvocations == 4 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tcache must have been updated after each successful insert"
				if len(kc) == numExpectedInserts {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tstatus tracker must have been informed about failed read attempts"
				statusCopy := tl.gatherer.AssembleStatusCopy()
				failedReadAttempts := statusCopy[string(statusKeyNumFailedReads)].(uint64)
				if failedReadAttempts == uint64(ms.m.getInvocations) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\telement index keeping track of inserted elements must have been moved forward"
				if ic.current == uint32(numExpectedInserts) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen read and remove operations fail after successful inserts")
			{
				operationChainLength := 4 * len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(
					rpOneMapOneRunNoEvictionScDisabled,
					sleepConfigDisabled,
					&sleepConfig{true, 1_000, false},
					1.0,
					0.0,
					1.0,
					operationChainLength,
					true,
				)
				ms := assembleTestMapStore(&testMapStoreBehavior{
					returnErrorUponGet:    true,
					returnErrorUponRemove: true,
				})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				ts := &testSleeper{}
				tl.s = ts

				ac := &actionCache{}
				ic := &indexCache{}
				kc := make(map[string]struct{})
				go tl.gatherer.Listen(make(chan struct{}, 1))

				availableForInsertion := make(map[string]struct{}, len(theFellowship))
				for i := 0; i < len(theFellowship); i++ {
					element := theFellowship[i]
					key := assembleMapKey(defaultTestMapName, defaultTestMapNumber, element)
					availableForInsertion[key] = struct{}{}
				}

				available := &availableElementsWrapper{
					maxNum: uint32(len(theFellowship)),
					pool:   availableForInsertion,
				}

				_ = tl.runOperationChain(3, ms.m, &modeCache{}, ac, ic, defaultTestMapName, defaultTestMapNumber, kc, available)
				tl.gatherer.StopListen()

				waitForStatusGatheringDone(tl.gatherer)

				msg := "\t\t\tremoves must have been retried"
				if ms.m.removeInvocations == operationChainLength/4 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.m.removeInvocations)
				}

				msg = "\t\t\treads must have been retried"
				if ms.m.getInvocations == operationChainLength/2 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ms.m.getInvocations)
				}

				msg = "\t\t\tno element must have been removed from cache"
				if len(kc) == ms.m.setInvocations {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, len(kc))
				}

				msg = "\t\t\tno elements must have been inserted into store representing elements available for insertion"
				if len(availableForInsertion) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, len(availableForInsertion))
				}

				msg = "\t\t\tindex tracking element insertion must be equal to amount of inserted elements"
				if ic.current == uint32(ms.m.setInvocations) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, ic.current)
				}
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
				tl := assembleBatchTestLoop(id, testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
			tl := assembleBatchTestLoop(id, testSource, true, &testHzClientHandler{}, ms, rc)

			go tl.gatherer.Listen(make(chan struct{}, 1))
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
				scAfterActionBatch := &sleepConfig{}
				rc := assembleRunnerConfigForBatchTestLoop(
					&runnerProperties{
						numMaps:             1,
						numRuns:             20,
						cleanMapsPriorToRun: false,
						sleepBetweenRuns:    scBetweenRuns,
					},
					sleepConfigDisabled,
					scAfterActionBatch,
				)
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, assembleTestMapStore(&testMapStoreBehavior{}), rc)

				numInvocationsBetweenRuns := 0
				numInvocationsAfterActionBatch := 0
				sleepTimeFunc = func(sc *sleepConfig) int {
					if sc == scBetweenRuns {
						numInvocationsBetweenRuns++
					} else if sc == scAfterActionBatch {
						numInvocationsAfterActionBatch++
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
				if numInvocationsAfterActionBatch == 0 {
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, assembleTestMapStore(&testMapStoreBehavior{}), rc)

				numInvocationsBetweenRuns := uint32(0)
				numInvocationsAfterActionBatch := uint32(0)
				sleepTimeFunc = func(sc *sleepConfig) int {
					if sc == scBetweenRuns {
						numInvocationsBetweenRuns++
					} else if sc == scBetweenActionsBatches {
						numInvocationsAfterActionBatch++
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

				msg = "\t\tnumber of sleeps after action batch must three times the number of runs"
				if numInvocationsAfterActionBatch == 3*numRuns {
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
				populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedInserts, 1, eq); ok {
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedKeyChecks, 1, eq); ok {
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
				s := &testSleeper{}
				tl.s = s

				go tl.gatherer.Listen(make(chan struct{}, 1))
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
				tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.readAll(ms.m, defaultTestMapName, defaultTestMapNumber)
			tl.gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			msg := "\t\tstatus gatherer must indicate zero failed operations"
			statusCopy := tl.gatherer.AssembleStatusCopy()
			for _, v := range counters {
				if ok, detail := expectedCounterValuePresent(statusCopy, v, 0, eq); ok {
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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.readAll(ms.m, testMapBaseName, 0)
			tl.gatherer.StopListen()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			msg = "\t\tstatus gatherer must have been informed about at least one failed read"
			if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedReads, 1, gte); ok {
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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			ms.m.data.Store(assembleMapKey("awesome-map", 0, "legolas"), nil)

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.readAll(ms.m, testMapBaseName, 0)
			tl.gatherer.StopListen()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			statusCopy := tl.gatherer.AssembleStatusCopy()

			msg = "\t\tstatus gatherer must indicate zero failed reads"
			if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumFailedReads, 0, eq); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tstatus gatherer must indicate at least one nil read"
			if ok, detail := expectedCounterValuePresent(statusCopy, statusKeyNumNilReads, 1, gte); ok {
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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

			s := &testSleeper{}
			tl.s = s

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.readAll(ms.m, defaultTestMapName, defaultTestMapNumber)
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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)

			populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.removeSome(ms.m, defaultTestMapName, defaultTestMapNumber)
			tl.gatherer.StopListen()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			waitForStatusGatheringDone(tl.gatherer)

			msg = "\t\tstatus gatherer must have been informed about at least one failed remove operation"
			// Since a failure to remove an element isn't treated as being severe enough to cancel the entire remove
			// loop and there is an element of randomness in the selection of how many elements should be removed,
			// it's not possible to tell exactly how many failed attempts we'll record -- we just know it has to be
			// at least one.
			if ok, detail := expectedCounterValuePresent(tl.gatherer.AssembleStatusCopy(), statusKeyNumFailedRemoves, 1, gte); ok {
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
			tl := assembleBatchTestLoop(uuid.New(), testSource, true, &testHzClientHandler{}, ms, rc)
			populateTestHzMapStore(defaultTestMapName, defaultTestMapNumber, &ms)

			s := &testSleeper{}
			tl.s = s

			go tl.gatherer.Listen(make(chan struct{}, 1))
			err := tl.removeSome(ms.m, defaultTestMapName, defaultTestMapNumber)
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

type comparisonMode string

const eq comparisonMode = "equals"
const gte comparisonMode = "greaterThanOrEqualTo"

func expectedCounterValuePresent(statusCopy map[string]any, expectedKey statusKey, expectedValue uint64, cMode comparisonMode) (bool, string) {

	recordedValue := statusCopy[string(expectedKey)].(uint64)

	switch cMode {
	case eq:
		if recordedValue == expectedValue {
			return true, ""
		}
		return false, fmt.Sprintf("expected %d, got %d", expectedValue, recordedValue)
	case gte:
		if recordedValue >= expectedValue {
			return true, ""
		}
		return false, fmt.Sprintf("expected value to be greater than or equal to %d, got %d", expectedValue, recordedValue)
	default:
		return false, fmt.Sprintf("test setup error: no such comparison mode: %s", cMode)
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

func assembleBoundaryTestLoopForBenchmark(id uuid.UUID, source string, numElements int, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore, rc *runnerConfig) boundaryTestLoop[string] {

	elements := make([]string, numElements)

	for i := 0; i < numElements; i++ {
		elements[i] = fmt.Sprintf("%d", i)
	}

	tle := assembleTestLoopExecution(id, source, elements, true, rc, ch, ms)
	tl := boundaryTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl

}

func assembleBoundaryTestLoop(id uuid.UUID, source string, usePreInitializedElements bool, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore, rc *runnerConfig) boundaryTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, theFellowship, usePreInitializedElements, rc, ch, ms)
	tl := boundaryTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl
}

func assembleBatchTestLoop(id uuid.UUID, source string, usePreInitializedElements bool, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore, rc *runnerConfig) batchTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, theFellowship, usePreInitializedElements, rc, ch, ms)
	tl := batchTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl

}

func assembleTestLoopExecution(id uuid.UUID, source string, elements []string, usePreInitializedElements bool, rc *runnerConfig, ch hazelcastwrapper.HzClientHandler, ms hazelcastwrapper.MapStore) testLoopExecution[string] {

	return testLoopExecution[string]{
		id:                        id,
		source:                    source,
		hzClientHandler:           ch,
		hzMapStore:                ms,
		runnerConfig:              rc,
		elements:                  elements,
		usePreInitializedElements: usePreInitializedElements,
		ctx:                       nil,
		getElementID:              fellowshipMemberName,
		getOrAssemblePayload:      returnFellowshipMemberName,
	}

}

func returnFellowshipMemberName(_ string, _ uint16, element string) (*loadsupport.PayloadWrapper, error) {
	getOrAssembleObservations.numInvocations++

	if getOrAssembleBehavior.returnError {
		return nil, getOrAssemblePayloadError
	}
	return &loadsupport.PayloadWrapper{Payload: []byte(element)}, nil
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
	sleepAfterActionBatch *sleepConfig,
) *runnerConfig {

	c := assembleBaseRunnerConfig(rp)
	c.batch = &batchTestLoopConfig{sleepAfterChainAction, sleepAfterActionBatch}

	return c

}

func fellowshipMemberName(element any) string {

	return element.(string)

}

func populateElementsAvailableForInsertion(mapName string, mapNumber uint16, sourceData []string) map[string]struct{} {

	result := make(map[string]struct{}, len(sourceData))

	for i := 0; i < len(sourceData); i++ {
		element := sourceData[i]
		key := assembleMapKey(mapName, mapNumber, element)
		result[key] = struct{}{}
	}

	return result

}

func populateTestHzMapStore(mapName string, mapNumber uint16, ms *testHzMapStore) {

	// Store all elements from test data source in map
	for _, value := range theFellowship {
		key := assembleMapKey(mapName, mapNumber, value)

		ms.m.data.Store(key, value)
	}

}

func assembleBaseRunnerConfig(rp *runnerProperties) *runnerConfig {

	return &runnerConfig{
		enabled:                 true,
		numMaps:                 rp.numMaps,
		numRuns:                 rp.numRuns,
		numEntriesPerMap:        rp.numEntriesPerMap,
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
