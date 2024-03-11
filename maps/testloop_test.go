package maps

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/client"
	"hazeltest/status"
	"math"
	"strings"
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

func fellowshipMemberName(element any) string {

	return element.(string)

}

func populateDummyHzMapStore(ms *dummyHzMapStore) {

	mapNumber := uint16(0)
	// Store all elements from dummy data source in map
	for _, value := range theFellowship {
		key := assembleMapKey(mapNumber, value)

		ms.m.data.Store(key, value)
	}

}

func TestMapTestLoopCountersTrackerInit(t *testing.T) {

	t.Log("given the tracker's init function")
	{
		t.Log("\twhen init method is invoked")
		{
			st := &mapTestLoopCountersTracker{}
			g := status.NewGatherer()

			go g.Listen()
			st.init(g)
			g.StopListen()

			msg := "\t\tgatherer must have been assigned"
			if st.gatherer == g {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tall status keys must have been inserted into status record"
			initialCounterValue := 0
			for _, v := range counters {
				if counter, ok := st.counters[v]; ok && counter == initialCounterValue {
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
		st := &mapTestLoopCountersTracker{
			counters: make(map[statusKey]int),
			l:        sync.Mutex{},
			gatherer: status.NewGatherer(),
		}

		for _, v := range counters {
			t.Log(fmt.Sprintf("\twhen initial value is zero for counter '%s' and increase method is invoked", v))
			{
				st.counters[v] = 0
				st.increaseCounter(v)

				msg := "\t\tcounter increase must be reflected in counter tracker's state"
				if st.counters[v] == 1 {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}

				msg = "\t\tcorresponding update must have been sent to status gatherer"
				update := <-st.gatherer.Updates
				if update.Key == string(v) && update.Value == 1 {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}

			}
		}

		t.Log("\twhen multiple goroutines want to increase a counter")
		{
			wg := sync.WaitGroup{}
			st := &mapTestLoopCountersTracker{
				counters: make(map[statusKey]int),
				l:        sync.Mutex{},
				gatherer: status.NewGatherer(),
			}
			go st.gatherer.Listen()
			st.counters[statusKeyNumFailedInserts] = 0
			numInvokingGoroutines := 100
			for i := 0; i < numInvokingGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					st.increaseCounter(statusKeyNumFailedInserts)
				}()
			}
			wg.Wait()
			st.gatherer.StopListen()

			msg := "\t\tfinal counter value must be equal to number of invoking goroutines"

			if st.counters[statusKeyNumFailedInserts] == numInvokingGoroutines {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestPopulateLocalCache(t *testing.T) {

	t.Log("given a method to populate the boundary test loop's local key cache")
	{
		mapName := "ht_" + mapBaseName
		t.Log("\twhen retrieving the key set is successful")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
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
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})

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
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponGetKeySet: true})

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

func TestChooseRandomElementFromSourceData(t *testing.T) {

	t.Log("given a populated source data as part of the test loop execution's state")
	{
		t.Log("\twhen caller desires random element from source data")
		{
			tle := testLoopExecution[pokemon]{
				elements: []pokemon{
					{Name: "Charmander"},
				},
			}
			tl := boundaryTestLoop[pokemon]{
				execution: &tle,
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
					execution: &testLoopExecution[string]{
						elements: theFellowship,
						getElementIdFunc: func(element any) string {
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
					execution: &testLoopExecution[string]{
						elements: []string{
							theFellowship[0],
						},
						getElementIdFunc: func(element any) string {
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
						execution: &testLoopExecution[string]{
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
						execution: &testLoopExecution[string]{
							elements: []string{
								elementInSourceData,
							},
							getElementIdFunc: func(element any) string {
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
						execution: &testLoopExecution[string]{
							elements: theFellowship,
							getElementIdFunc: func(element any) string {
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
				execution: &testLoopExecution[string]{
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

func TestQueryRemoteMapKeys(t *testing.T) {

	t.Log("given a function to query a remote hazelcast map for keys")
	{
		t.Log("\twhen remote map contains entries matching predicate")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})

			// Insert elements matching own client ID and map number predicate --> Result must contain these
			populateDummyHzMapStore(&ms)

			// Insert some elements matching own client ID, but not own map number --> Must not be contained in result
			elementFromSourceData := theFellowship[0]
			otherMapNumber := 42
			ms.m.data.Store(fmt.Sprintf("%s-%d-%s", client.ID(), otherMapNumber, elementFromSourceData), elementFromSourceData)

			// Insert elements matching own map number, but not own client ID --> Must be not contained in result
			otherClientID := uuid.New()
			ownMapNumber := uint16(0)
			ms.m.data.Store(fmt.Sprintf("%s-%d-%s", otherClientID, ownMapNumber, elementFromSourceData), elementFromSourceData)

			// Insert elements matching neither client ID nor map number --> Must not be contained in result
			ms.m.data.Store(fmt.Sprintf("%s-%d-%s", otherClientID, otherMapNumber, elementFromSourceData), elementFromSourceData)

			queriedKeys, err := queryRemoteMapKeys(context.TODO(), ms.m, "", ownMapNumber)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tnumber of queried keys must be equal to length of source data"
			if len(queriedKeys) == len(theFellowship) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d elements, got %d", len(theFellowship), len(queriedKeys)))
			}

			msg = "\t\tqueried keys map must contain only elements matching own client ID and map number"
			containsOnlyMatchingKeys := true
			for k := range queriedKeys {
				if !strings.HasPrefix(k, fmt.Sprintf("%s-%d", client.ID(), ownMapNumber)) {
					containsOnlyMatchingKeys = false
				}
			}

			if containsOnlyMatchingKeys {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen remote map does not contain entries matching predicate")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})

			ms.m.data.Store("dumbledore", "wrong franchise, mate")

			queriedKeys, err := queryRemoteMapKeys(context.TODO(), ms.m, "", 0)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tset of queried keys must be empty"
			if len(queriedKeys) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 keys, got %d", len(queriedKeys)))
			}
		}

		t.Log("\twhen querying key set yields error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{
				returnErrorUponGetKeySet: true,
			})

			queriedKeys, err := queryRemoteMapKeys(context.TODO(), ms.m, "", 0)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tset of queried keys must be empty"
			if len(queriedKeys) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected 0 keys, got %d", len(queriedKeys)))
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
			rc := assembleRunnerConfigForBatchTestLoop(1, 1, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
			go tl.gatherer.Listen()
			runWrapper(tl.execution, tl.gatherer, func(config *runnerConfig, u uint16) string {
				return "banana"
			}, func(h hzMap, s string, u uint16) {
				// No-op
			})
			tl.gatherer.StopListen()

			waitForStatusGatheringDone(tl.gatherer)

			msg := "\t\tstatus gatherer must contain initial state: %s"
			for _, v := range []statusKey{statusKeyNumFailedInserts, statusKeyNumFailedReads, statusKeyNumFailedRemoves, statusKeyNumNilReads} {
				if ok, detail := expectedStatusPresent(tl.gatherer.AssembleStatusCopy(), v, 0); ok {
					t.Log(fmt.Sprintf(msg, v), checkMark)
				} else {
					t.Fatal(fmt.Sprintf(msg, v), ballotX, detail)
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
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				action := insert

				mapNumber := 0
				mapName := fmt.Sprintf("%s-%s-%d", rc.mapPrefix, rc.mapBaseName, mapNumber)
				statusRecord := map[statusKey]any{
					statusKeyNumFailedInserts: 0,
				}
				err := tl.executeMapAction(ms.m, mapName, uint16(mapNumber), theFellowship[0], action)

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

				msg = "\t\t\tstatus record must indicate zero failed set operations"
				expected := 0
				actual := statusRecord[statusKeyNumFailedInserts].(int)
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}
			}
			t.Log("\t\twhen target map does not contain key yet and set yields error")
			{
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponSet: true})
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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

			}

			t.Log("\t\twhen target map already contains key")
			{
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
				action := insert

				mapNumber := uint16(0)
				populateDummyHzMapStore(&ms)
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

			}
		}
		t.Log("\twhen next action is remove")
		{
			t.Log("\t\twhen target map does not contain key")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

				statusRecord := map[statusKey]any{
					statusKeyNumFailedRemoves: 0,
				}
				err := tl.executeMapAction(ms.m, "my-map-name", uint16(0), theFellowship[0], remove)

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

				msg = "\t\t\tstatus record must indicate zero failed remove attempts"
				expected := 0
				actual := statusRecord[statusKeyNumFailedRemoves].(int)
				if expected == actual {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d", expected, actual))
				}
			}

			t.Log("\t\twhen target map contains key and remove yields error")
			{
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponRemove: true})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

				populateDummyHzMapStore(&ms)

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
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

				populateDummyHzMapStore(&ms)

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
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponGet: true})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

				populateDummyHzMapStore(&ms)

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
				rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

				populateDummyHzMapStore(&ms)

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

			}
		}
		t.Log("\twhen unknown action is provided")
		{
			rc := assembleRunnerConfigForBoundaryTestLoop(uint16(1), uint32(1), sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 0.5, 42, true)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)
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

func expectedStatusPresent(statusCopy map[string]any, expectedKey statusKey, expectedValue int) (bool, string) {

	recordedValue := statusCopy[string(expectedKey)].(int)

	if recordedValue == expectedValue {
		return true, ""
	} else {
		return false, fmt.Sprintf("expected %d, got %d\n", expectedValue, recordedValue)
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
				execution: &testLoopExecution[string]{
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
						numMaps,
						numRuns,
						sleepConfigDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0,
						0.0,
						1.0,
						// Set operation chain length to length of source data for this set of tests
						len(theFellowship),
						true,
					)
					ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

					tl.run()

					msg := "\t\t\t\tall elements must be inserted in map"
					if ms.m.setInvocations == len(theFellowship) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, ms.m.setInvocations)
					}

					msg = "\t\t\t\tremote map must have been queried for keys exactly once"
					if ms.m.getKeySetInvocations == 1 {
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
						sleepConfigDisabled,
						1.0, 0.0,
						0.0,
						len(theFellowship),
						true,
					)
					ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

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
						numMaps,
						numRuns,
						sleepConfigDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.5,
						chainLength,
						true,
					)
					ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(id, testSource, ms, rc)

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
						1,
						1,
						sleepConfigDisabled,
						sleepConfigDisabled,
						sleepConfigDisabled,
						1.0, 0.0,
						0.5,
						len(theFellowship)-1,
						false,
					)
					ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
					tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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

	t.Log("given a method to reset both local and remote state after execution of an operation chain has finished")
	{
		t.Log("\twhen state exists both locally and remotely")
		{
			t.Log("\t\twhen remove all on remote map does not yield error")
			{
				mapNumber := uint16(0)
				tl := boundaryTestLoop[string]{
					execution: &testLoopExecution[string]{
						ctx: context.TODO(),
					},
				}

				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				populateDummyHzMapStore(&ms)

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
					execution: &testLoopExecution[string]{
						ctx: context.TODO(),
					},
				}

				ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponRemoveAll: true})
				populateDummyHzMapStore(&ms)

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
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBoundaryTestLoop(1, 1, sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 1.0, 0, true)
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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

			msg = "\t\tmode cache must be in initial state"
			if mc.current == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tactions cache must be in initial state"
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
				ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
				chainLength := 10 * len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(1, 1, sleepConfigDisabled, sleepConfigDisabled, sleepConfigDisabled, 1.0, 0.0, 1.0, chainLength, true)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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
				ms := assembleDummyMapStoreWithBoundaryMonitoring(&dummyMapStoreBehavior{}, &boundaryMonitoring{
					upperBoundaryNumElements: int(math.Round(float64(len(theFellowship)) * upperBoundary)),
					lowerBoundaryNumElements: int(math.Round(float64(len(theFellowship)) * lowerBoundary)),
				})
				chainLength := 1_000 * len(theFellowship)
				rc := assembleRunnerConfigForBoundaryTestLoop(
					1,
					1,
					sleepConfigDisabled,
					sleepConfigDisabled,
					sleepConfigDisabled,
					float32(upperBoundary),
					float32(lowerBoundary),
					0.6,
					chainLength,
					true,
				)
				tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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
			rc := assembleRunnerConfigForBoundaryTestLoop(1, 1, sleepConfigDisabled, &sleepConfig{true, 1_000, false}, sleepConfigDisabled, 1.0, 0.0, 1.0, 1, true)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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
			rc := assembleRunnerConfigForBoundaryTestLoop(1, 1, sleepConfigDisabled, sleepConfigDisabled, &sleepConfig{true, 1_000, false}, 1.0, 0.0, 1.0, 1, true)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			tl := assembleBoundaryTestLoop(uuid.New(), testSource, ms, rc)

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

type testSleeper struct {
	sleepInvoked bool
}

func (s *testSleeper) sleep(_ *sleepConfig, _ evaluateTimeToSleep) {

	s.sleepInvoked = true

}

func TestRunWithBatchTestLoop(t *testing.T) {

	t.Log("given the maps batch test loop")
	{
		t.Log("\twhen only one map goroutine is used and the test loop runs only once")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, rc)

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
		}

		t.Log("\twhen multiple goroutines execute test loops")
		{
			numMaps, numRuns := uint16(10), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
		}

		t.Log("\twhen get map yields error")
		{
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponGetMap: true})
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
		}

		t.Log("\twhen only one run is executed an error is thrown during read all")
		{
			numMaps, numRuns := uint16(1), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponGet: true})
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
		}

		t.Log("\twhen no map goroutine is launched because the configured number of maps is zero")
		{
			id := uuid.New()
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			numMaps, numRuns := uint16(0), uint32(1)
			rc := assembleRunnerConfigForBatchTestLoop(numMaps, numRuns, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(id, testSource, ms, rc)

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
			scBetweenRuns := &sleepConfig{}
			scBetweenActionBatches := &sleepConfig{}
			rc := assembleRunnerConfigForBatchTestLoop(1, 20, scBetweenRuns, scBetweenActionBatches)
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(&dummyMapStoreBehavior{}), rc)

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
			tl := assembleBatchTestLoop(uuid.New(), testSource, assembleDummyMapStore(&dummyMapStoreBehavior{}), rc)

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

func TestIngestAll(t *testing.T) {

	t.Log("given a hazelcast map and elements for insertion")
	{
		t.Log("\twhen target map does not contain key yet and set does not yield error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				uint16(1),
				uint32(9),
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

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
			expected := len(tl.execution.elements)
			actual := ms.m.containsKeyInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tnumber of set invocations must be equal to number of elements in source data, too"
			actual = ms.m.setInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tstatus record must indicate there have been zero failed insert attempts"
			expected = 0
			actual = statusRecord[statusKeyNumFailedInserts].(int)
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}
		}

		t.Log("\twhen target map contains all keys")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(
				uint16(1),
				uint32(9),
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)
			populateDummyHzMapStore(&ms)

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
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tnumber of inserts must be zero"
			expected = 0
			actual = ms.m.setInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tstatus record must indicate zero failed inserts"
			actual = statusRecord[statusKeyNumFailedInserts].(int)
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}
		}

		t.Log("\twhen target map contains no keys and insert yields error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{
				returnErrorUponSet: true,
			})
			rc := assembleRunnerConfigForBatchTestLoop(
				uint16(1),
				uint32(9),
				sleepConfigDisabled,
				sleepConfigDisabled,
			)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedInserts: 0,
			}
			err := tl.ingestAll(ms.m, "awesome-map", uint16(0))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			expected := 1
			msg = fmt.Sprintf("\t\tnumber of contains key checks must be %d\n", expected)
			actual := ms.m.containsKeyInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = fmt.Sprintf("\t\tnumber of inserts must be %d\n", expected)
			actual = ms.m.setInvocations
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tstatus report must indicate there was one failed insert attempt"
			actual = statusRecord[statusKeyNumFailedInserts].(int)
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tcorresponding update must have been sent to status gatherer"
			update := <-tl.gatherer.Updates
			if update.Key == string(statusKeyNumFailedInserts) && update.Value == expected {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestReadAll(t *testing.T) {

	t.Log("given a hazelcast map")
	{
		t.Log("\twhen map contains all elements expected based on data source and no access operation fails")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(1, 12, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)
			populateDummyHzMapStore(&ms)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedReads: 0,
				statusKeyNumNilReads:    0,
			}
			err := tl.readAll(ms.m, mapBaseName, 0)

			msg := "\t\tstatus record must indicate so"
			for k, v := range statusRecord {
				if v == 0 {
					t.Log(msg, checkMark, k)
				} else {
					t.Fatal(msg, ballotX, k)
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
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}
		}

		t.Log("\twhen get invocation yields error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponGet: true})
			rc := assembleRunnerConfigForBatchTestLoop(1, 9, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedReads: 0,
			}
			err := tl.readAll(ms.m, mapBaseName, 0)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus record must indicate one failed read"
			expected := 1
			actual := statusRecord[statusKeyNumFailedReads].(int)
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tcorresponding update must have been sent to status g"
			update := <-tl.gatherer.Updates
			if update.Key == string(statusKeyNumFailedReads) && update.Value == expected {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen map contains elements and get yields no error, but retrieved value is nil")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(1, 9, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

			ms.m.data.Store(assembleMapKey(0, "legolas"), nil)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedReads: 0,
				statusKeyNumNilReads:    0,
			}

			err := tl.readAll(ms.m, mapBaseName, 0)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus record must indicate zero failed reads"
			expected := 0
			actual := statusRecord[statusKeyNumFailedReads].(int)

			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}

			msg = "\t\tstatus record must indicate one nil read"
			expected = 1
			actual = statusRecord[statusKeyNumNilReads].(int)
			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}
		}
	}
}

func TestRemoveSome(t *testing.T) {

	t.Log("given a hazelcast map containing state")
	{
		t.Log("\twhen remove operation does not yield error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{})
			rc := assembleRunnerConfigForBatchTestLoop(1, 9, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

			populateDummyHzMapStore(&ms)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedRemoves: 0,
			}
			err := tl.removeSome(ms.m, mapBaseName, uint16(0))

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
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
			}
		}

		t.Log("\twhen remove operation yields error")
		{
			ms := assembleDummyMapStore(&dummyMapStoreBehavior{returnErrorUponRemove: true})
			rc := assembleRunnerConfigForBatchTestLoop(1, 9, sleepConfigDisabled, sleepConfigDisabled)
			tl := assembleBatchTestLoop(uuid.New(), testSource, ms, rc)

			populateDummyHzMapStore(&ms)

			statusRecord := map[statusKey]any{
				statusKeyNumFailedRemoves: 0,
			}
			err := tl.removeSome(ms.m, mapBaseName, uint16(0))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus record must indicate one failed remove invocation"
			expected := 1
			actual := statusRecord[statusKeyNumFailedRemoves].(int)

			if expected == actual {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected %d, got %d\n", expected, actual))
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

func assembleBoundaryTestLoop(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) boundaryTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, rc, ms)
	tl := boundaryTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl
}

func assembleBatchTestLoop(id uuid.UUID, source string, ms hzMapStore, rc *runnerConfig) batchTestLoop[string] {

	tle := assembleTestLoopExecution(id, source, rc, ms)
	tl := batchTestLoop[string]{}
	tl.init(&tle, &defaultSleeper{}, status.NewGatherer())

	return tl

}

func assembleTestLoopExecution(id uuid.UUID, source string, rc *runnerConfig, ms hzMapStore) testLoopExecution[string] {

	return testLoopExecution[string]{
		id:               id,
		source:           source,
		mapStore:         ms,
		runnerConfig:     rc,
		elements:         theFellowship,
		ctx:              nil,
		getElementIdFunc: fellowshipMemberName,
	}

}

type dummyMapStoreBehavior struct {
	returnErrorUponGetMap, returnErrorUponGet, returnErrorUponSet, returnErrorUponContainsKey, returnErrorUponRemove, returnErrorUponGetKeySet, returnErrorUponRemoveAll bool
}

func assembleDummyMapStoreWithBoundaryMonitoring(b *dummyMapStoreBehavior, bm *boundaryMonitoring) dummyHzMapStore {

	ms := assembleDummyMapStore(b)
	ms.m.bm = bm

	return ms

}

func assembleDummyMapStore(b *dummyMapStoreBehavior) dummyHzMapStore {

	dummyBackend := &sync.Map{}

	return dummyHzMapStore{
		m: &dummyHzMap{
			data:                       dummyBackend,
			returnErrorUponGet:         b.returnErrorUponGet,
			returnErrorUponSet:         b.returnErrorUponSet,
			returnErrorUponContainsKey: b.returnErrorUponContainsKey,
			returnErrorUponRemove:      b.returnErrorUponRemove,
			returnErrorUponGetKeySet:   b.returnErrorUponGetKeySet,
			returnErrorUponRemoveAll:   b.returnErrorUponRemoveAll,
		},
		returnErrorUponGetMap: b.returnErrorUponGetMap,
	}

}

func assembleRunnerConfigForBoundaryTestLoop(
	numMaps uint16,
	numRuns uint32,
	sleepBetweenRuns *sleepConfig,
	sleepBetweenOperationChains *sleepConfig,
	sleepUponModeChange *sleepConfig,
	upperBoundaryMapFillPercentage, lowerBoundaryMapFillPercentage, actionTowardsBoundaryProbability float32,
	operationChainLength int,
	resetAfterChain bool,
) *runnerConfig {

	c := assembleBaseRunnerConfig(numMaps, numRuns, sleepBetweenRuns)
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
