package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/state"
	"hazeltest/status"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type (
	evaluateTimeToSleep      func(sc *sleepConfig) int
	getElementIdFunc         func(element any) string
	getOrAssemblePayloadFunc func(mapName string, mapNumber uint16, elementID string) (*string, error)
	looper[t any]            interface {
		init(lc *testLoopExecution[t], s sleeper, gatherer status.Gatherer)
		run()
	}
	counterTracker interface {
		init(gatherer status.Gatherer)
		increaseCounter(sk statusKey)
	}
	sleeper interface {
		sleep(sc *sleepConfig, sf evaluateTimeToSleep, runnerName string)
	}
	defaultSleeper struct{}
)

type (
	batchTestLoop[t any] struct {
		tle      *testLoopExecution[t]
		gatherer status.Gatherer
		ct       counterTracker
		s        sleeper
	}
	modeCache struct {
		current actionMode
		// TODO Test needs to verify force gets set back to false once it is no longer required
		forceActionTowardsMode bool
	}
	actionCache struct {
		last mapAction
		next mapAction
	}
	indexCache struct {
		current uint32
	}
	boundaryTestLoop[t any] struct {
		tle      *testLoopExecution[t]
		gatherer status.Gatherer
		s        sleeper
		ct       counterTracker
	}
	testLoopExecution[t any] struct {
		id                        uuid.UUID
		runnerName                string
		source                    string
		hzClientHandler           hazelcastwrapper.HzClientHandler
		hzMapStore                hazelcastwrapper.MapStore
		stateCleanerBuilder       state.SingleMapCleanerBuilder
		runnerConfig              *runnerConfig
		elements                  []t
		usePreInitializedElements bool
		ctx                       context.Context
		getElementID              getElementIdFunc
		getOrAssemblePayload      getOrAssemblePayloadFunc
	}
	mapTestLoopCountersTracker struct {
		counters map[statusKey]uint64
		l        sync.Mutex
		gatherer status.Gatherer
	}
	availableElementsWrapper struct {
		maxNum uint32
		pool   map[string]struct{}
	}
)

type (
	actionMode string
	mapAction  string
)

const (
	updateStep            uint32     = 50
	statusKeyNumMaps      statusKey  = "numMaps"
	statusKeyNumRuns      statusKey  = "numRuns"
	statusKeyTotalNumRuns statusKey  = "totalNumRuns"
	statusKeyFinished     statusKey  = "finished"
	fill                  actionMode = "fill"
	drain                 actionMode = "drain"
	insert                mapAction  = "insert"
	remove                mapAction  = "remove"
	read                  mapAction  = "read"
	// Special action introduced to represent cases where
	// no insert should be executed (e.g. because the action
	// probability was set to zero percent), but the other actions
	// would not make sense and therefore should not be carried
	// out, either (e.g. target map is empty, so cannot remove or
	// read anything)
	noop mapAction = "noop"
)

const (
	statusKeyNumFailedInserts   statusKey = "numFailedInserts"
	statusKeyNumFailedReads     statusKey = "numFailedReads"
	statusKeyNumNilReads        statusKey = "numNilReads"
	statusKeyNumFailedRemoves   statusKey = "numFailedRemoves"
	statusKeyNumFailedKeyChecks statusKey = "numFailedKeyChecks"
)

var (
	sleepTimeFunc evaluateTimeToSleep = func(sc *sleepConfig) int {
		var sleepDuration int
		if sc.enableRandomness {
			sleepDuration = rand.Intn(sc.durationMs + 1)
		} else {
			sleepDuration = sc.durationMs
		}
		return sleepDuration
	}
	counters = []statusKey{statusKeyNumFailedInserts, statusKeyNumFailedReads, statusKeyNumNilReads, statusKeyNumFailedRemoves, statusKeyNumFailedKeyChecks}
)

func (ct *mapTestLoopCountersTracker) init(gatherer status.Gatherer) {
	ct.gatherer = gatherer

	ct.counters = make(map[statusKey]uint64)

	initialCounterValue := uint64(0)
	for _, v := range counters {
		ct.counters[v] = initialCounterValue
		gatherer.Gather(status.Update{Key: string(v), Value: initialCounterValue})
	}
}

func (ct *mapTestLoopCountersTracker) increaseCounter(sk statusKey) {

	var newValue uint64
	ct.l.Lock()
	{
		newValue = ct.counters[sk] + 1
		ct.counters[sk] = newValue
	}
	ct.l.Unlock()

	ct.gatherer.Gather(status.Update{Key: string(sk), Value: newValue})

}

func (l *boundaryTestLoop[t]) init(tle *testLoopExecution[t], s sleeper, gatherer status.Gatherer) {
	l.tle = tle
	l.s = s
	l.gatherer = gatherer

	ct := &mapTestLoopCountersTracker{}
	ct.init(gatherer)

	l.ct = ct
}

func (l *boundaryTestLoop[t]) run() {

	runWrapper(
		l.tle,
		l.gatherer,
		assembleMapName,
		l.runForMap,
	)

}

func evaluateNextMapElementIndex(runnerName string, action mapAction, cursor uint32) (string, error) {

	switch action {
	case insert:
		return strconv.Itoa(int(cursor)), nil
	case read, remove:
		return strconv.Itoa(int(cursor - 1)), nil
	default:
		msg := fmt.Sprintf("no such map action: %s", action)
		lp.LogMapRunnerEvent(msg, runnerName, log.ErrorLevel)
		return "", errors.New(msg)
	}

}

func (l *boundaryTestLoop[t]) chooseNextMapElementKey(action mapAction, elementsInserted, elementsAvailableForInsertion map[string]struct{}) (string, error) {

	switch action {
	case insert:
		// Maps do not preserve order in Go, but that's totally fine -- just use whatever element from those available
		// for insertion happens to be the first one right now
		for k := range elementsAvailableForInsertion {
			return k, nil
		}
		// Getting to this point means that the 'insert' action was chosen elsewhere despite the fact
		// that all elements have already been stored in cache. This case should not occur,
		// but when it does nonetheless, it is not sufficiently severe to report an error
		// and abort execution. So, in this case, we simply choose an element from the source data randomly.
		lp.LogMapRunnerEvent("cache already contains all elements of data source, so cannot pick element not yet contained -- using first element from runner source data", l.tle.runnerName, log.WarnLevel)
		return l.tle.getElementID(l.tle.elements[0]), nil
	case read, remove:
		if len(elementsInserted) == 0 {
			return "", errors.New("no elements have been previously inserted, so cannot choose element to read or remove")
		}
		// Similar to above, the fact that maps do not preserve order doesn't concern us
		for k := range elementsInserted {
			return k, nil
		}
		// This case cannot occur
		return "", nil
	default:
		msg := fmt.Sprintf("no such map action: %s", action)
		lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
		return "", errors.New(msg)
	}

}

func assemblePredicate(clientID uuid.UUID, mapName string, mapNumber uint16) predicate.Predicate {

	return predicate.SQL(fmt.Sprintf("__key like %s-%s-%d%%", clientID, mapName, mapNumber))

}

func (l *boundaryTestLoop[t]) runForMap(m hazelcastwrapper.Map, mapName string, mapNumber uint16) {

	sleepBetweenRunsConfig := l.tle.runnerConfig.sleepBetweenRuns

	mc := &modeCache{}
	ac := &actionCache{}
	ic := &indexCache{}
	elementsInserted := make(map[string]struct{})

	availableElements := &availableElementsWrapper{
		maxNum: l.tle.runnerConfig.numEntriesPerMap,
		pool:   l.populateElementsAvailableForInsertion(),
	}

	for i := uint32(0); i < l.tle.runnerConfig.numRuns; i++ {

		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc, l.tle.runnerName)

		if i > 0 && i%updateStep == 0 {
			lp.LogMapRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.tle.runnerConfig.numRuns, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)
		}

		if err := l.runOperationChain(i, m, mc, ac, ic, mapName, mapNumber, elementsInserted, availableElements); err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("running operation chain unsuccessful in map run %d on map '%s' in goroutine %d -- retrying in next run", i, mapName, mapNumber), l.tle.runnerName, log.WarnLevel)
		} else {
			lp.LogMapRunnerEvent(fmt.Sprintf("successfully finished operation chain for map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), l.tle.runnerName, log.InfoLevel)
		}

		if l.tle.runnerConfig.boundary.resetAfterChain {
			lp.LogMapRunnerEvent(fmt.Sprintf("performing reset after operation chain on map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), l.tle.runnerName, log.InfoLevel)
			l.resetAfterOperationChain(m, mapName, mapNumber, &elementsInserted, availableElements, mc, ac, ic)
		}

	}

	lp.LogMapRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

}

func (l *boundaryTestLoop[t]) resetAfterOperationChain(m hazelcastwrapper.Map, mapName string, mapNumber uint16, elementsInserted *map[string]struct{}, availableElements *availableElementsWrapper, mc *modeCache, ac *actionCache, ic *indexCache) {

	lp.LogMapRunnerEvent(fmt.Sprintf("resetting mode and action cache for map '%s' on goroutine %d", mapName, mapNumber), l.tle.runnerName, log.TraceLevel)

	*mc = modeCache{}
	*ac = actionCache{}

	p := assemblePredicate(client.ID(), mapName, mapNumber)
	lp.LogMapRunnerEvent(fmt.Sprintf("removing all keys from map '%s' in goroutine %d having match for predicate '%s'", mapName, mapNumber, p), l.tle.runnerName, log.TraceLevel)
	err := m.RemoveAll(l.tle.ctx, p)
	if err != nil {
		lp.LogHzEvent(fmt.Sprintf("won't update local cache because removing all keys from map '%s' in goroutine %d having match for predicate '%s' failed due to error: '%s'", mapName, mapNumber, p, err.Error()), log.WarnLevel)
	} else {

		*ic = indexCache{}

		var pool map[string]struct{}
		if l.tle.usePreInitializedElements {
			pool = l.populateElementsAvailableForInsertion()
		} else {
			// TODO Verify map is empty when index-only mode was enabled
			pool = make(map[string]struct{})
		}
		*elementsInserted = make(map[string]struct{})
		*availableElements = availableElementsWrapper{
			maxNum: l.tle.runnerConfig.numEntriesPerMap,
			pool:   pool,
		}
	}

}

func evaluateMapFillBoundaries(bc *boundaryTestLoopConfig) (float32, float32) {

	upper := float32(0)
	if bc.upper.enableRandomness {
		upper = bc.upper.mapFillPercentage + rand.Float32()*(1-bc.upper.mapFillPercentage)
	} else {
		upper = bc.upper.mapFillPercentage
	}

	lower := float32(0)
	if bc.lower.enableRandomness {
		lower = rand.Float32() * bc.lower.mapFillPercentage
	} else {
		lower = bc.lower.mapFillPercentage
	}

	return upper, lower

}

func (l *boundaryTestLoop[t]) runOperationChain(
	currentRun uint32,
	m hazelcastwrapper.Map,
	modes *modeCache,
	actions *actionCache,
	index *indexCache,
	mapName string,
	mapNumber uint16,
	elementsInserted map[string]struct{},
	availableElements *availableElementsWrapper,
) error {

	chainLength := uint32(l.tle.runnerConfig.boundary.chainLength)

	lp.LogMapRunnerEvent(fmt.Sprintf("starting operation chain of length %d for map '%s' on goroutine %d", chainLength, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

	l.s.sleep(l.tle.runnerConfig.boundary.sleepBetweenOperationChains, sleepTimeFunc, l.tle.runnerName)

	upperBoundary, lowerBoundary := evaluateMapFillBoundaries(l.tle.runnerConfig.boundary)
	actionProbability := l.tle.runnerConfig.boundary.actionTowardsBoundaryProbability

	lp.LogMapRunnerEvent(fmt.Sprintf("using upper boundary %f and lower boundary %f for map '%s' on goroutine %d", upperBoundary, lowerBoundary, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

	for j := uint32(0); j < chainLength; j++ {

		if (actions.last == insert || actions.last == remove) && j > 0 && j%updateStep == 0 {
			lp.LogMapRunnerEvent(fmt.Sprintf("chain position %d of %d for map '%s' on goroutine %d", j, chainLength, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)
		}

		nextMode, forceActionTowardsMode := l.checkForModeChange(upperBoundary, lowerBoundary, index.current, availableElements.maxNum, modes.current)
		if nextMode != modes.current && modes.current != "" {
			lp.LogMapRunnerEvent(fmt.Sprintf("detected mode change from '%s' to '%s' for map '%s' in chain position %d with %d map items currently under management", modes.current, nextMode, mapName, j, index.current), l.tle.runnerName, log.InfoLevel)
			l.s.sleep(l.tle.runnerConfig.boundary.sleepUponModeChange, sleepTimeFunc, l.tle.runnerName)
		}
		modes.current, modes.forceActionTowardsMode = nextMode, forceActionTowardsMode

		actions.next = determineNextMapAction(modes, actions.last, actionProbability, index.current)

		lp.LogMapRunnerEvent(fmt.Sprintf("for map '%s' in goroutine %d, current mode is '%s', and next map action was determined to be '%s'", mapName, mapNumber, modes.current, actions.next), l.tle.runnerName, log.TraceLevel)
		if actions.next == noop {
			msg := fmt.Sprintf("encountered no-op case for map '%s' in goroutine %d in operation chain iteration %d -- assuming incorrect configuration, aborting", mapName, mapNumber, j)
			lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
			return errors.New(msg)
		}

		var nextMapElementKey string
		var err error
		if l.tle.usePreInitializedElements {
			nextMapElementKey, err = l.chooseNextMapElementKey(actions.next, elementsInserted, availableElements.pool)
		} else {
			nextMapElementKey, err = evaluateNextMapElementIndex(l.tle.runnerName, actions.next, index.current)
		}

		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("unable to choose next map element to work on for map '%s' due to error ('%s') -- aborting operation chain to try in next run", mapName, err.Error()), l.tle.runnerName, log.ErrorLevel)
			break
		}

		lp.LogMapRunnerEvent(fmt.Sprintf("successfully chose next map element for map '%s' in goroutine %d for map action '%s'", mapName, mapNumber, actions.next), l.tle.runnerName, log.TraceLevel)

		err = l.executeMapAction(m, mapName, mapNumber, nextMapElementKey, actions.next)
		actions.last = actions.next
		actions.next = ""

		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon execution of '%s' action on map '%s' in run '%d' (still moving to next loop iteration): %v", actions.last, mapName, currentRun, err), l.tle.runnerName, log.WarnLevel)
		} else {
			lp.LogMapRunnerEvent(fmt.Sprintf("action '%s' successfully executed on map '%s', moving to next action in upcoming loop iteration", actions.last, mapName), l.tle.runnerName, log.TraceLevel)
			if l.tle.usePreInitializedElements {
				updateKeysCache(nextMapElementKey, actions.last, elementsInserted, availableElements.pool, l.tle.runnerName)
			}
			if actions.last == insert {
				index.current++
			} else if actions.last == remove {
				index.current--
			}
		}

		l.s.sleep(l.tle.runnerConfig.boundary.sleepAfterChainAction, sleepTimeFunc, l.tle.runnerName)

	}

	return nil

}

func (l *boundaryTestLoop[t]) populateElementsAvailableForInsertion() map[string]struct{} {

	notYetInserted := make(map[string]struct{}, len(l.tle.elements))
	for _, v := range l.tle.elements {
		notYetInserted[l.tle.getElementID(v)] = struct{}{}
	}

	return notYetInserted

}

func updateKeysCache(elementID string, lastSuccessfulAction mapAction, elementsInserted, elementsAvailableForInsertion map[string]struct{}, runnerName string) {

	switch lastSuccessfulAction {
	case insert, remove:
		if lastSuccessfulAction == insert {
			elementsInserted[elementID] = struct{}{}
			delete(elementsAvailableForInsertion, elementID)
		} else {
			delete(elementsInserted, elementID)
			elementsAvailableForInsertion[elementID] = struct{}{}
		}
		lp.LogMapRunnerEvent(fmt.Sprintf("update on key cache successful for map action '%s', cache now containing %d element/-s", lastSuccessfulAction, len(elementsInserted)), runnerName, log.TraceLevel)
	default:
		lp.LogMapRunnerEvent(fmt.Sprintf("no action to perform on local cache for last successful action '%s'", lastSuccessfulAction), runnerName, log.TraceLevel)
	}

}

func (l *boundaryTestLoop[t]) executeMapAction(m hazelcastwrapper.Map, mapName string, mapNumber uint16, elementID string, action mapAction) error {

	key := assembleMapKey(mapName, mapNumber, elementID)

	switch action {
	case insert:
		payload, err := l.tle.getOrAssemblePayload(mapName, mapNumber, elementID)
		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("unable to execute insert operation for map '%s' due to error upon generating payload: %v", mapName, err), l.tle.runnerName, log.ErrorLevel)
			return err
		}
		if err := m.Set(l.tle.ctx, key, toValue(payload)); err != nil {
			l.ct.increaseCounter(statusKeyNumFailedInserts)
			lp.LogHzEvent(fmt.Sprintf("failed to insert key '%s' into map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogHzEvent(fmt.Sprintf("successfully inserted key '%s' into map '%s'", key, mapName), log.TraceLevel)
			return nil
		}
	case remove:
		if _, err := m.Remove(l.tle.ctx, key); err != nil {
			l.ct.increaseCounter(statusKeyNumFailedRemoves)
			lp.LogHzEvent(fmt.Sprintf("failed to remove key '%s' from map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogHzEvent(fmt.Sprintf("successfully removed key '%s' from map '%s'", key, mapName), log.TraceLevel)
			return nil
		}
	case read:
		if v, err := m.Get(l.tle.ctx, key); err != nil {
			l.ct.increaseCounter(statusKeyNumFailedReads)
			lp.LogHzEvent(fmt.Sprintf("read for key '%s' failed for map '%s'", key, mapName), log.WarnLevel)
			return err
		} else if v == nil {
			l.ct.increaseCounter(statusKeyNumNilReads)
			return fmt.Errorf("read for key '%s' successful for map '%s', but associated value was nil", key, mapName)
		} else {
			lp.LogHzEvent(fmt.Sprintf("successfully read key '%s' in map '%s'", key, mapName), log.TraceLevel)
			return nil
		}

	}

	return fmt.Errorf("unknown map action: %s", action)

}

func determineNextMapAction(mc *modeCache, lastAction mapAction, actionProbability float32, currentCacheSize uint32) mapAction {

	if currentCacheSize == 0 {
		if actionProbability > 0 {
			// If an action is desired, no action but an insert would make sense on an empty cache
			return insert
		} else {
			// Case when cache is empty (e.g. initial state), but no action towards the boundary is desired
			// (a runner thus configured can only ever execute no-ops, so this wouldn't make much sense config-wise,
			// but this case still needs to be addressed)
			return noop
		}
	}

	if lastAction == insert || lastAction == remove {
		return read
	}

	var hit bool
	if mc.forceActionTowardsMode {
		// Prevents violating threshold if map has reached threshold and hit happens to be false,
		// so next action would cross threshold
		// Example: With 150 elements in total and upper boundary of 80 %, threshold would be 120
		// elements. If 120 elements have been inserted and mode was correctly switched to "drain",
		// switch below could still return "insert" as next map action in cases when action probability
		// is less than 100 %
		hit = true
	} else {
		hit = rand.Float32() < actionProbability
	}

	switch mc.current {
	case fill:
		if hit {
			return insert
		}
		return remove
	case drain:
		if hit {
			return remove
		}
		return insert
	default:
		return lastAction
	}

}

func (l *boundaryTestLoop[t]) checkForModeChange(upperBoundary, lowerBoundary float32, currentIndex, maxNumElements uint32, currentMode actionMode) (actionMode, bool) {

	if currentIndex == 0 || currentMode == "" {
		return fill, false
	}

	currentNumElements := float64(currentIndex)

	if currentNumElements <= math.Round(float64(maxNumElements)*float64(lowerBoundary)) {
		lp.LogMapRunnerEvent(fmt.Sprintf("enforcing 'fill' mode -- current number of elements: %d; total number of elements: %d", currentIndex, len(l.tle.elements)), l.tle.runnerName, log.TraceLevel)
		return fill, true
	}

	if currentNumElements >= math.Round(float64(maxNumElements)*float64(upperBoundary)) {
		lp.LogMapRunnerEvent(fmt.Sprintf("enforcing 'drain' mode -- current number of elements: %d; total number of elements: %d", currentIndex, len(l.tle.elements)), l.tle.runnerName, log.TraceLevel)
		return drain, true
	}

	return currentMode, false

}

func (l *batchTestLoop[t]) init(tle *testLoopExecution[t], s sleeper, gatherer status.Gatherer) {
	l.tle = tle
	l.s = s
	l.gatherer = gatherer

	ct := &mapTestLoopCountersTracker{}
	ct.init(gatherer)

	l.ct = ct
}

func runWrapper[t any](tle *testLoopExecution[t],
	gatherer status.Gatherer,
	assembleMapNameFunc func(*runnerConfig, uint16) string,
	runFunc func(hazelcastwrapper.Map, string, uint16)) {

	rc := tle.runnerConfig
	insertInitialTestLoopStatus(gatherer, rc.numMaps, rc.numRuns)

	var stateCleaner state.SingleCleaner
	var hzService string
	if tle.runnerConfig.preRunClean.enabled {
		// TODO Add information collected by tracker to test loop status
		// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/70
		bv := &state.SingleMapCleanerBuildValues{
			Ctx: tle.ctx,
			Ms:  tle.hzMapStore,
			Tr:  &state.CleanedDataStructureTracker{G: gatherer},
			Cih: &state.DefaultLastCleanedInfoHandler{
				Ctx: tle.ctx,
				Ms:  tle.hzMapStore,
				Cfg: &state.LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: tle.runnerConfig.preRunClean.applyCleanAgainThreshold,
					CleanAgainThresholdMs:  tle.runnerConfig.preRunClean.cleanAgainThresholdMs,
				},
			},
			Cm: rc.preRunClean.cleanMode,
		}
		stateCleaner, hzService = tle.stateCleanerBuilder.Build(bv)
	}

	var wg sync.WaitGroup
	for i := uint16(0); i < rc.numMaps; i++ {
		wg.Add(1)
		go func(i uint16) {
			defer wg.Done()
			mapName := assembleMapNameFunc(tle.runnerConfig, i)
			lp.LogMapRunnerEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), tle.runnerName, log.InfoLevel)
			start := time.Now()
			m, err := tle.hzMapStore.GetMap(tle.ctx, mapName)
			if err != nil {
				lp.LogHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err), log.ErrorLevel)
				return
			}
			defer func() {
				_ = m.Destroy(tle.ctx)
			}()
			elapsed := time.Since(start).Milliseconds()
			lp.LogTimingEvent("getMap()", mapName, int(elapsed), log.InfoLevel)
			if tle.runnerConfig.preRunClean.enabled {
				if stateCleaner == nil || hzService == "" {
					lp.LogMapRunnerEvent("pre-run map eviction enabled, but encountered uninitialized state cleaner -- won't start test run for this map", tle.runnerName, log.ErrorLevel)
					return
				}
				lp.LogMapRunnerEvent(fmt.Sprintf("invoking single map cleaner for map '%s' using clean mode '%s'", mapName, tle.runnerConfig.preRunClean.cleanMode), tle.runnerName, log.InfoLevel)
				if scResult := stateCleaner.Clean(mapName); scResult.Err != nil {
					configuredErrorBehavior := tle.runnerConfig.preRunClean.errorBehavior
					if state.Ignore == tle.runnerConfig.preRunClean.errorBehavior {
						lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to clean single map '%s' in scope of pre-run eviction, but error behavior is '%s', so test loop will commence: %v", mapName, configuredErrorBehavior, err), tle.runnerName, log.WarnLevel)
					} else {
						lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to clean single map '%s' in scope of pre-run eviction and error behavior is '%s' -- won't start test run for this map: %v", mapName, configuredErrorBehavior, err), tle.runnerName, log.ErrorLevel)
						return
					}
				} else if scResult.NumCleanedItems > 0 {
					lp.LogMapRunnerEvent(fmt.Sprintf("successfully cleaned %d items from map '%s'", scResult.NumCleanedItems, mapName), tle.runnerName, log.InfoLevel)
				} else {
					lp.LogMapRunnerEvent(fmt.Sprintf("payload map '%s' either didn't contain elements to be cleaned, or wasn't susceptible to cleaning yet", mapName), tle.runnerName, log.InfoLevel)
				}
			}
			runFunc(m, mapName, i)
		}(i)
	}
	wg.Wait()

}

func (l *batchTestLoop[t]) run() {

	runWrapper(
		l.tle,
		l.gatherer,
		assembleMapName,
		l.runForMap,
	)

}

func insertInitialTestLoopStatus(g status.Gatherer, numMaps uint16, numRuns uint32) {

	g.Gather(status.Update{Key: string(statusKeyNumMaps), Value: numMaps})
	g.Gather(status.Update{Key: string(statusKeyNumRuns), Value: numRuns})
	g.Gather(status.Update{Key: string(statusKeyTotalNumRuns), Value: uint32(numMaps) * numRuns})

}

func (l *batchTestLoop[t]) runForMap(m hazelcastwrapper.Map, mapName string, mapNumber uint16) {

	sleepAfterActionBatchConfig := l.tle.runnerConfig.batch.sleepAfterActionBatch
	sleepBetweenRunsConfig := l.tle.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.tle.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc, l.tle.runnerName)
		if i > 0 && i%updateStep == 0 {
			lp.LogMapRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.tle.runnerConfig.numRuns, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)
		}
		lp.LogMapRunnerEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), l.tle.runnerName, log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepAfterActionBatchConfig, sleepTimeFunc, l.tle.runnerName)
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepAfterActionBatchConfig, sleepTimeFunc, l.tle.runnerName)
		err = l.removeSome(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepAfterActionBatchConfig, sleepTimeFunc, l.tle.runnerName)
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

}

func (l *batchTestLoop[t]) evaluateMaxIndex() uint32 {

	if l.tle.usePreInitializedElements {
		return uint32(len(l.tle.elements))
	} else {
		return l.tle.runnerConfig.numEntriesPerMap
	}

}

func (l *batchTestLoop[t]) evaluateElementID(currentIndex uint32) string {

	if l.tle.usePreInitializedElements {
		return l.tle.getElementID(l.tle.elements[currentIndex])
	} else {
		return strconv.Itoa(int(currentIndex))
	}

}

func (l *batchTestLoop[t]) ingestAll(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	numSuccessfullyIngested := uint32(0)
	maxIndex := l.evaluateMaxIndex()
	for i := uint32(0); i < maxIndex; i++ {
		elementID := l.evaluateElementID(i)
		if err := l.performSingleIngest(m, elementID, mapName, mapNumber); err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to ingest map element with ID '%s' into map '%s' after having ingested %d element/-s: %v", elementID, mapName, numSuccessfullyIngested, err), l.tle.runnerName, log.ErrorLevel)
			return err
		} else {
			numSuccessfullyIngested++
		}
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numSuccessfullyIngested, mapName), l.tle.runnerName, log.TraceLevel)
	return nil

}

func (l *batchTestLoop[t]) performSingleIngest(m hazelcastwrapper.Map, elementID, mapName string, mapNumber uint16) error {

	defer func() {
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
	}()

	key := assembleMapKey(mapName, mapNumber, elementID)
	containsKey, err := m.ContainsKey(l.tle.ctx, key)
	if err != nil {
		l.ct.increaseCounter(statusKeyNumFailedKeyChecks)
		return err
	}
	if containsKey {
		return nil
	}
	value, err := l.tle.getOrAssemblePayload(mapName, mapNumber, elementID)
	if err != nil {
		return err
	}
	if err = m.Set(l.tle.ctx, key, toValue(value)); err != nil {
		l.ct.increaseCounter(statusKeyNumFailedInserts)
		return err
	}

	return nil

}

func (l *batchTestLoop[t]) readAll(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	maxIndex := l.evaluateMaxIndex()
	numSuccessfulReads := uint32(0)
	for i := uint32(0); i < maxIndex; i++ {
		elementID := l.evaluateElementID(i)
		if err := l.performSingleRead(m, elementID, mapName, mapNumber); err != nil {
			// An error encountered during a read isn't as severe as one encountered upon set because the value could
			// have simply expired or been simply evicted. Therefore, only log warning and continue.
			lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to read element with ID '%s' from map '%s': %v", elementID, mapName, err), l.tle.runnerName, log.WarnLevel)
		} else {
			numSuccessfulReads++
		}
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("successfully read %d items from hazelcast map '%s'", len(l.tle.elements), mapName), l.tle.runnerName, log.TraceLevel)
	return nil

}

func (l *batchTestLoop[t]) performSingleRead(m hazelcastwrapper.Map, elementID, mapName string, mapNumber uint16) error {

	defer func() {
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
	}()

	key := assembleMapKey(mapName, mapNumber, elementID)
	valueFromHZ, err := m.Get(l.tle.ctx, key)
	if err != nil {
		l.ct.increaseCounter(statusKeyNumFailedReads)
		return err
	}
	if valueFromHZ == nil {
		l.ct.increaseCounter(statusKeyNumNilReads)
		return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil", key)
	}

	return nil

}

func (l *batchTestLoop[t]) removeSome(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	maxIndex := l.evaluateMaxIndex()
	numElementsToDelete := uint32(rand.Intn(int(maxIndex)) + 1)
	numSuccessfullyRemoved := 0

	for i := uint32(0); i < numElementsToDelete; i++ {
		elementID := l.evaluateElementID(i)
		if err := l.performSingleRemove(m, elementID, mapName, mapNumber); err != nil {
			// Error upon removal less severe than for ingestion (value to be removed could have expired or been
			// evicted), so merely log warning and continue
			lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to remove element with ID '%s' from map '%s': %v", elementID, mapName, err), l.tle.runnerName, log.WarnLevel)
		} else {
			numSuccessfullyRemoved++
		}
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("removed %d elements from hazelcast map '%s'", numSuccessfullyRemoved, mapName), l.tle.runnerName, log.TraceLevel)
	return nil

}

func (l *batchTestLoop[t]) performSingleRemove(m hazelcastwrapper.Map, elementID, mapName string, mapNumber uint16) error {

	defer func() {
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
	}()

	key := assembleMapKey(mapName, mapNumber, elementID)
	containsKey, err := m.ContainsKey(l.tle.ctx, key)
	if err != nil {
		return err
	}
	if !containsKey {
		return nil
	}
	_, err = m.Remove(l.tle.ctx, key)
	if err != nil {
		l.ct.increaseCounter(statusKeyNumFailedRemoves)
		return err
	}

	return nil

}

func assembleMapName(rc *runnerConfig, mapIndex uint16) string {

	mapName := rc.mapBaseName
	if rc.useMapPrefix && rc.mapPrefix != "" {
		mapName = fmt.Sprintf("%s%s", rc.mapPrefix, mapName)
	}
	if rc.appendMapIndexToMapName {
		mapName = fmt.Sprintf("%s-%d", mapName, mapIndex)
	}
	if rc.appendClientIdToMapName {
		mapName = fmt.Sprintf("%s-%s", mapName, client.ID())
	}

	return mapName

}

func (s *defaultSleeper) sleep(sc *sleepConfig, sf evaluateTimeToSleep, runnerName string) {

	if sc.enabled {
		sleepDuration := sf(sc)
		lp.LogMapRunnerEvent(fmt.Sprintf("sleeping for %d milliseconds", sleepDuration), runnerName, log.TraceLevel)
		time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	}

}

func assembleMapKey(mapName string, mapNumber uint16, elementID string) string {

	return fmt.Sprintf("%s-%s-%d-%s", client.ID(), mapName, mapNumber, elementID)

}

/*
	toValue simply dereferences the given pointer and returns the string value.

We need to make sure to dereference strings and pass their values into Hazelcast due to auto-registering of
values to be serialized using gob.Register that happens in Hazelcast Golang client as of version 1.4.2.
At this point in code execution, multiple values have already been sent to the target Hazelcast cluster,
having gone through serialization -- hence through auto-registration --, and since these values are strings,
the string type has already been registered. Trying to pass in a string pointer now yields the
'gob: registering duplicate names for *string: "string" != "*string' error message.
*/
func toValue(s *string) string {
	return *s
}
