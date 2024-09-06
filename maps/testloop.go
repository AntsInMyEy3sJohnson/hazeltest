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
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type (
	evaluateTimeToSleep      func(sc *sleepConfig) int
	getElementIdFunc         func(element any) string
	getOrAssemblePayloadFunc func(mapName string, mapNumber uint16, element any) (any, error)
	looper[t any]            interface {
		init(lc *testLoopExecution[t], s sleeper, gatherer *status.Gatherer)
		run()
	}
	counterTracker interface {
		init(gatherer *status.Gatherer)
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
		gatherer *status.Gatherer
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
	boundaryTestLoop[t any] struct {
		tle      *testLoopExecution[t]
		gatherer *status.Gatherer
		s        sleeper
		ct       counterTracker
	}
	testLoopExecution[t any] struct {
		id                   uuid.UUID
		runnerName           string
		source               string
		hzClientHandler      hazelcastwrapper.HzClientHandler
		hzMapStore           hazelcastwrapper.MapStore
		stateCleanerBuilder  state.SingleMapCleanerBuilder
		runnerConfig         *runnerConfig
		elements             []t
		ctx                  context.Context
		getElementID         getElementIdFunc
		getOrAssemblePayload getOrAssemblePayloadFunc
	}
	mapTestLoopCountersTracker struct {
		counters map[statusKey]uint64
		l        sync.Mutex
		gatherer *status.Gatherer
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

func (ct *mapTestLoopCountersTracker) init(gatherer *status.Gatherer) {
	ct.gatherer = gatherer

	ct.counters = make(map[statusKey]uint64)

	initialCounterValue := uint64(0)
	for _, v := range counters {
		ct.counters[v] = initialCounterValue
		gatherer.Updates <- status.Update{Key: string(v), Value: initialCounterValue}
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

	ct.gatherer.Updates <- status.Update{Key: string(sk), Value: newValue}

}

func (l *boundaryTestLoop[t]) init(tle *testLoopExecution[t], s sleeper, gatherer *status.Gatherer) {
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

func chooseRandomKeyFromCache(cache map[string]struct{}) (string, error) {

	if len(cache) == 0 {
		return "", errors.New("cannot pick element from empty cache")
	}

	randomIndex := rand.Intn(len(cache))

	i := 0
	for k := range cache {
		if i == randomIndex {
			return k, nil
		}
		i++
	}

	// Due to the way the random index is initialized and the element is selected, this cannot occur
	return "", fmt.Errorf("no match found for index %d in cache of size %d", randomIndex, len(cache))

}

func (l *boundaryTestLoop[t]) chooseRandomElementFromSourceData() t {

	randomIndex := rand.Intn(len(l.tle.elements))
	return l.tle.elements[randomIndex]

}

func (l *boundaryTestLoop[t]) chooseNextMapElement(action mapAction, keysCache map[string]struct{}, mapNumber uint16) (t, error) {

	switch action {
	case insert:
		for _, v := range l.tle.elements {
			key := assembleMapKey(mapNumber, l.tle.getElementID(v))
			if _, containsKey := keysCache[key]; !containsKey {
				return v, nil
			}
		}
		// Getting to this point means that the 'insert' action was chosen elsewhere despite the fact
		// that all elements have already been stored in cache. This case should not occur,
		// but when it does nonetheless, it is not sufficiently severe to report an error
		// and abort execution. So, in this case, we simply choose an element from the source data randomly.
		lp.LogMapRunnerEvent("cache already contains all elements of data source, so cannot pick element not yet contained -- choosing one at random", l.tle.runnerName, log.WarnLevel)
		return l.chooseRandomElementFromSourceData(), nil
	case read, remove:
		keyFromCache, err := chooseRandomKeyFromCache(keysCache)
		if err != nil {
			msg := fmt.Sprintf("choosing next map element for map action '%s' unsuccessful: %s", action, err.Error())
			lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
			return l.tle.elements[0], fmt.Errorf(msg)
		}
		for _, v := range l.tle.elements {
			keyFromSourceData := assembleMapKey(mapNumber, l.tle.getElementID(v))
			if keyFromSourceData == keyFromCache {
				return v, nil
			}
		}
		msg := fmt.Sprintf("key '%s' from local cache had no match in source data -- cache may have been populated incorrectly", keyFromCache)
		lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
		return l.tle.elements[0], errors.New(msg)
	default:
		msg := fmt.Sprintf("no such map action: %s", action)
		lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
		return l.tle.elements[0], errors.New(msg)
	}

}

func assemblePredicate(clientID uuid.UUID, mapNumber uint16) predicate.Predicate {

	return predicate.SQL(fmt.Sprintf("__key like %s-%d%%", clientID, mapNumber))

}

func (l *boundaryTestLoop[t]) runForMap(m hazelcastwrapper.Map, mapName string, mapNumber uint16) {

	sleepBetweenRunsConfig := l.tle.runnerConfig.sleepBetweenRuns

	mc := &modeCache{}
	ac := &actionCache{}
	keysCache := make(map[string]struct{})

	for i := uint32(0); i < l.tle.runnerConfig.numRuns; i++ {

		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc, l.tle.runnerName)

		if i > 0 && i%updateStep == 0 {
			lp.LogMapRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.tle.runnerConfig.numRuns, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)
		}

		if err := l.runOperationChain(i, m, mc, ac, mapName, mapNumber, keysCache); err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("running operation chain unsuccessful in map run %d on map '%s' in goroutine %d -- retrying in next run", i, mapName, mapNumber), l.tle.runnerName, log.WarnLevel)
		} else {
			lp.LogMapRunnerEvent(fmt.Sprintf("successfully finished operation chain for map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), l.tle.runnerName, log.InfoLevel)
		}

		if l.tle.runnerConfig.boundary.resetAfterChain {
			lp.LogMapRunnerEvent(fmt.Sprintf("performing reset after operation chain on map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), l.tle.runnerName, log.InfoLevel)
			l.resetAfterOperationChain(m, mapName, mapNumber, &keysCache, mc, ac)
		}

	}

	lp.LogMapRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

}

func (l *boundaryTestLoop[t]) resetAfterOperationChain(m hazelcastwrapper.Map, mapName string, mapNumber uint16, keysCache *map[string]struct{}, mc *modeCache, ac *actionCache) {

	lp.LogMapRunnerEvent(fmt.Sprintf("resetting mode and action cache for map '%s' on goroutine %d", mapName, mapNumber), l.tle.runnerName, log.TraceLevel)

	*mc = modeCache{}
	*ac = actionCache{}

	p := assemblePredicate(client.ID(), mapNumber)
	lp.LogMapRunnerEvent(fmt.Sprintf("removing all keys from map '%s' in goroutine %d having match for predicate '%s'", mapName, mapNumber, p), l.tle.runnerName, log.TraceLevel)
	err := m.RemoveAll(l.tle.ctx, p)
	if err != nil {
		lp.LogHzEvent(fmt.Sprintf("won't update local cache because removing all keys from map '%s' in goroutine %d having match for predicate '%s' failed due to error: '%s'", mapName, mapNumber, p, err.Error()), log.WarnLevel)
	} else {
		*keysCache = make(map[string]struct{})
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
	mapName string,
	mapNumber uint16,
	keysCache map[string]struct{},
) error {

	chainLength := l.tle.runnerConfig.boundary.chainLength
	lp.LogMapRunnerEvent(fmt.Sprintf("starting operation chain of length %d for map '%s' on goroutine %d", chainLength, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

	l.s.sleep(l.tle.runnerConfig.boundary.sleepBetweenOperationChains, sleepTimeFunc, l.tle.runnerName)

	upperBoundary, lowerBoundary := evaluateMapFillBoundaries(l.tle.runnerConfig.boundary)
	actionProbability := l.tle.runnerConfig.boundary.actionTowardsBoundaryProbability

	lp.LogMapRunnerEvent(fmt.Sprintf("using upper boundary %f and lower boundary %f for map '%s' on goroutine %d", upperBoundary, lowerBoundary, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

	for j := 0; j < chainLength; j++ {

		if (actions.last == insert || actions.last == remove) && j > 0 && uint32(j)%updateStep == 0 {
			lp.LogMapRunnerEvent(fmt.Sprintf("chain position %d of %d for map '%s' on goroutine %d", j, chainLength, mapName, mapNumber), l.tle.runnerName, log.InfoLevel)
		}

		nextMode, forceActionTowardsMode := l.checkForModeChange(upperBoundary, lowerBoundary, uint32(len(keysCache)), modes.current)
		if nextMode != modes.current && modes.current != "" {
			lp.LogMapRunnerEvent(fmt.Sprintf("detected mode change from '%s' to '%s' for map '%s' in chain position %d with %d map items currently under management", modes.current, nextMode, mapName, j, len(keysCache)), l.tle.runnerName, log.InfoLevel)
			l.s.sleep(l.tle.runnerConfig.boundary.sleepUponModeChange, sleepTimeFunc, l.tle.runnerName)
		}
		modes.current, modes.forceActionTowardsMode = nextMode, forceActionTowardsMode

		actions.next = determineNextMapAction(modes, actions.last, actionProbability, len(keysCache))

		lp.LogMapRunnerEvent(fmt.Sprintf("for map '%s' in goroutine %d, current mode is '%s', and next map action was determined to be '%s'", mapName, mapNumber, modes.current, actions.next), l.tle.runnerName, log.TraceLevel)
		if actions.next == noop {
			msg := fmt.Sprintf("encountered no-op case for map '%s' in goroutine %d in operation chain iteration %d -- assuming incorrect configuration, aborting", mapName, mapNumber, j)
			lp.LogMapRunnerEvent(msg, l.tle.runnerName, log.ErrorLevel)
			return errors.New(msg)
		}

		if actions.next == read && j > 0 {
			// We need this loop to perform a state-changing operation on every element, so
			// don't count read operation since it did not change state (except potentially some
			// meta information on the key read in the map on the cluster)
			// Also, in case of a read, the element the read operation will be attempted for
			// must refer to an element previously inserted
			j--
		}

		nextMapElement, err := l.chooseNextMapElement(actions.next, keysCache, mapNumber)

		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("unable to choose next map element to work on for map '%s' due to error ('%s') -- aborting operation chain to try in next run", mapName, err.Error()), l.tle.runnerName, log.ErrorLevel)
			break
		}

		lp.LogMapRunnerEvent(fmt.Sprintf("successfully chose next map element for map '%s' in goroutine %d for map action '%s'", mapName, mapNumber, actions.next), l.tle.runnerName, log.TraceLevel)

		err = l.executeMapAction(m, mapName, mapNumber, nextMapElement, actions.next)
		actions.last = actions.next
		actions.next = ""

		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon execution of '%s' action on map '%s' in run '%d' (still moving to next loop iteration): %v", actions.last, mapName, currentRun, err), l.tle.runnerName, log.WarnLevel)
		} else {
			lp.LogMapRunnerEvent(fmt.Sprintf("action '%s' successfully executed on map '%s', moving to next action in upcoming loop iteration", actions.last, mapName), l.tle.runnerName, log.TraceLevel)
			updateKeysCache(actions.last, keysCache, assembleMapKey(mapNumber, l.tle.getElementID(nextMapElement)), l.tle.runnerName)
		}

		l.s.sleep(l.tle.runnerConfig.boundary.sleepAfterChainAction, sleepTimeFunc, l.tle.runnerName)

	}

	return nil

}

func updateKeysCache(lastSuccessfulAction mapAction, keysCache map[string]struct{}, key, runnerName string) {

	switch lastSuccessfulAction {
	case insert, remove:
		if lastSuccessfulAction == insert {
			keysCache[key] = struct{}{}
		} else {
			delete(keysCache, key)
		}
		lp.LogMapRunnerEvent(fmt.Sprintf("update on key cache successful for map action '%s', cache now containing %d element/-s", lastSuccessfulAction, len(keysCache)), runnerName, log.TraceLevel)
	default:
		lp.LogMapRunnerEvent(fmt.Sprintf("no action to perform on local cache for last successful action '%s'", lastSuccessfulAction), runnerName, log.TraceLevel)
	}

}

func (l *boundaryTestLoop[t]) executeMapAction(m hazelcastwrapper.Map, mapName string, mapNumber uint16, element t, action mapAction) error {

	elementID := l.tle.getElementID(element)

	key := assembleMapKey(mapNumber, elementID)

	switch action {
	case insert:
		payload, err := l.tle.getOrAssemblePayload(mapName, mapNumber, element)
		if err != nil {
			lp.LogMapRunnerEvent(fmt.Sprintf("unable to execute insert operation for map '%s' due to error upon generating payload: %v", mapName, err), l.tle.runnerName, log.ErrorLevel)
			return err
		}
		if err := m.Set(l.tle.ctx, key, payload); err != nil {
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

func determineNextMapAction(mc *modeCache, lastAction mapAction, actionProbability float32, currentCacheSize int) mapAction {

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

func (l *boundaryTestLoop[t]) checkForModeChange(upperBoundary, lowerBoundary float32, currentCacheSize uint32, currentMode actionMode) (actionMode, bool) {

	if currentCacheSize == 0 || currentMode == "" {
		return fill, false
	}

	maxNumElements := float64(len(l.tle.elements))
	currentNumElements := float64(currentCacheSize)

	if currentNumElements <= math.Round(maxNumElements*float64(lowerBoundary)) {
		lp.LogMapRunnerEvent(fmt.Sprintf("enforcing 'fill' mode -- current number of elements: %d; total number of elements: %d", currentCacheSize, len(l.tle.elements)), l.tle.runnerName, log.TraceLevel)
		return fill, true
	}

	if currentNumElements >= math.Round(maxNumElements*float64(upperBoundary)) {
		lp.LogMapRunnerEvent(fmt.Sprintf("enforcing 'drain' mode -- current number of elements: %d; total number of elements: %d", currentCacheSize, len(l.tle.elements)), l.tle.runnerName, log.TraceLevel)
		return drain, true
	}

	return currentMode, false

}

func (l *batchTestLoop[t]) init(tle *testLoopExecution[t], s sleeper, gatherer *status.Gatherer) {
	l.tle = tle
	l.s = s
	l.gatherer = gatherer

	ct := &mapTestLoopCountersTracker{}
	ct.init(gatherer)

	l.ct = ct
}

func runWrapper[t any](tle *testLoopExecution[t],
	gatherer *status.Gatherer,
	assembleMapNameFunc func(*runnerConfig, uint16) string,
	runFunc func(hazelcastwrapper.Map, string, uint16)) {

	rc := tle.runnerConfig
	insertInitialTestLoopStatus(gatherer.Updates, rc.numMaps, rc.numRuns)

	var stateCleaner state.SingleCleaner
	var hzService string
	if tle.runnerConfig.preRunClean.enabled {
		// TODO Add information collected by tracker to test loop status
		// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/70
		stateCleaner, hzService = tle.stateCleanerBuilder.Build(
			tle.ctx,
			tle.hzMapStore,
			&state.CleanedDataStructureTracker{G: gatherer},
			&state.DefaultLastCleanedInfoHandler{
				Ctx: tle.ctx,
				Ms:  tle.hzMapStore,
				Cfg: &state.LastCleanedInfoHandlerConfig{
					UseCleanAgainThreshold: tle.runnerConfig.preRunClean.applyCleanAgainThreshold,
					CleanAgainThresholdMs:  tle.runnerConfig.preRunClean.cleanAgainThresholdMs,
				},
			},
		)
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
				if numCleanedItems, err := stateCleaner.Clean(mapName); err != nil {
					configuredErrorBehavior := tle.runnerConfig.preRunClean.errorBehavior
					if state.Ignore == tle.runnerConfig.preRunClean.errorBehavior {
						lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to clean single map '%s' in scope of pre-run eviction, but error behavior is '%s', so test loop will commence: %v", mapName, configuredErrorBehavior, err), tle.runnerName, log.WarnLevel)
					} else {
						lp.LogMapRunnerEvent(fmt.Sprintf("encountered error upon attempt to clean single map '%s' in scope of pre-run eviction and error behavior is '%s' -- won't start test run for this map: %v", mapName, configuredErrorBehavior, err), tle.runnerName, log.ErrorLevel)
						return
					}
				} else if numCleanedItems > 0 {
					lp.LogMapRunnerEvent(fmt.Sprintf("successfully cleaned %d items from map '%s'", numCleanedItems, mapName), tle.runnerName, log.InfoLevel)
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

func insertInitialTestLoopStatus(c chan status.Update, numMaps uint16, numRuns uint32) {

	c <- status.Update{Key: string(statusKeyNumMaps), Value: numMaps}
	c <- status.Update{Key: string(statusKeyNumRuns), Value: numRuns}
	c <- status.Update{Key: string(statusKeyTotalNumRuns), Value: uint32(numMaps) * numRuns}

}

func (l *batchTestLoop[t]) runForMap(m hazelcastwrapper.Map, mapName string, mapNumber uint16) {

	sleepBetweenActionBatchesConfig := l.tle.runnerConfig.batch.sleepBetweenActionBatches
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
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc, l.tle.runnerName)
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc, l.tle.runnerName)
		err = l.removeSome(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), l.tle.runnerName, log.InfoLevel)

}

func (l *batchTestLoop[t]) ingestAll(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	numNewlyIngested := 0
	for _, v := range l.tle.elements {
		key := assembleMapKey(mapNumber, l.tle.getElementID(v))
		containsKey, err := m.ContainsKey(l.tle.ctx, key)
		if err != nil {
			l.ct.increaseCounter(statusKeyNumFailedKeyChecks)
			return err
		}
		if containsKey {
			continue
		}
		value, err := l.tle.getOrAssemblePayload(mapName, mapNumber, v)
		if err != nil {
			return err
		}
		if err = m.Set(l.tle.ctx, key, value); err != nil {
			l.ct.increaseCounter(statusKeyNumFailedInserts)
			return err
		}
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
		numNewlyIngested++
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), l.tle.runnerName, log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) readAll(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	for _, v := range l.tle.elements {
		key := assembleMapKey(mapNumber, l.tle.getElementID(v))
		valueFromHZ, err := m.Get(l.tle.ctx, key)
		if err != nil {
			l.ct.increaseCounter(statusKeyNumFailedReads)
			return err
		}
		if valueFromHZ == nil {
			l.ct.increaseCounter(statusKeyNumNilReads)
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil", key)
		}
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.tle.elements), mapName), l.tle.runnerName, log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) removeSome(m hazelcastwrapper.Map, mapName string, mapNumber uint16) error {

	numElementsToDelete := rand.Intn(len(l.tle.elements)) + 1
	removed := 0

	elements := l.tle.elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(mapNumber, l.tle.getElementID(elements[i]))
		containsKey, err := m.ContainsKey(l.tle.ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(l.tle.ctx, key)
		if err != nil {
			l.ct.increaseCounter(statusKeyNumFailedRemoves)
			return err
		}
		removed++
		l.s.sleep(l.tle.runnerConfig.batch.sleepAfterBatchAction, sleepTimeFunc, l.tle.runnerName)
	}

	lp.LogMapRunnerEvent(fmt.Sprintf("removed %d elements from hazelcast map '%s'", removed, mapName), l.tle.runnerName, log.TraceLevel)

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

func assembleMapKey(mapNumber uint16, elementID string) string {

	return fmt.Sprintf("%s-%d-%s", client.ID(), mapNumber, elementID)

}
