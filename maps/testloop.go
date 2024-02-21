package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/status"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type (
	evaluateTimeToSleep func(sc *sleepConfig) int
	getElementID        func(element any) string
	looper[t any]       interface {
		init(lc *testLoopExecution[t], s sleeper, g *status.Gatherer)
		run()
	}
	sleeper interface {
		sleep(sc *sleepConfig, sf evaluateTimeToSleep)
	}
	defaultSleeper struct{}
)

type (
	batchTestLoop[t any] struct {
		execution *testLoopExecution[t]
		s         sleeper
		g         *status.Gatherer
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
		execution *testLoopExecution[t]
		s         sleeper
		g         *status.Gatherer
	}
	testLoopExecution[t any] struct {
		id               uuid.UUID
		source           string
		mapStore         hzMapStore
		runnerConfig     *runnerConfig
		elements         []t
		ctx              context.Context
		getElementIdFunc getElementID
	}
)

type (
	actionMode string
	mapAction  string
)

const (
	updateStep            uint32     = 50
	statusKeyNumMaps                 = "numMaps"
	statusKeyNumRuns                 = "numRuns"
	statusKeyTotalNumRuns            = "totalNumRuns"
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
	statusKeyNumInsertsFailed = "numInsertsFailed"
	statusKeyNumReadsFailed   = "numReadsFailed"
	statusKeyNumNilReads      = "numNilReads"
	statusKeyNumRemovesFailed = "numRemovesFailed"
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
)

func (l *boundaryTestLoop[t]) init(lc *testLoopExecution[t], s sleeper, g *status.Gatherer) {
	l.execution = lc
	l.s = s
	l.g = g
	api.RegisterTestLoopStatus(api.Maps, lc.source, l.g.AssembleStatusCopy)
}

func (l *boundaryTestLoop[t]) run() {

	runWrapper(
		l.execution,
		l.g,
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

	randomIndex := rand.Intn(len(l.execution.elements))
	return l.execution.elements[randomIndex]

}

func (l *boundaryTestLoop[t]) chooseNextMapElement(action mapAction, keysCache map[string]struct{}, mapNumber uint16) (t, error) {

	switch action {
	case insert:
		for _, v := range l.execution.elements {
			key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
			if _, containsKey := keysCache[key]; !containsKey {
				return v, nil
			}
		}
		// Getting to this point means that the 'insert' action was chose elsewhere despite the fact
		// that all elements have already been stored in cache. This case should not occur,
		// but when it does nonetheless, it is not sufficiently severe to report an error
		// and abort execution. So, in this case, we simply choose an element from the source data randomly.
		lp.LogRunnerEvent("cache already contains all elements of data source, so cannot pick element not yet contained -- choosing one at random", log.WarnLevel)
		return l.chooseRandomElementFromSourceData(), nil
	case read, remove:
		keyFromCache, err := chooseRandomKeyFromCache(keysCache)
		if err != nil {
			msg := fmt.Sprintf("choosing next map element for map action '%s' unsuccessful: %s", action, err.Error())
			lp.LogRunnerEvent(msg, log.ErrorLevel)
			return l.execution.elements[0], fmt.Errorf(msg)
		}
		for _, v := range l.execution.elements {
			keyFromSourceData := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
			if keyFromSourceData == keyFromCache {
				return v, nil
			}
		}
		msg := fmt.Sprintf("key '%s' from local cache had no match in source data -- cache may have been populated incorrectly", keyFromCache)
		lp.LogRunnerEvent(msg, log.ErrorLevel)
		return l.execution.elements[0], errors.New(msg)
	default:
		msg := fmt.Sprintf("no such map action: %s", action)
		lp.LogRunnerEvent(msg, log.ErrorLevel)
		return l.execution.elements[0], errors.New(msg)
	}

}

func assemblePredicate(clientID uuid.UUID, mapNumber uint16) predicate.Predicate {

	return predicate.SQL(fmt.Sprintf("__key like %s-%d%%", clientID, mapNumber))

}

func queryRemoteMapKeys(ctx context.Context, m hzMap, mapName string, mapNumber uint16) (map[string]struct{}, error) {

	p := assemblePredicate(client.ID(), mapNumber)
	lp.LogRunnerEvent(fmt.Sprintf("querying map keys for map '%s' in goroutine %d using predicate '%s'", mapName, mapNumber, p.String()), log.TraceLevel)
	keySet, err := m.GetKeySetWithPredicate(ctx, p)

	result := make(map[string]struct{})

	if err != nil {
		lp.LogHzEvent(fmt.Sprintf("unable to populate local cache because predicated query for key set on map '%s' was unsuccessful: %s", mapName, err.Error()), log.WarnLevel)
		return result, err
	}

	for _, v := range keySet {
		result[v.(string)] = struct{}{}
	}

	return result, nil

}

func (l *boundaryTestLoop[t]) runForMap(m hzMap, mapName string, mapNumber uint16, statusRecord map[string]any) {

	sleepBetweenRunsConfig := l.execution.runnerConfig.sleepBetweenRuns

	mc := &modeCache{}
	ac := &actionCache{}

	for i := uint32(0); i < l.execution.runnerConfig.numRuns; i++ {

		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)

		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.execution.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
		}

		keysCache, err := queryRemoteMapKeys(l.execution.ctx, m, mapName, mapNumber)

		if err != nil {
			lp.LogRunnerEvent("populating local cache unsuccessful -- aborting since feature is not able to function without local cache", log.ErrorLevel)
			return
		}

		lp.LogRunnerEvent(fmt.Sprintf("queried %d element/-s from target map '%s' on map goroutine %d -- using as local state", len(keysCache), mapName, mapNumber), log.TraceLevel)

		if err := l.runOperationChain(i, m, mc, ac, mapName, mapNumber, keysCache, statusRecord); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("running operation chain unsuccessful in map run %d on map '%s' in goroutine %d -- retrying in next run", i, mapName, mapNumber), log.WarnLevel)
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully finished operation chain for map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), log.InfoLevel)
		}

		if l.execution.runnerConfig.boundary.resetAfterChain {
			lp.LogRunnerEvent(fmt.Sprintf("performing reset after operation chain on map '%s' in goroutine %d in map run %d", mapName, mapNumber, i), log.InfoLevel)
			l.resetAfterOperationChain(m, mapName, mapNumber, &keysCache, mc, ac)
		}

	}

	lp.LogRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l *boundaryTestLoop[t]) resetAfterOperationChain(m hzMap, mapName string, mapNumber uint16, keysCache *map[string]struct{}, mc *modeCache, ac *actionCache) {

	lp.LogRunnerEvent(fmt.Sprintf("resetting mode and action cache for map '%s' on goroutine %d", mapName, mapNumber), log.TraceLevel)

	*mc = modeCache{}
	*ac = actionCache{}

	p := assemblePredicate(client.ID(), mapNumber)
	lp.LogRunnerEvent(fmt.Sprintf("removing all keys from map '%s' in goroutine %d having match for predicate '%s'", mapName, mapNumber, p), log.TraceLevel)
	err := m.RemoveAll(l.execution.ctx, p)
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
	m hzMap,
	modes *modeCache,
	actions *actionCache,
	mapName string,
	mapNumber uint16,
	keysCache map[string]struct{},
	statusRecord map[string]any,
) error {

	chainLength := l.execution.runnerConfig.boundary.chainLength
	lp.LogRunnerEvent(fmt.Sprintf("starting operation chain of length %d for map '%s' on goroutine %d", chainLength, mapName, mapNumber), log.InfoLevel)

	l.s.sleep(l.execution.runnerConfig.boundary.sleepBetweenOperationChains, sleepTimeFunc)

	upperBoundary, lowerBoundary := evaluateMapFillBoundaries(l.execution.runnerConfig.boundary)
	actionProbability := l.execution.runnerConfig.boundary.actionTowardsBoundaryProbability

	lp.LogRunnerEvent(fmt.Sprintf("using upper boundary %f and lower boundary %f for map '%s' on goroutine %d", upperBoundary, lowerBoundary, mapName, mapNumber), log.InfoLevel)

	for j := 0; j < chainLength; j++ {

		if (actions.last == insert || actions.last == remove) && j > 0 && uint32(j)%updateStep == 0 {
			// TODO Include in status endpoint --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/20
			lp.LogRunnerEvent(fmt.Sprintf("chain position %d of %d for map '%s' on goroutine %d", j, chainLength, mapName, mapNumber), log.InfoLevel)
		}

		nextMode, forceActionTowardsMode := l.checkForModeChange(upperBoundary, lowerBoundary, uint32(len(keysCache)), modes.current)
		if nextMode != modes.current && modes.current != "" {
			lp.LogRunnerEvent(fmt.Sprintf("detected mode change from '%s' to '%s' for map '%s' in chain position %d with %d map items currently under management", modes.current, nextMode, mapName, j, len(keysCache)), log.InfoLevel)
			l.s.sleep(l.execution.runnerConfig.boundary.sleepUponModeChange, sleepTimeFunc)
		}
		modes.current, modes.forceActionTowardsMode = nextMode, forceActionTowardsMode

		actions.next = determineNextMapAction(modes, actions.last, actionProbability, len(keysCache))

		lp.LogRunnerEvent(fmt.Sprintf("for map '%s' in goroutine %d, current mode is '%s', and next map action was determined to be '%s'", mapName, mapNumber, modes.current, actions.next), log.TraceLevel)
		if actions.next == noop {
			msg := fmt.Sprintf("encountered no-op case for map '%s' in goroutine %d in operation chain iteration %d -- assuming incorrect configuration, aborting", mapName, mapNumber, j)
			lp.LogRunnerEvent(msg, log.ErrorLevel)
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
			lp.LogRunnerEvent(fmt.Sprintf("unable to choose next map element to work on for map '%s' due to error ('%s') -- aborting operation chain to try in next run", mapName, err.Error()), log.ErrorLevel)
			break
		}

		lp.LogRunnerEvent(fmt.Sprintf("successfully chose next map element for map '%s' in goroutine %d for map action '%s'", mapName, mapNumber, actions.next), log.TraceLevel)

		if err := l.executeMapAction(m, mapName, mapNumber, nextMapElement, actions.next, statusRecord); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("unable to execute action '%s' on map '%s' in iteration '%d'", actions.next, mapName, currentRun), log.WarnLevel)
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("action '%s' successfully executed on map '%s', moving to next action in upcoming loop", actions.next, mapName), log.TraceLevel)
			actions.last = actions.next
			actions.next = ""
			updateKeysCache(actions.last, keysCache, assembleMapKey(mapNumber, l.execution.getElementIdFunc(nextMapElement)))
		}

		l.s.sleep(l.execution.runnerConfig.boundary.sleepAfterChainAction, sleepTimeFunc)

	}

	return nil

}

func updateKeysCache(lastSuccessfulAction mapAction, keysCache map[string]struct{}, key string) {

	switch lastSuccessfulAction {
	case insert, remove:
		if lastSuccessfulAction == insert {
			keysCache[key] = struct{}{}
		} else {
			delete(keysCache, key)
		}
		lp.LogRunnerEvent(fmt.Sprintf("update on key cache successful for map action '%s', cache now containing %d element/-s", lastSuccessfulAction, len(keysCache)), log.TraceLevel)
	default:
		lp.LogRunnerEvent(fmt.Sprintf("no action to perform on local cache for last successful action '%s'", lastSuccessfulAction), log.TraceLevel)
	}

}

func increaseNumInsertsFailed(g *status.Gatherer, statusRecord map[string]any) {

	statusRecord[statusKeyNumInsertsFailed] = statusRecord[statusKeyNumInsertsFailed].(int) + 1
	sendUpdate(g, statusKeyNumInsertsFailed, statusRecord[statusKeyNumInsertsFailed])

}

func increaseNumRemovesFailed(g *status.Gatherer, statusRecord map[string]any) {

	statusRecord[statusKeyNumRemovesFailed] = statusRecord[statusKeyNumRemovesFailed].(int) + 1
	sendUpdate(g, statusKeyNumRemovesFailed, statusRecord[statusKeyNumRemovesFailed])

}

func increaseNumReadsFailed(g *status.Gatherer, statusRecord map[string]any) {

	statusRecord[statusKeyNumReadsFailed] = statusRecord[statusKeyNumReadsFailed].(int) + 1
	sendUpdate(g, statusKeyNumReadsFailed, statusRecord[statusKeyNumReadsFailed])

}

func increaseNumNilReads(g *status.Gatherer, statusRecord map[string]any) {

	statusRecord[statusKeyNumNilReads] = statusRecord[statusKeyNumNilReads].(int) + 1
	sendUpdate(g, statusKeyNumNilReads, statusRecord[statusKeyNumNilReads])

}

func sendUpdate(g *status.Gatherer, key string, value any) {

	g.Updates <- status.Update{Key: key, Value: value}

}

func (l *boundaryTestLoop[t]) executeMapAction(m hzMap, mapName string, mapNumber uint16, element t, action mapAction, statusRecord map[string]any) error {

	elementID := l.execution.getElementIdFunc(element)

	key := assembleMapKey(mapNumber, elementID)

	switch action {
	case insert:
		if err := m.Set(l.execution.ctx, key, element); err != nil {
			increaseNumInsertsFailed(l.g, statusRecord)
			lp.LogHzEvent(fmt.Sprintf("failed to insert key '%s' into map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogHzEvent(fmt.Sprintf("successfully inserted key '%s' into map '%s'", key, mapName), log.TraceLevel)
			return nil
		}
	case remove:
		if _, err := m.Remove(l.execution.ctx, key); err != nil {
			increaseNumRemovesFailed(l.g, statusRecord)
			lp.LogHzEvent(fmt.Sprintf("failed to remove key '%s' from map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogHzEvent(fmt.Sprintf("successfully removed key '%s' from map '%s'", key, mapName), log.TraceLevel)
			return nil
		}
	case read:
		if _, err := m.Get(l.execution.ctx, key); err != nil {
			increaseNumReadsFailed(l.g, statusRecord)
			lp.LogHzEvent(fmt.Sprintf("failed to read key from '%s' in map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			// TODO Check whether value was nil, increase num nil reads, if so
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

	maxNumElements := float64(len(l.execution.elements))
	currentNumElements := float64(currentCacheSize)

	if currentNumElements <= math.Round(maxNumElements*float64(lowerBoundary)) {
		lp.LogRunnerEvent(fmt.Sprintf("enforcing 'fill' mode -- current number of elements: %d; total number of elements: %d", currentCacheSize, len(l.execution.elements)), log.TraceLevel)
		return fill, true
	}

	if currentNumElements >= math.Round(maxNumElements*float64(upperBoundary)) {
		lp.LogRunnerEvent(fmt.Sprintf("enforcing 'drain' mode -- current number of elements: %d; total number of elements: %d", currentCacheSize, len(l.execution.elements)), log.TraceLevel)
		return drain, true
	}

	return currentMode, false

}

func (l *batchTestLoop[t]) init(lc *testLoopExecution[t], s sleeper, g *status.Gatherer) {
	l.execution = lc
	l.s = s
	l.g = g
	api.RegisterTestLoopStatus(api.Maps, lc.source, l.g.AssembleStatusCopy)
}

func runWrapper[t any](c *testLoopExecution[t],
	gatherer *status.Gatherer,
	assembleMapNameFunc func(*runnerConfig, uint16) string,
	runFunc func(hzMap, string, uint16, map[string]any)) {

	defer gatherer.StopListen()
	go gatherer.Listen()

	rc := c.runnerConfig
	insertLoopWithInitialStatus(gatherer.Updates, rc.numMaps, rc.numRuns)

	var wg sync.WaitGroup
	for i := uint16(0); i < rc.numMaps; i++ {
		wg.Add(1)
		go func(i uint16) {
			defer wg.Done()
			mapName := assembleMapNameFunc(c.runnerConfig, i)
			lp.LogRunnerEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			m, err := c.mapStore.GetMap(c.ctx, mapName)
			if err != nil {
				lp.LogHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err), log.ErrorLevel)
				return
			}
			defer func() {
				_ = m.Destroy(c.ctx)
			}()
			elapsed := time.Since(start).Milliseconds()
			lp.LogTimingEvent("getMap()", mapName, int(elapsed), log.InfoLevel)
			statusRecord := map[string]any{
				statusKeyNumInsertsFailed: 0,
				statusKeyNumReadsFailed:   0,
				statusKeyNumRemovesFailed: 0,
				statusKeyNumNilReads:      0,
			}
			runFunc(m, mapName, i, statusRecord)
		}(i)
	}
	wg.Wait()

}

func (l *batchTestLoop[t]) run() {

	runWrapper(
		l.execution,
		l.g,
		assembleMapName,
		l.runForMap,
	)

}

func insertLoopWithInitialStatus(c chan status.Update, numMaps uint16, numRuns uint32) {

	c <- status.Update{Key: statusKeyNumMaps, Value: numMaps}
	c <- status.Update{Key: statusKeyNumRuns, Value: numRuns}
	c <- status.Update{Key: statusKeyTotalNumRuns, Value: uint32(numMaps) * numRuns}

}

func (l *batchTestLoop[t]) runForMap(m hzMap, mapName string, mapNumber uint16, statusRecord map[string]any) {

	sleepBetweenActionBatchesConfig := l.execution.runnerConfig.batch.sleepBetweenActionBatches
	sleepBetweenRunsConfig := l.execution.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.execution.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.execution.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		lp.LogRunnerEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber, statusRecord)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc)
		err = l.readAll(m, mapName, mapNumber, statusRecord)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc)
		err = l.removeSome(m, mapName, mapNumber, statusRecord)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l *batchTestLoop[t]) ingestAll(m hzMap, mapName string, mapNumber uint16, statusRecord map[string]any) error {

	numNewlyIngested := 0
	for _, v := range l.execution.elements {
		key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
		containsKey, err := m.ContainsKey(l.execution.ctx, key)
		if err != nil {
			// TODO Make this an update to the status gatherer, too
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(l.execution.ctx, key, v); err != nil {
			increaseNumInsertsFailed(l.g, statusRecord)
			return err
		}
		numNewlyIngested++
	}

	lp.LogRunnerEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) readAll(m hzMap, mapName string, mapNumber uint16, statusRecord map[string]any) error {

	for _, v := range l.execution.elements {
		key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
		valueFromHZ, err := m.Get(l.execution.ctx, key)
		if err != nil {
			increaseNumReadsFailed(l.g, statusRecord)
			return err
		}
		if valueFromHZ == nil {
			increaseNumNilReads(l.g, statusRecord)
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil -- value might have been evicted or expired in hazelcast", key)
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.execution.elements), mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) removeSome(m hzMap, mapName string, mapNumber uint16, statusRecord map[string]any) error {

	numElementsToDelete := rand.Intn(len(l.execution.elements))
	removed := 0

	elements := l.execution.elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(elements[i]))
		containsKey, err := m.ContainsKey(l.execution.ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(l.execution.ctx, key)
		if err != nil {
			increaseNumRemovesFailed(l.g, statusRecord)
			return err
		}
		removed++
	}

	lp.LogRunnerEvent(fmt.Sprintf("removed %d elements from hazelcast map '%s'", removed, mapName), log.TraceLevel)

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

func (s *defaultSleeper) sleep(sc *sleepConfig, sf evaluateTimeToSleep) {

	if sc.enabled {
		sleepDuration := sf(sc)
		lp.LogRunnerEvent(fmt.Sprintf("sleeping for %d milliseconds", sleepDuration), log.TraceLevel)
		time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	}

}

func assembleMapKey(mapNumber uint16, elementID string) string {

	return fmt.Sprintf("%s-%d-%s", client.ID(), mapNumber, elementID)

}
