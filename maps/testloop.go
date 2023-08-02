package maps

import (
	"context"
	"fmt"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/status"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type (
	evaluateTimeToSleep func(sc *sleepConfig) int
	getElementID        func(element any) string
	deserializeElement  func(element any) error
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
	boundaryTestLoop[t any] struct {
		execution         *testLoopExecution[t]
		s                 sleeper
		g                 *status.Gatherer
		currentMode       actionMode
		lastAction        mapAction
		nextAction        mapAction
		numElementsStored uint32
	}
	testLoopExecution[t any] struct {
		id                     uuid.UUID
		source                 string
		mapStore               hzMapStore
		runnerConfig           *runnerConfig
		elements               []t
		ctx                    context.Context
		getElementIdFunc       getElementID
		deserializeElementFunc deserializeElement
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
}

func (l *boundaryTestLoop[t]) run() {

	runWrapper(
		l.execution,
		l.g,
		assembleMapName,
		l.runForMap,
	)

}

func (l *boundaryTestLoop[t]) runForMap(m hzMap, mapName string, mapNumber uint16) {

	// Hard-code stuff to simplify things in first iteration
	upperBoundary := float32(0.8)
	lowerBoundary := float32(0.2)

	sleepBetweenRunsConfig := l.execution.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.execution.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.execution.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		l.currentMode = checkForModeChange(upperBoundary, lowerBoundary, uint32(len(l.execution.elements)), l.numElementsStored, l.currentMode)

		actionProbability := float32(0.75)
		l.nextAction = determineNextMapAction(l.currentMode, l.lastAction, actionProbability)

		if actionExecuted, err := l.executeMapAction(m, mapName, mapNumber); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("unable to execute action '%s' on map '%s' in iteration '%d'", l.nextAction, mapName, i), log.WarnLevel)
		} else {
			if actionExecuted {
				lp.LogRunnerEvent(fmt.Sprintf("action '%s' successfully executed on map '%s', moving to next action in upcoming loop", l.nextAction, mapName), log.TraceLevel)
				l.lastAction = l.nextAction
			} else {
				lp.LogRunnerEvent(fmt.Sprintf("action '%s' did not return error, but was not executed -- trying again in next loop", l.nextAction), log.TraceLevel)
			}
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l *boundaryTestLoop[t]) executeMapAction(m hzMap, mapName string, mapNumber uint16) (bool, error) {

	randomElement := l.execution.elements[rand.Intn(len(l.execution.elements))]
	elementID := l.execution.getElementIdFunc(randomElement)

	key := assembleMapKey(mapNumber, elementID)

	containsKey, err := m.ContainsKey(l.execution.ctx, key)
	if err != nil {
		return false, err
	}
	if l.nextAction == insert && containsKey {
		lp.LogRunnerEvent(fmt.Sprintf("was asked to insert key '%s', but map '%s' already contained key -- no-op", key, mapName), log.TraceLevel)
		return false, nil
	}
	if l.nextAction == remove && !containsKey {
		lp.LogRunnerEvent(fmt.Sprintf("was asked to remove key '%s' from map '%s', but map did not contain key -- no-op", key, mapName), log.TraceLevel)
		return false, nil
	}
	if l.nextAction == read && !containsKey {
		lp.LogRunnerEvent(fmt.Sprintf("was asked to read key '%s' in map '%s', but map did not contain key -- no-op", key, mapName), log.TraceLevel)
		return false, nil
	}

	switch l.nextAction {
	case insert:
		if err := m.Set(l.execution.ctx, key, randomElement); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to insert key '%s' into map '%s'", key, mapName), log.WarnLevel)
			return false, err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully inserted key '%s' into map '%s'", key, mapName), log.TraceLevel)
			return true, nil
		}
	case remove:
		if _, err := m.Remove(l.execution.ctx, key); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to remove key '%s' from map '%s'", key, mapName), log.WarnLevel)
			return false, err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully removed key '%s' from map '%s'", key, mapName), log.TraceLevel)
			return true, nil
		}
	case read:
		if v, err := m.Get(l.execution.ctx, key); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to read key from '%s' in map '%s'", key, mapName), log.WarnLevel)
			return false, err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully read key '%s' in map '%s': %v", key, mapName, v), log.TraceLevel)
			return true, nil
		}

	}

	return false, fmt.Errorf("unknown map action: %s", l.nextAction)

}

func determineNextMapAction(currentMode actionMode, lastAction mapAction, actionProbability float32) mapAction {

	if lastAction == insert || lastAction == remove {
		return read
	}

	hit := rand.Float32() < actionProbability

	switch currentMode {
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

func checkForModeChange(upperBoundary, lowerBoundary float32,
	totalNumberOfElements, currentlyStoredNumberOfElements uint32, currentMode actionMode) actionMode {

	total := float32(totalNumberOfElements)
	current := float32(currentlyStoredNumberOfElements)

	if current < total*lowerBoundary {
		return fill
	}

	if current > total*upperBoundary {
		return drain
	}

	return currentMode

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
	runFunc func(hzMap, string, uint16)) {

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
			runFunc(m, mapName, i)
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

func (l *batchTestLoop[t]) runForMap(m hzMap, mapName string, mapNumber uint16) {

	sleepBetweenActionBatchesConfig := l.execution.runnerConfig.sleepBetweenActionBatches
	sleepBetweenRunsConfig := l.execution.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.execution.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.execution.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		lp.LogRunnerEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc)
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		l.s.sleep(sleepBetweenActionBatchesConfig, sleepTimeFunc)
		err = l.removeSome(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l *batchTestLoop[t]) ingestAll(m hzMap, mapName string, mapNumber uint16) error {

	numNewlyIngested := 0
	for _, v := range l.execution.elements {
		key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
		containsKey, err := m.ContainsKey(l.execution.ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(l.execution.ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	lp.LogRunnerEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) readAll(m hzMap, mapName string, mapNumber uint16) error {

	for _, v := range l.execution.elements {
		key := assembleMapKey(mapNumber, l.execution.getElementIdFunc(v))
		valueFromHZ, err := m.Get(l.execution.ctx, key)
		if err != nil {
			return err
		}
		if valueFromHZ == nil {
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil -- value might have been evicted or expired in hazelcast", key)
		}
		err = l.execution.deserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.execution.elements), mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) removeSome(m hzMap, mapName string, mapNumber uint16) error {

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
