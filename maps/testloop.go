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
		init(lc *testLoopConfig[t], s sleeper, g *status.Gatherer)
		run()
	}
	sleeper interface {
		sleep(sc *sleepConfig, sf evaluateTimeToSleep)
	}
	defaultSleeper struct{}
)

type (
	batchTestLoop[t any] struct {
		config *testLoopConfig[t]
		s      sleeper
		g      *status.Gatherer
	}
	boundaryTestLoop[t any] struct {
		config            *testLoopConfig[t]
		s                 sleeper
		g                 *status.Gatherer
		currentMode       actionMode
		lastAction        mapAction
		nextAction        mapAction
		numElementsStored uint32
	}
	testLoopConfig[t any] struct {
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

func (l *boundaryTestLoop[t]) init(lc *testLoopConfig[t], s sleeper, g *status.Gatherer) {
	l.config = lc
	l.s = s
	l.g = g
}

func (l *boundaryTestLoop[t]) run() {

	runWrapper(
		l.config,
		l.g,
		assembleMapName,
		l.runForMap,
	)

}

func (l *boundaryTestLoop[t]) runForMap(m hzMap, mapName string, mapNumber uint16) {

	// Hard-code stuff to simplify things in first iteration
	upperBoundary := float32(0.8)
	lowerBoundary := float32(0.2)

	sleepBetweenRunsConfig := l.config.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.config.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.config.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		l.currentMode = checkForModeChange(upperBoundary, lowerBoundary, uint32(len(l.config.elements)), l.numElementsStored, l.currentMode)

		actionProbability := float32(0.75)
		l.nextAction = determineNextMapAction(l.currentMode, l.lastAction, actionProbability)

		if err := l.executeMapAction(m, mapName, mapNumber); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("unable to execute action '%s' on map '%s' in iteration '%d'", l.nextAction, mapName, i), log.WarnLevel)
		} else {
			l.lastAction = l.nextAction
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l *boundaryTestLoop[t]) executeMapAction(m hzMap, mapName string, mapNumber uint16) error {

	randomElement := l.config.elements[rand.Intn(len(l.config.elements))]
	elementID := l.config.getElementIdFunc(randomElement)

	key := assembleMapKey(mapNumber, elementID)

	if l.nextAction == insert || l.nextAction == remove {
		containsKey, err := m.ContainsKey(l.config.ctx, key)
		if err != nil {
			return err
		}
		if l.nextAction == insert && containsKey {
			lp.LogRunnerEvent(fmt.Sprintf("was asked to insert key '%s', but map '%s' already contained key -- no-op", key, mapName), log.TraceLevel)
			return nil
		}
		if l.nextAction == remove && !containsKey {
			lp.LogRunnerEvent(fmt.Sprintf("was asked to remove key '%s' from map '%s', but map did not contain key -- no-op", key, mapName), log.TraceLevel)
			return nil
		}
		if l.nextAction == read && !containsKey {
			lp.LogRunnerEvent(fmt.Sprintf("was asked to read key '%s' in map '%s', but map did not contain key -- no-op", key, mapName), log.TraceLevel)
		}
	}

	switch l.nextAction {
	case insert:
		if err := m.Set(l.config.ctx, key, randomElement); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to insert key '%s' into map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully inserted key '%s' into map '%s'", key, mapName), log.TraceLevel)
		}
	case remove:
		if _, err := m.Remove(l.config.ctx, key); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to remove key '%s' from map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully removed key '%s' from map '%s'", key, mapName), log.TraceLevel)
		}
	case read:
		if v, err := m.Get(l.config.ctx, key); err != nil {
			lp.LogRunnerEvent(fmt.Sprintf("failed to read key from '%s' in map '%s'", key, mapName), log.WarnLevel)
			return err
		} else {
			lp.LogRunnerEvent(fmt.Sprintf("successfully read key '%s' in map '%s': %v", key, mapName, v), log.TraceLevel)
		}

	}

	return nil

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

func (l *batchTestLoop[t]) init(lc *testLoopConfig[t], s sleeper, g *status.Gatherer) {
	l.config = lc
	l.s = s
	l.g = g
	api.RegisterTestLoopStatus(api.Maps, lc.source, l.g.AssembleStatusCopy)
}

func runWrapper[t any](c *testLoopConfig[t],
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
		l.config,
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

	sleepBetweenActionBatchesConfig := l.config.runnerConfig.sleepBetweenActionBatches
	sleepBetweenRunsConfig := l.config.runnerConfig.sleepBetweenRuns

	for i := uint32(0); i < l.config.runnerConfig.numRuns; i++ {
		l.s.sleep(sleepBetweenRunsConfig, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogRunnerEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d", i, l.config.runnerConfig.numRuns, mapName, mapNumber), log.InfoLevel)
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
	for _, v := range l.config.elements {
		key := assembleMapKey(mapNumber, l.config.getElementIdFunc(v))
		containsKey, err := m.ContainsKey(l.config.ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(l.config.ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	lp.LogRunnerEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) readAll(m hzMap, mapName string, mapNumber uint16) error {

	for _, v := range l.config.elements {
		key := assembleMapKey(mapNumber, l.config.getElementIdFunc(v))
		valueFromHZ, err := m.Get(l.config.ctx, key)
		if err != nil {
			return err
		}
		if valueFromHZ == nil {
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil -- value might have been evicted or expired in hazelcast", key)
		}
		err = l.config.deserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	lp.LogRunnerEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.config.elements), mapName), log.TraceLevel)

	return nil

}

func (l *batchTestLoop[t]) removeSome(m hzMap, mapName string, mapNumber uint16) error {

	numElementsToDelete := rand.Intn(len(l.config.elements))
	removed := 0

	elements := l.config.elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(mapNumber, l.config.getElementIdFunc(elements[i]))
		containsKey, err := m.ContainsKey(l.config.ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(l.config.ctx, key)
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
