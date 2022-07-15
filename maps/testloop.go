package maps

import (
	"context"
	"fmt"
	"hazeltest/api"
	"hazeltest/client"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type (
	getElementID func(element interface{}) string

	deserializeElement func(element interface{}) error

	testLoop[t any] struct {
		id                     uuid.UUID
		source                 string
		hzClient               *hazelcast.Client
		config                 *runnerConfig
		elements               []t
		ctx                    context.Context
		getElementIdFunc       getElementID
		deserializeElementFunc deserializeElement
	}
)

func (l testLoop[T]) run() {

	l.insertLoopWithInitialStatus()

	// TODO Introduce randomness to sleeps -- right now, they produce cyclic high CPU usage

	var wg sync.WaitGroup
	for i := 0; i < l.config.numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := l.assembleMapName(i)
			lp.LogInternalStateEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			hzMap, err := l.hzClient.GetMap(l.ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			lp.LogTimingEvent("getMap()", mapName, int(elapsed), log.InfoLevel)
			if err != nil {
				lp.LogHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err), log.FatalLevel)
			}
			defer hzMap.Destroy(l.ctx)
			l.runForMap(hzMap, mapName, i)
		}(i)
	}
	wg.Wait()

}

func (l testLoop[T]) insertLoopWithInitialStatus() {

	c := l.config

	status := &api.TestLoopStatus{
		Source:            l.source,
		NumMaps:           c.numMaps,
		NumRuns:           c.numRuns,
		TotalRuns:         c.numMaps * c.numRuns,
		TotalRunsFinished: 0,
	}

	api.InsertInitialTestLoopStatus(l.id, status)

}

func (l testLoop[T]) runForMap(m *hazelcast.Map, mapName string, mapNumber int) {

	updateStep := 50
	sleepBetweenActionBatchesConfig := l.config.sleepBetweenActionBatches
	sleepBetweenRunsConfig := l.config.sleepBetweenRuns

	for i := 0; i < l.config.numRuns; i++ {
		sleep(sleepBetweenRunsConfig)
		if i > 0 && i%updateStep == 0 {
			l.increaseTotalNumRunsCompleted(updateStep)
			lp.LogInternalStateEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d -- test loop status updated", i, l.config.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		lp.LogInternalStateEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		sleep(sleepBetweenActionBatchesConfig)
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
		sleep(sleepBetweenActionBatchesConfig)
		err = l.deleteSome(m, mapName, mapNumber)
		if err != nil {
			lp.LogHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err), log.WarnLevel)
			continue
		}
	}

	lp.LogInternalStateEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l testLoop[T]) increaseTotalNumRunsCompleted(increase int) {

	api.IncreaseTotalNumRunsCompleted(l.id, increase)

}

func (l testLoop[T]) ingestAll(m *hazelcast.Map, mapName string, mapNumber int) error {

	numNewlyIngested := 0
	for _, v := range l.elements {
		key := assembleMapKey(mapNumber, l.getElementIdFunc(v))
		containsKey, err := m.ContainsKey(l.ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(l.ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	lp.LogInternalStateEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l testLoop[T]) readAll(m *hazelcast.Map, mapName string, mapNumber int) error {

	for _, v := range l.elements {
		key := assembleMapKey(mapNumber, l.getElementIdFunc(v))
		valueFromHZ, err := m.Get(l.ctx, key)
		if err != nil {
			return err
		}
		if valueFromHZ == nil {
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil -- value might have been evicted or expired in hazelcast", key)
		}
		err = l.deserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	lp.LogInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.elements), mapName), log.TraceLevel)

	return nil

}

func (l testLoop[T]) deleteSome(m *hazelcast.Map, mapName string, mapNumber int) error {

	numElementsToDelete := rand.Intn(len(l.elements))
	deleted := 0

	elements := l.elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(mapNumber, l.getElementIdFunc(elements[i]))
		containsKey, err := m.ContainsKey(l.ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(l.ctx, key)
		if err != nil {
			return err
		}
		deleted++
	}

	lp.LogInternalStateEvent(fmt.Sprintf("deleted %d elements from hazelcast map '%s'", deleted, mapName), log.TraceLevel)

	return nil

}

func (l testLoop[T]) assembleMapName(mapIndex int) string {

	c := l.config

	mapName := c.mapBaseName
	if c.useMapPrefix && c.mapPrefix != "" {
		mapName = fmt.Sprintf("%s%s", c.mapPrefix, mapName)
	}
	if c.appendMapIndexToMapName {
		mapName = fmt.Sprintf("%s-%d", mapName, mapIndex)
	}
	if c.appendClientIdToMapName {
		mapName = fmt.Sprintf("%s-%s", mapName, client.ID())
	}

	return mapName

}

func sleep(sleepConfig *sleepConfig) {

	if sleepConfig.enabled {
		lp.LogInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds", sleepConfig.durationMs), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.durationMs) * time.Millisecond)
	}

}

func assembleMapKey(mapNumber int, elementID string) string {

	return fmt.Sprintf("%s-%d-%s", client.ID(), mapNumber, elementID)

}
