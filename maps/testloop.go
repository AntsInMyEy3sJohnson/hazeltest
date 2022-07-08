package maps

import (
	"context"
	"fmt"
	"hazeltest/client"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type getElementID func(element interface{}) string

type deserializeElement func(element interface{}) error

type testLoop[t any] struct {
	id                     uuid.UUID
	source                 string
	hzClient               *hazelcast.Client
	config                 *runnerConfig
	elements               []t
	ctx                    context.Context
	getElementIdFunc       getElementID
	deserializeElementFunc deserializeElement
}

type TestLoopStatus struct {
	Source            string
	Config            *runnerConfig
	NumMaps           int
	NumRuns           int
	TotalRuns         int
	TotalRunsFinished int
}

var (
	Loops map[uuid.UUID]*TestLoopStatus
	mutex sync.Mutex
)

func init() {

	Loops = make(map[uuid.UUID]*TestLoopStatus)

}

func (l testLoop[T]) run() {

	l.insertLoopWithInitialStatus()

	// TODO Introduce randomness to sleeps -- right now, they produce cyclic high CPU usage

	var wg sync.WaitGroup
	for i := 0; i < l.config.numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := l.assembleMapName(i)
			logInternalStateEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			hzMap, err := l.hzClient.GetMap(l.ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			logTimingEvent("getMap()", mapName, int(elapsed))
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			defer hzMap.Destroy(l.ctx)
			l.runForMap(hzMap, mapName, i)
		}(i)
	}
	wg.Wait()

}

func (l testLoop[T]) insertLoopWithInitialStatus() {

	c := l.config

	status := &TestLoopStatus{
		Source:            l.source,
		Config:            c,
		NumMaps:           c.numMaps,
		NumRuns:           c.numRuns,
		TotalRuns:         c.numMaps * c.numRuns,
		TotalRunsFinished: 0,
	}

	mutex.Lock()
	{
		Loops[l.id] = status
	}
	mutex.Unlock()

}

func (l testLoop[T]) runForMap(m *hazelcast.Map, mapName string, mapNumber int) {

	updateStep := 50
	sleepBetweenActionBatchesConfig := l.config.sleepBetweenActionBatches
	sleepBetweenRunsConfig := l.config.sleepBetweenRuns

	for i := 0; i < l.config.numRuns; i++ {
		sleep(sleepBetweenRunsConfig)
		if i > 0 && i%updateStep == 0 {
			l.increaseTotalNumRunsCompleted(updateStep)
			logInternalStateEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d -- test loop status updated", i, l.config.numRuns, mapName, mapNumber), log.InfoLevel)
		}
		logInternalStateEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		sleep(sleepBetweenActionBatchesConfig)
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		sleep(sleepBetweenActionBatchesConfig)
		err = l.deleteSome(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
	}

	logInternalStateEvent(fmt.Sprintf("map test loop done on map '%s' in map goroutine %d", mapName, mapNumber), log.InfoLevel)

}

func (l testLoop[T]) increaseTotalNumRunsCompleted(increase int) {

	mutex.Lock()
	{
		Loops[l.id].TotalRunsFinished += increase
	}
	mutex.Unlock()

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

	logInternalStateEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

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

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.elements), mapName), log.TraceLevel)

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

	logInternalStateEvent(fmt.Sprintf("deleted %d elements from hazelcast map '%s'", deleted, mapName), log.TraceLevel)

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
		mapName = fmt.Sprintf("%s-%s", mapName, client.ClientID())
	}

	return mapName

}

func sleep(sleepConfig *sleepConfig) {

	if sleepConfig.enabled {
		logInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds", sleepConfig.durationMs), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.durationMs) * time.Millisecond)
	}

}

func assembleMapKey(mapNumber int, elementID string) string {

	return fmt.Sprintf("%s-%d-%s", client.ClientID(), mapNumber, elementID)

}
