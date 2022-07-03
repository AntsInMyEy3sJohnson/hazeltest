package maps

import (
	"context"
	"fmt"
	"hazeltest/client"
	"hazeltest/logging"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type GetElementID func(element interface{}) string

type DeserializeElement func(element interface{}) error

type AssembleMapName func(mapNumber int) string

type TestLoop[T any] struct {
	ID                     uuid.UUID
	Source                 string
	HzClient               *hazelcast.Client
	Config                 *MapRunnerConfig
	Elements               []T
	Ctx                    context.Context
	GetElementIdFunc       GetElementID
	DeserializeElementFunc DeserializeElement
}

type TestLoopStatus struct {
	Source            string
	Config            *MapRunnerConfig
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

func (l TestLoop[T]) Run() {

	l.insertLoopWithInitialStatus()

	// TODO Introduce randomness to sleeps -- right now, they produce cyclic high CPU usage

	var wg sync.WaitGroup
	for i := 0; i < l.Config.NumMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := l.assembleMapName(i)
			logInternalStateEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			hzMap, err := l.HzClient.GetMap(l.Ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			logTimingEvent("getMap()", mapName, int(elapsed))
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			defer hzMap.Destroy(l.Ctx)
			l.runForMap(hzMap, mapName, i)
		}(i)
	}
	wg.Wait()

}

func (l TestLoop[T]) insertLoopWithInitialStatus() {

	c := l.Config

	status := &TestLoopStatus{
		Source:            l.Source,
		Config:            c,
		NumMaps:           c.NumMaps,
		NumRuns:           c.NumRuns,
		TotalRuns:         c.NumMaps * c.NumRuns,
		TotalRunsFinished: 0,
	}

	mutex.Lock()
	{
		Loops[l.ID] = status
	}
	mutex.Unlock()

}

func (l TestLoop[T]) runForMap(m *hazelcast.Map, mapName string, mapNumber int) {

	updateStep := 50
	sleepBetweenActionBatchesConfig := l.Config.SleepBetweenActionBatches
	sleepBetweenRunsConfig := l.Config.SleepBetweenRuns

	for i := 0; i < l.Config.NumRuns; i++ {
		sleep(sleepBetweenRunsConfig)
		if i > 0 && i%updateStep == 0 {
			l.increaseTotalNumRunsCompleted(updateStep)
			logInternalStateEvent(fmt.Sprintf("finished %d of %d runs for map %s in map goroutine %d -- test loop status updated", i, l.Config.NumRuns, mapName, mapNumber), log.InfoLevel)
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

func sleep(sleepConfig *SleepConfig) {

	if sleepConfig.Enabled {
		logInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds", sleepConfig.DurationMs), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.DurationMs) * time.Millisecond)
	}

}

func (l TestLoop[T]) increaseTotalNumRunsCompleted(increase int) {

	mutex.Lock()
	{
		Loops[l.ID].TotalRunsFinished += increase
	}
	mutex.Unlock()

}

func (l TestLoop[T]) ingestAll(m *hazelcast.Map, mapName string, mapNumber int) error {

	numNewlyIngested := 0
	for _, v := range l.Elements {
		key := assembleMapKey(mapNumber, l.GetElementIdFunc(v))
		containsKey, err := m.ContainsKey(l.Ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(l.Ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	logInternalStateEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) readAll(m *hazelcast.Map, mapName string, mapNumber int) error {

	for _, v := range l.Elements {
		key := assembleMapKey(mapNumber, l.GetElementIdFunc(v))
		valueFromHZ, err := m.Get(l.Ctx, key)
		if err != nil {
			return err
		}
		if valueFromHZ == nil {
			return fmt.Errorf("value retrieved from hazelcast for key '%s' was nil -- value might have been evicted or expired in hazelcast", key)
		}
		err = l.DeserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.Elements), mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) deleteSome(m *hazelcast.Map, mapName string, mapNumber int) error {

	numElementsToDelete := rand.Intn(len(l.Elements))
	deleted := 0

	elements := l.Elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(mapNumber, l.GetElementIdFunc(elements[i]))
		containsKey, err := m.ContainsKey(l.Ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(l.Ctx, key)
		if err != nil {
			return err
		}
		deleted++
	}

	logInternalStateEvent(fmt.Sprintf("deleted %d elements from hazelcast map '%s'", deleted, mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) assembleMapName(mapIndex int) string {

	c := l.Config

	mapName := c.MapBaseName
	if c.UseMapPrefix && c.MapPrefix != "" {
		mapName = fmt.Sprintf("%s%s", c.MapPrefix, mapName)
	}
	if c.AppendMapIndexToMapName {
		mapName = fmt.Sprintf("%s-%d", mapName, mapIndex)
	}
	if c.AppendClientIdToMapName {
		mapName = fmt.Sprintf("%s-%s", mapName, client.ClientID())
	}

	return mapName

}

func assembleMapKey(mapNumber int, elementID string) string {

	return fmt.Sprintf("%s-%d-%s", client.ClientID(), mapNumber, elementID)

}

func logTimingEvent(operation string, mapName string, tookMs int) {

	log.WithFields(log.Fields{
		"kind":   logging.TimingInfo,
		"client": client.ClientID(),
		"map":    mapName,
		"tookMs": tookMs,
	}).Infof("'%s' took %d ms", operation, tookMs)

}

func logInternalStateEvent(msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}

	if logLevel == log.TraceLevel {
		log.WithFields(fields).Trace(msg)
	} else {
		log.WithFields(fields).Info(msg)
	}

}

func logHzEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}).Warn(msg)

}
