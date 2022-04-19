package maps

import (
	"context"
	"fmt"
	"hazeltest/client"
	"hazeltest/logging"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type GetElementID func(element interface{}) string

type DeserializeElement func(element interface{}) error

type AssembleMapName func(mapNumber int) string

type TestLoop[T any] struct {
	HzClient               *hazelcast.Client
	RunnerConfig           *MapRunnerConfig
	NumMaps                int
	NumRuns                int
	Elements               *[]T
	Ctx                    context.Context
	GetElementIdFunc       GetElementID
	DeserializeElementFunc DeserializeElement
}

func (l TestLoop[T]) Run() {

	var wg sync.WaitGroup
	for i := 0; i < l.NumMaps; i++ {
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
			l.runForMap(hzMap, l.NumRuns, mapName, i)
		}(i)
	}
	wg.Wait()

}

func (l TestLoop[T]) runForMap(m *hazelcast.Map, numRuns int, mapName string, mapNumber int) {

	for i := 0; i < numRuns; i++ {
		if i > 0 && i%50 == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d runs for map %s in map goroutine %d", i, mapName, mapNumber), log.InfoLevel)
		}
		logInternalStateEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.ingestAll(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		err = l.readAll(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		err = l.deleteSome(m, mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
	}

}

func (l TestLoop[T]) ingestAll(m *hazelcast.Map, mapName string, mapNumber int) error {

	numNewlyIngested := 0
	for _, v := range *l.Elements {
		key := assembleMapKey(l.GetElementIdFunc(v), mapNumber)
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

	for _, v := range *l.Elements {
		valueFromHZ, err := m.Get(l.Ctx, assembleMapKey(l.GetElementIdFunc(v), mapNumber))
		if err != nil {
			return err
		}
		err = l.DeserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(*l.Elements), mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) deleteSome(m *hazelcast.Map, mapName string, mapNumber int) error {

	numElementsToDelete := rand.Intn(len(*l.Elements))
	deleted := 0

	elements := *l.Elements

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(l.GetElementIdFunc(elements[i]), mapNumber)
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

	c := l.RunnerConfig

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

func assembleMapKey(id string, mapNumber int) string {

	return fmt.Sprintf("%s-%s", client.ClientID(), id)

}

func logTimingEvent(operation string, mapName string, tookMs int) {

	log.WithFields(log.Fields{
		"kind":   logging.TimingInfo,
		"client": client.ClientID(),
		"map": mapName,
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
	}).Fatal(msg)

}
