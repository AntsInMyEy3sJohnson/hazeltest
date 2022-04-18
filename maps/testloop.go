package maps

import (
	"context"
	"fmt"
	"hazeltest/client"
	"hazeltest/logging"
	"math/rand"

	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type GetElementID func(element interface{}) string

type DeserializeElement func(element interface{}) error

type TestLoop[T any] struct {
	Elements               []T
	Ctx                    context.Context
	HzMap                  *hazelcast.Map
	GetElementIdFunc       GetElementID
	DeserializeElementFunc DeserializeElement
}

func (l TestLoop[T]) Run(numRuns int, mapName string, mapNumber int) {

	for i := 0; i < numRuns; i++ {
		if i > 0 && i%100 == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d runs for map %s in map goroutine %d", i, mapName, mapNumber), log.InfoLevel)
		}
		logInternalStateEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := l.IngestAll(mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		err = l.ReadAll(mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
		err = l.DeleteSome(mapName, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
	}

}

func (l TestLoop[T]) IngestAll(mapName string, mapNumber int) error {

	numNewlyIngested := 0
	for _, v := range l.Elements {
		key := assembleMapKey(l.GetElementIdFunc(v), mapNumber)
		containsKey, err := l.HzMap.ContainsKey(l.Ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = l.HzMap.Set(l.Ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	logInternalStateEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) ReadAll(mapName string, mapNumber int) error {

	for _, v := range l.Elements {
		valueFromHZ, err := l.HzMap.Get(l.Ctx, assembleMapKey(l.GetElementIdFunc(v), mapNumber))
		if err != nil {
			return err
		}
		err = l.DeserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(l.Elements), mapName), log.TraceLevel)

	return nil

}

func (l TestLoop[T]) DeleteSome(mapName string, mapNumber int) error {

	numElementsToDelete := rand.Intn(len(l.Elements))
	deleted := 0

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(l.GetElementIdFunc(l.Elements[i]), mapNumber)
		containsKey, err := l.HzMap.ContainsKey(l.Ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = l.HzMap.Remove(l.Ctx, key)
		if err != nil {
			return err
		}
		deleted++
	}

	logInternalStateEvent(fmt.Sprintf("deleted %d elements from hazelcast map '%s'", deleted, mapName), log.TraceLevel)

	return nil

}

func assembleMapKey(id string, mapNumber int) string {

	return fmt.Sprintf("%s-%s", client.ClientID(), id)

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
