package maps

import (
	"context"
	"fmt"
	"math/rand"
	"hazeltest/client"
	"hazeltest/logging"

	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
)

type GetElementID func(element interface{}) string

type DeserializeElement func(element interface{}) error

func IngestAll[T any](ctx context.Context, m *hazelcast.Map, elements []T, mapName string, mapNumber int, getElementIdFunc GetElementID) error {

	numNewlyIngested := 0
	for _, v := range elements {
		key := assembleMapKey(getElementIdFunc(v), mapNumber)
		containsKey, err := m.ContainsKey(ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	logInternalStateEvent(fmt.Sprintf("stored %d items in hazelcast map '%s'", numNewlyIngested, mapName), log.TraceLevel)

	return nil

}

func ReadAll[T any](ctx context.Context, m *hazelcast.Map, elements []T, mapName string, mapNumber int,
	getElementIdFunc GetElementID, deserializeElementFunc DeserializeElement) error {

	for _, v := range elements {
		valueFromHZ, err := m.Get(ctx, assembleMapKey(getElementIdFunc(v), mapNumber))
		if err != nil {
			return err
		}
		err = deserializeElementFunc(valueFromHZ)
		if err != nil {
			return err
		}
	}

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map '%s'", len(elements), mapName), log.TraceLevel)

	return nil

}

func DeleteSome[T any](ctx context.Context, m *hazelcast.Map, elements []T, mapName string, mapNumber int, getElementIdFunc GetElementID) error {

	numElementsToDelete := rand.Intn(len(elements))
	deleted := 0

	for i := 0; i < numElementsToDelete; i++ {
		key := assembleMapKey(getElementIdFunc(elements[i]), mapNumber)
		containsKey, err := m.ContainsKey(ctx, key)
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(ctx, key)
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
