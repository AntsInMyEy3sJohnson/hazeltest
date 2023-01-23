package api

import (
	"sync"
)

type StatusType string
type TestLoopDataStructure string

const (
	Maps   TestLoopDataStructure = "maps"
	Queues TestLoopDataStructure = "queues"
)

const (
	TestLoopStatusType    StatusType = "testLoops"
	ChaosMonkeyStatusType StatusType = "chaosMonkeys"
)

var (
	mapTestLoopStatusFunctions   sync.Map
	queueTestLoopStatusFunctions sync.Map
	chaosMonkeyStatusFunctions   sync.Map
)

func RegisterTestLoopStatus(s TestLoopDataStructure, source string, queryStatusFunc func() map[string]any) {

	switch s {
	case Maps:
		mapTestLoopStatusFunctions.Store(source, queryStatusFunc)
	case Queues:
		queueTestLoopStatusFunctions.Store(source, queryStatusFunc)
	}

}

func RegisterChaosMonkeyStatus(source string, queryStatusFunc func() map[string]any) {

	chaosMonkeyStatusFunctions.Store(source, queryStatusFunc)

}

func assembleTestLoopStatus() map[StatusType]any {

	mapTestLoopStatus := map[string]any{}
	populateWithStatus(mapTestLoopStatus, &mapTestLoopStatusFunctions)

	queueTestLoopStatus := map[string]any{}
	populateWithStatus(queueTestLoopStatus, &queueTestLoopStatusFunctions)

	chaosMonkeyStatus := map[string]any{}
	populateWithStatus(chaosMonkeyStatus, &chaosMonkeyStatusFunctions)

	return map[StatusType]any{
		TestLoopStatusType: map[TestLoopDataStructure]any{
			Maps:   mapTestLoopStatus,
			Queues: queueTestLoopStatus,
		},
		ChaosMonkeyStatusType: chaosMonkeyStatus,
	}

}

func populateWithStatus(target map[string]any, statusFunctionsMap *sync.Map) {

	statusFunctionsMap.Range(func(key, value any) bool {
		runnerStatus := value.(func() map[string]any)()
		if runnerStatus != nil {
			target[key.(string)] = runnerStatus
		} else {
			target[key.(string)] = map[string]any{}
		}
		return true
	})

}
