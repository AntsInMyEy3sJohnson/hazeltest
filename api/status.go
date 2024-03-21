package api

import (
	"sync"
)

type StatusType string
type RunnerDataStructure string

const (
	Maps   RunnerDataStructure = "maps"
	Queues RunnerDataStructure = "queues"
)

const (
	TestLoopStatusType    StatusType = "testLoops"
	ChaosMonkeyStatusType StatusType = "chaosMonkeys"
)

var (
	mapRunnerStatusFunctions   sync.Map
	queueRunnerStatusFunction  sync.Map
	chaosMonkeyStatusFunctions sync.Map
)

func RegisterRunnerStatus(s RunnerDataStructure, source string, queryStatusFunc func() map[string]any) {

	switch s {
	case Maps:
		mapRunnerStatusFunctions.Store(source, queryStatusFunc)
	case Queues:
		queueRunnerStatusFunction.Store(source, queryStatusFunc)
	}

}

func RegisterChaosMonkeyStatus(source string, queryStatusFunc func() map[string]any) {

	chaosMonkeyStatusFunctions.Store(source, queryStatusFunc)

}

func assembleRunnerStatus() map[StatusType]any {

	mapTestLoopStatus := map[string]any{}
	populateWithStatus(mapTestLoopStatus, &mapRunnerStatusFunctions)

	queueTestLoopStatus := map[string]any{}
	populateWithStatus(queueTestLoopStatus, &queueRunnerStatusFunction)

	chaosMonkeyStatus := map[string]any{}
	populateWithStatus(chaosMonkeyStatus, &chaosMonkeyStatusFunctions)

	return map[StatusType]any{
		TestLoopStatusType: map[RunnerDataStructure]any{
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
