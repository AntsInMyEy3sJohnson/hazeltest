package api

import (
	"sync"
)

type TestLoopType string

const (
	MapTestLoopType   TestLoopType = "maps"
	QueueTestLoopType TestLoopType = "queues"
)

var (
	mapTestLoopStatusFunctions   sync.Map
	queueTestLoopStatusFunctions sync.Map
)

func RegisterTestLoop(t TestLoopType, source string, queryStatusFunc func() map[string]any) {

	if t == MapTestLoopType {
		mapTestLoopStatusFunctions.Store(source, queryStatusFunc)
	} else {
		queueTestLoopStatusFunctions.Store(source, queryStatusFunc)
	}

}

func assembleTestLoopStatus() map[TestLoopType]any {

	mapTestLoopStatus := map[string]any{}
	populateWithRunnerStatus(mapTestLoopStatus, &mapTestLoopStatusFunctions)

	queueTestLoopStatus := map[string]any{}
	populateWithRunnerStatus(queueTestLoopStatus, &queueTestLoopStatusFunctions)

	return map[TestLoopType]any{
		MapTestLoopType:   mapTestLoopStatus,
		QueueTestLoopType: queueTestLoopStatus,
	}

}

func populateWithRunnerStatus(target map[string]any, statusFunctionsMap *sync.Map) {

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
