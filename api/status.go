package api

import (
	"sync"
)

type TestLoopType string

const (
	Maps   TestLoopType = "maps"
	Queues TestLoopType = "queues"
)

var (
	mapTestLoopStatusFunctions   sync.Map
	queueTestLoopStatusFunctions sync.Map
)

func RegisterTestLoop(t TestLoopType, source string, queryStatusFunc func() map[string]interface{}) {

	if t == Maps {
		mapTestLoopStatusFunctions.Store(source, queryStatusFunc)
	} else {
		queueTestLoopStatusFunctions.Store(source, queryStatusFunc)
	}

}

func assembleTestLoopStatus() map[TestLoopType]interface{} {

	mapTestLoopStatus := map[string]interface{}{}
	populateWithRunnerStatus(mapTestLoopStatus, &mapTestLoopStatusFunctions)

	queueTestLoopStatus := map[string]interface{}{}
	populateWithRunnerStatus(queueTestLoopStatus, &queueTestLoopStatusFunctions)

	return map[TestLoopType]interface{}{
		Maps:   mapTestLoopStatus,
		Queues: queueTestLoopStatus,
	}

}

func populateWithRunnerStatus(target map[string]interface{}, statusFunctionsMap *sync.Map) {

	statusFunctionsMap.Range(func(key, value any) bool {
		runnerStatus := value.(func() map[string]interface{})()
		if runnerStatus != nil {
			target[key.(string)] = runnerStatus
		} else {
			target[key.(string)] = map[string]interface{}{}
		}
		return true
	})

}
