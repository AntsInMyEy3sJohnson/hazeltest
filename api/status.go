package api

import (
	"sync"
)

var (
	testLoopStatusFunctions sync.Map
)

func RegisterTestLoop(source string, queryStatusFunc func() map[string]interface{}) {

	testLoopStatusFunctions.Store(source, queryStatusFunc)

}

func assembleTestLoopStatus() map[string]interface{} {

	testLoopStatus := make(map[string]interface{})

	testLoopStatusFunctions.Range(func(key, value any) bool {
		runnerStatus := value.(func() map[string]interface{})()
		testLoopStatus[key.(string)] = runnerStatus
		return true
	})

	return testLoopStatus

}
