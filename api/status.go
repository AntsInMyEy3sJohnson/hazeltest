package api

import (
	"sync"
)

var (
	runnerStatusFunctions sync.Map
)

func RegisterRunner(source string, queryStatusFunc func() map[string]interface{}) {

	runnerStatusFunctions.Store(source, queryStatusFunc)

}

func assembleTestLoopStatus() map[string]interface{} {

	testLoopStatus := make(map[string]interface{})

	runnerStatusFunctions.Range(func(key, value any) bool {
		runnerStatus := value.(func() map[string]interface{})()
		testLoopStatus[key.(string)] = runnerStatus
		return true
	})

	return testLoopStatus

}
