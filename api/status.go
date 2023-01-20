package api

import (
	"sync"
)

type StatusType string

const (
	MapTestLoopStatusType   StatusType = "maps"
	QueueTestLoopStatusType StatusType = "queues"
	ChaosMonkeyStatusType   StatusType = "chaosMonkeys"
)

var (
	mapTestLoopStatusFunctions   sync.Map
	queueTestLoopStatusFunctions sync.Map
	chaosMonkeyStatusFunctions   sync.Map
)

func RegisterStatusType(t StatusType, source string, queryStatusFunc func() map[string]any) {

	switch t {
	case MapTestLoopStatusType:
		mapTestLoopStatusFunctions.Store(source, queryStatusFunc)
	case QueueTestLoopStatusType:
		queueTestLoopStatusFunctions.Store(source, queryStatusFunc)
	case ChaosMonkeyStatusType:
		chaosMonkeyStatusFunctions.Store(source, queryStatusFunc)
	}

}

func assembleTestLoopStatus() map[StatusType]any {

	mapTestLoopStatus := map[string]any{}
	populateWithStatus(mapTestLoopStatus, &mapTestLoopStatusFunctions)

	queueTestLoopStatus := map[string]any{}
	populateWithStatus(queueTestLoopStatus, &queueTestLoopStatusFunctions)

	chaosMonkeyStatus := map[string]any{}
	populateWithStatus(chaosMonkeyStatus, &chaosMonkeyStatusFunctions)

	return map[StatusType]any{
		MapTestLoopStatusType:   mapTestLoopStatus,
		QueueTestLoopStatusType: queueTestLoopStatus,
		ChaosMonkeyStatusType:   chaosMonkeyStatus,
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
