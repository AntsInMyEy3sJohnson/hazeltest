package api

import (
	"sync"
)

type ActorGroup string
type RunnerDataStructure string
type queryActorStateFunc func() map[string]any

const (
	Maps   RunnerDataStructure = "maps"
	Queues RunnerDataStructure = "queues"
)

const (
	MapRunners    ActorGroup = "mapRunners"
	QueueRunners  ActorGroup = "queueRunners"
	ChaosMonkeys  ActorGroup = "chaosMonkeys"
	StateCleaners ActorGroup = "stateCleaners"
)

var (
	mapRunnerStatusFunctions      sync.Map
	queueRunnerStatusFunction     sync.Map
	chaosMonkeyStatusFunctions    sync.Map
	statefulActorsStatusFunctions = struct {
		sync.RWMutex
		m map[ActorGroup]map[string]queryActorStateFunc
	}{m: make(map[ActorGroup]map[string]queryActorStateFunc)}
)

func RegisterStatefulActor(g ActorGroup, actorName string, queryStatusFunc func() map[string]any) {

	statefulActorsStatusFunctions.Lock()
	defer statefulActorsStatusFunctions.Unlock()

	if _, ok := statefulActorsStatusFunctions.m[g]; !ok {
		statefulActorsStatusFunctions.m[g] = make(map[string]queryActorStateFunc)
	}

	statefulActorsStatusFunctions.m[g][actorName] = queryStatusFunc

}

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

func assembleActorStatus() map[ActorGroup]map[string]any {

	statefulActorsStatusFunctions.RLock()
	defer statefulActorsStatusFunctions.RUnlock()

	result := make(map[ActorGroup]map[string]any)

	for actorGroup, registeredActors := range statefulActorsStatusFunctions.m {
		result[actorGroup] = make(map[string]any)
		for actor, actorStateQueryFunc := range registeredActors {
			result[actorGroup][actor] = actorStateQueryFunc()
		}
	}

	return result

}

func assembleRunnerStatus() map[ActorGroup]any {

	mapTestLoopStatus := map[string]any{}
	populateWithStatus(mapTestLoopStatus, &mapRunnerStatusFunctions)

	queueTestLoopStatus := map[string]any{}
	populateWithStatus(queueTestLoopStatus, &queueRunnerStatusFunction)

	chaosMonkeyStatus := map[string]any{}
	populateWithStatus(chaosMonkeyStatus, &chaosMonkeyStatusFunctions)

	return map[ActorGroup]any{
		MapRunners: map[RunnerDataStructure]any{
			Maps:   mapTestLoopStatus,
			Queues: queueTestLoopStatus,
		},
		ChaosMonkeys: chaosMonkeyStatus,
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
