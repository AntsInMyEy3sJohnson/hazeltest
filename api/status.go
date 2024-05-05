package api

import (
	"sync"
)

type (
	statefulActorTracker struct {
		sync.RWMutex
		m map[ActorGroup]map[string]queryActorStateFunc
	}
	ActorGroup          string
	RunnerDataStructure string
	queryActorStateFunc func() map[string]any
)

const (
	MapRunners    ActorGroup = "mapRunners"
	QueueRunners  ActorGroup = "queueRunners"
	ChaosMonkeys  ActorGroup = "chaosMonkeys"
	StateCleaners ActorGroup = "stateCleaners"
)

var (
	availableActorGroups = []ActorGroup{MapRunners, QueueRunners, ChaosMonkeys, StateCleaners}
	tracker              = newStatefulActorTracker()
)

func newStatefulActorTracker() *statefulActorTracker {

	t := &statefulActorTracker{m: make(map[ActorGroup]map[string]queryActorStateFunc)}

	for _, g := range availableActorGroups {
		t.m[g] = make(map[string]queryActorStateFunc)
	}

	return t

}

func RegisterStatefulActor(g ActorGroup, actorName string, queryStatusFunc func() map[string]any) {

	tracker.Lock()
	defer tracker.Unlock()

	if _, ok := tracker.m[g]; !ok {
		tracker.m[g] = make(map[string]queryActorStateFunc)
	}

	tracker.m[g][actorName] = queryStatusFunc

}

func assembleActorStatus() map[ActorGroup]map[string]any {

	tracker.RLock()
	defer tracker.RUnlock()

	result := make(map[ActorGroup]map[string]any)

	for actorGroup, registeredActors := range tracker.m {
		result[actorGroup] = make(map[string]any)
		for actor, actorStateQueryFunc := range registeredActors {
			result[actorGroup][actor] = actorStateQueryFunc()
		}
	}

	return result

}
