package api

import (
	"testing"
)

func TestNewStatefulActorTracker(t *testing.T) {

	t.Log("given a factory function for the assembly of a new stateful actor tracker")
	{
		t.Log("\twhen assembly is invoked")
		{
			instance := newStatefulActorTracker()

			msg := "\t\tinstance must carry map populated with empty sub-maps for each actor group"

			for _, v := range availableActorGroups {
				if subMap, ok := instance.m[v]; ok && len(subMap) == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestRegisterStatefulActor(t *testing.T) {

	t.Log("given a function to allow a stateful actor to register itself with its state query function")
	{
		t.Log("\twhen stateful actors register with status types for which no actors have previously registered")
		{
			actorGroup := MapRunners
			actorName := "pokedex"
			testKey := "awesome-key"
			testFn := func() map[string]any {
				return map[string]any{
					testKey: "awesome-value",
				}
			}
			RegisterStatefulActor(actorGroup, actorName, testFn)

			msg := "\t\tlist of stateful actors must contain entry for actor group"
			if _, ok := tracker.m[actorGroup]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tactor must have been stored beneath actor group"
			actorGroupList := tracker.m[actorGroup]

			if _, ok := actorGroupList[actorName]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tquery status function must have been associated with actor"
			if _, ok := actorGroupList[actorName]()[testKey]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestAssembleActorStatus(t *testing.T) {

	t.Log("given a function to assemble a status representation for all registered actors")
	{
		t.Log("\twhen multiple actors from multiple actor groups have registered")
		{
			tracker = newStatefulActorTracker()

			actors := map[ActorGroup]map[string]map[string]any{
				MapRunners: {
					"pokedex": {
						"aragorn": "the king",
					},
				},
				StateCleaners: {
					"maps": {
						"gimli":             "dwarf (never to be thrown)",
						"legolas":           "dude with never-ending supply of arrows to shoot (how convenient)",
						"gandalf the white": "actually has power here",
					},
					"queues": {
						"pippin": "fool of a took (would like to have second breakfast)",
						"merry":  "thinks aragorn does not know about second breakfast",
					},
					"topics": {
						"frodo": "the ring bearer",
						"sam":   "the real mvp",
					},
				},
			}

			for actorGroup, actorsWithinGroup := range actors {
				for actor, actorStateMap := range actorsWithinGroup {
					registerFunc := func(m map[string]any) queryActorStateFunc {
						return func() map[string]any {
							return m
						}
					}(actorStateMap)
					RegisterStatefulActor(actorGroup, actor, registerFunc)
				}
			}

			msg := "\t\tthere must be one sub-map for each available actor group"

			result := assembleActorStatus()

			if len(result) == len(availableActorGroups) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(result))
			}

			msg = "\t\tfor each actor group with a non-zero number of registered actors, the result must contain the assembled status"
			for actorGroup, actorsWithinGroup := range actors {
				for actor, actorStateMap := range actorsWithinGroup {
					for actorStateKey, actorStateValue := range actorStateMap {
						assembledActorState := result[actorGroup][actor].(map[string]any)
						if assembledActorState[actorStateKey] == actorStateValue {
							t.Log(msg, checkMark, actorGroup, actor, actorStateKey)
						} else {
							t.Fatal(msg, ballotX, actorGroup, actor, actorStateKey)
						}
					}
				}
			}

			msg = "\t\tfor each remaining actor group no actor has registered for, result must contain empty map"
			var groupsWithoutRegisteredActor []ActorGroup
			for _, v := range availableActorGroups {
				if _, ok := actors[v]; !ok {
					groupsWithoutRegisteredActor = append(groupsWithoutRegisteredActor, v)
				}
			}

			for _, v := range groupsWithoutRegisteredActor {
				if len(result[v]) == 0 {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}

		}
		t.Log("\twhen no actor has registered yet")
		{
			tracker = newStatefulActorTracker()

			result := assembleActorStatus()

			msg := "\t\tresult must contain one entry for each available actor group"
			if len(result) == len(availableActorGroups) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tsub-map for each actor group must be empty"
			for _, v := range availableActorGroups {
				if len(result[v]) == 0 {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}

		}
	}

}
