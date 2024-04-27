package api

import (
	"sync"
	"testing"
)

func TestRegisterStatefulActor(t *testing.T) {

	t.Log("given a function to allow a stateful actor to register itself with its state query function")
	{
		t.Log("\twhen stateful actors register with status types for which no actors have previously registered")
		{
			actors := map[ActorGroup]map[string]map[string]any{
				MapRunners: {
					"pokedex": {
						"aragorn": "the king",
					},
				},
				StateCleaners: {
					"maps": {
						"gimli": "dwarf (never to be thrown)",
					},
					"queues": {
						"pippin": "fool of a took",
					},
				},
			}

			for k1, v1 := range actors {
				for k2, v2 := range v1 {
					RegisterStatefulActor(k1, k2, func() map[string]any {
						return v2
					})
				}
			}

			msg := "\t\tthere must be one sub-map for each actor group"

			result := assembleActorStatus()

			if len(result) == 2 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(result))
			}

		}
	}

}

func TestAssembleTestLoopStatus(t *testing.T) {

	t.Log("given the test loop status assembly function")
	{
		t.Log("\twhen no test loop has been registered")
		{
			resetMaps()

			assembledStatus := assembleRunnerStatus()
			msg := "\t\tstatus map must still contain two elements"
			if len(assembledStatus) == 2 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(assembledStatus))
			}

			msg = "\t\tstatus map must contain one key for test loop status and chaos monkey status each"
			if _, ok := assembledStatus[MapRunners]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, MapRunners)
			}
			if _, ok := assembledStatus[ChaosMonkeys]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, ChaosMonkeys)
			}

			assembledTestLoopsStatus := assembledStatus[MapRunners].(map[RunnerDataStructure]any)

			msg = "\t\ttest loops map must still contain maps status"
			if _, ok := assembledTestLoopsStatus[Maps]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loops map must still contains queues status"
			if _, ok := assembledTestLoopsStatus[Queues]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus registered for both maps and queues must be empty"
			if len(assembledTestLoopsStatus[Maps].(map[string]any)) == 0 && len(assembledTestLoopsStatus[Queues].(map[string]any)) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus for chaos monkeys must be empty"
			chaosMonkeyStatus := assembledStatus[ChaosMonkeys].(map[string]any)
			if len(chaosMonkeyStatus) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(chaosMonkeyStatus))
			}

		}

		t.Log("\twhen non-empty status is provided for two map runners, two queue runners, and one chaos monkey")
		{
			RegisterRunnerStatus(Maps, sourceMapPokedexRunner, func() map[string]any {
				return dummyStatusMapPokedexTestLoop
			})
			RegisterRunnerStatus(Maps, sourceMapLoadRunner, func() map[string]any {
				return dummyStatusMapLoadTestLoop
			})
			RegisterRunnerStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return dummyStatusQueueTweetTestLoop
			})
			RegisterRunnerStatus(Queues, sourceQueueLoadRunner, func() map[string]any {
				return dummyStatusQueueLoadTestLoop
			})
			RegisterChaosMonkeyStatus(sourceChaosMonkeyMemberKiller, func() map[string]any {
				return dummyStatusMemberKillerMonkey
			})

			assembledStatus := assembleRunnerStatus()
			msg := "\t\tassembled map must contain two top-level keys"
			if len(assembledStatus) == 2 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(assembledStatus))
			}

			msg = "\t\tassembled map must contain one top-level key for test loop status and chaos monkey status each"
			if _, ok := assembledStatus[MapRunners]; ok {
				t.Log(msg, checkMark, MapRunners)
			} else {
				t.Fatal(msg, ballotX, MapRunners)
			}
			if _, ok := assembledStatus[ChaosMonkeys]; ok {
				t.Log(msg, checkMark, ChaosMonkeys)
			} else {
				t.Fatal(msg, ballotX, ChaosMonkeys)
			}

			assembledTestLoopsStatus := assembledStatus[MapRunners].(map[RunnerDataStructure]any)

			msg = "\t\ttest loops map must contain maps status"
			if _, ok := assembledTestLoopsStatus[Maps]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
			msg = "\t\ttest loops map must contain queues status"
			if _, ok := assembledTestLoopsStatus[Queues]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmaps map must contain keys for both registered map-type sources"
			assembledStatusMaps := assembledTestLoopsStatus[Maps].(map[string]any)
			if _, ok := assembledStatusMaps[sourceMapPokedexRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}
			if _, ok := assembledStatusMaps[sourceMapLoadRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			msg = "\t\tqueues map must contain keys for both registered queue-type source"
			assembledStatusQueues := assembledTestLoopsStatus[Queues].(map[string]any)
			if _, ok := assembledStatusQueues[sourceQueueTweetRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}
			if _, ok := assembledStatusQueues[sourceQueueLoadRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueLoadRunner)
			}

			msg = "\t\tvalues contained in assembled status for maps must mirror provided test loop status"
			assembledStatusMapPokedexTestLoop := assembledStatusMaps[sourceMapPokedexRunner].(map[string]any)
			if ok, detail := mapsEqualInContent(dummyStatusMapPokedexTestLoop, assembledStatusMapPokedexTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
			msg = "\t\tvalues contained in assembled status must mirror provided test loop status"
			assembledStatusMapLoadTestLoop := assembledStatusMaps[sourceMapLoadRunner].(map[string]any)
			if ok, detail := mapsEqualInContent(dummyStatusMapLoadTestLoop, assembledStatusMapLoadTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tvalues contained in assembled status for queues must mirror provided test loop status"
			assembledStatusQueueTweetTestLoop := assembledStatusQueues[sourceQueueTweetRunner].(map[string]any)
			if ok, detail := mapsEqualInContent(dummyStatusQueueTweetTestLoop, assembledStatusQueueTweetTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
			assembledStatusQueueLoadTestLoop := assembledStatusQueues[sourceQueueLoadRunner].(map[string]any)
			if ok, detail := mapsEqualInContent(dummyStatusQueueLoadTestLoop, assembledStatusQueueLoadTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tchaos monkey status must contain exactly one key"
			chaosMonkeyStatus := assembledStatus[ChaosMonkeys].(map[string]any)
			if len(chaosMonkeyStatus) == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tassembled member killer status must mirror given test status"
			memberKillerStatus := chaosMonkeyStatus[sourceChaosMonkeyMemberKiller].(map[string]any)
			if ok, detail := mapsEqualInContent(dummyStatusMemberKillerMonkey, memberKillerStatus); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}

		t.Log("\twhen function for querying status yields empty map")
		{
			resetMaps()

			RegisterRunnerStatus(Maps, sourceMapPokedexRunner, func() map[string]any {
				return map[string]any{}
			})
			RegisterRunnerStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return map[string]any{}
			})
			RegisterChaosMonkeyStatus(sourceChaosMonkeyMemberKiller, func() map[string]any {
				return map[string]any{}
			})

			assembledStatus := assembleRunnerStatus()
			assembledTestLoopsStatus := assembledStatus[MapRunners].(map[RunnerDataStructure]any)

			msg := "\t\ttest loops map must contain key for maps"
			if _, ok := assembledTestLoopsStatus[Maps]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loops map must contain key for queues"
			if _, ok := assembledTestLoopsStatus[Queues]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmaps status must contain key for registered map test loop"
			assembledStatusMaps := assembledTestLoopsStatus[Maps].(map[string]any)
			if _, ok := assembledStatusMaps[sourceMapPokedexRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			assembledStatusQueues := assembledTestLoopsStatus[Queues].(map[string]any)
			if _, ok := assembledStatusQueues[sourceQueueTweetRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			msg = "\t\tstatus map must be empty"
			if len(assembledStatusMaps[sourceMapPokedexRunner].(map[string]any)) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			if len(assembledStatusQueues[sourceQueueTweetRunner].(map[string]any)) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			msg = "\t\tchaos monkey status must contain exactly one key"
			assembledChaosMonkeyStatus := assembledStatus[ChaosMonkeys].(map[string]any)
			if len(assembledChaosMonkeyStatus) == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(assembledChaosMonkeyStatus))
			}

			msg = "\t\tmember killer status must be empty"
			memberKillerStatus := assembledChaosMonkeyStatus[sourceChaosMonkeyMemberKiller].(map[string]any)
			if len(memberKillerStatus) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, len(memberKillerStatus))
			}
		}

		t.Log("\twhen function for querying status yields nil")
		{
			resetMaps()

			RegisterRunnerStatus(Maps, sourceMapLoadRunner, func() map[string]any {
				return nil
			})
			RegisterRunnerStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return nil
			})
			RegisterChaosMonkeyStatus(sourceChaosMonkeyMemberKiller, func() map[string]any {
				return nil
			})

			assembledStatus := assembleRunnerStatus()
			assembledTestLoopsStatus := assembledStatus[MapRunners].(map[RunnerDataStructure]any)

			msg := "\t\ttest loops map must still contain top-level key for map status"
			if _, ok := assembledTestLoopsStatus[Maps]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttest loops map must still contain top-level key for queue status"
			if _, ok := assembledTestLoopsStatus[Queues]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmaps status must contain key for registered map test loop"
			assembledStatusMaps := assembledTestLoopsStatus[Maps].(map[string]any)
			if _, ok := assembledStatusMaps[sourceMapLoadRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			assembledStatusQueues := assembledTestLoopsStatus[Queues].(map[string]any)
			if _, ok := assembledStatusQueues[sourceQueueTweetRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			msg = "\t\tstatus map must be empty"
			if len(assembledStatusMaps[sourceMapLoadRunner].(map[string]any)) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			if len(assembledStatusQueues[sourceQueueTweetRunner].(map[string]any)) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			assembledChaosMonkeyStatus := assembledStatus[ChaosMonkeys].(map[string]any)
			msg = "\t\tchaos monkey status must still contain top-level key for member killer monkey"
			if _, ok := assembledChaosMonkeyStatus[sourceChaosMonkeyMemberKiller]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmember killer status must be empty"
			memberKillerStatus := assembledChaosMonkeyStatus[sourceChaosMonkeyMemberKiller].(map[string]any)
			if len(memberKillerStatus) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

	}

}

func resetMaps() {
	mapRunnerStatusFunctions = sync.Map{}
	queueRunnerStatusFunction = sync.Map{}
	chaosMonkeyStatusFunctions = sync.Map{}
}
