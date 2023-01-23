package api

import (
	"sync"
	"testing"
)

func TestAssembleTestLoopStatus(t *testing.T) {

	t.Log("given the need to test assembly of the test loop status")
	{
		t.Log("\twhen no test loop has been registered")
		{
			resetMaps()

			assembledStatus := assembleTestLoopStatus()
			assembledTestLoopsStatus := assembledStatus[TestLoopStatusType].(map[TestLoopDataStructure]any)

			msg := "\t\ttest loops map must still contain maps status"
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

		}

		t.Log("\twhen non-empty status is provided for two map runners and two queue runners")
		{
			RegisterTestLoopStatus(Maps, sourceMapPokedexRunner, func() map[string]any {
				return dummyStatusMapPokedexTestLoop
			})
			RegisterTestLoopStatus(Maps, sourceMapLoadRunner, func() map[string]any {
				return dummyStatusMapLoadTestLoop
			})
			RegisterTestLoopStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return dummyStatusQueueTweetTestLoop
			})
			RegisterTestLoopStatus(Queues, sourceQueueLoadRunner, func() map[string]any {
				return dummyStatusQueueLoadTestLoop
			})

			assembledStatus := assembleTestLoopStatus()
			assembledTestLoopsStatus := assembledStatus[TestLoopStatusType].(map[TestLoopDataStructure]any)

			msg := "\t\ttest loops map must contain maps status"
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

		}

		t.Log("\twhen function for querying status yields empty map")
		{
			resetMaps()

			RegisterTestLoopStatus(Maps, sourceMapPokedexRunner, func() map[string]any {
				return map[string]any{}
			})
			RegisterTestLoopStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return map[string]any{}
			})

			assembledStatus := assembleTestLoopStatus()
			assembledTestLoopsStatus := assembledStatus[TestLoopStatusType].(map[TestLoopDataStructure]any)

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

		}

		t.Log("\twhen function for querying status yields nil")
		{
			resetMaps()

			RegisterTestLoopStatus(Maps, sourceMapLoadRunner, func() map[string]any {
				return nil
			})
			RegisterTestLoopStatus(Queues, sourceQueueTweetRunner, func() map[string]any {
				return nil
			})

			assembledStatus := assembleTestLoopStatus()
			assembledTestLoopsStatus := assembledStatus[TestLoopStatusType].(map[TestLoopDataStructure]any)

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

		}

	}

}

func resetMaps() {
	mapTestLoopStatusFunctions = sync.Map{}
	queueTestLoopStatusFunctions = sync.Map{}

}
