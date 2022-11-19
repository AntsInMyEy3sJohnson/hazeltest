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

			msg := "\t\ttop-level map must still contain keys and map and queue status"
			checkTopLevelElement(t, Maps, assembledStatus, msg)
			checkTopLevelElement(t, Queues, assembledStatus, msg)

			msg = "\t\tstatus registered for both maps and queues must be empty"

			if len(assembledStatus["maps"].(map[string]interface{})) == 0 && len(assembledStatus["queues"].(map[string]interface{})) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen non-empty status is provided for two map runners and two queue runners")
		{
			RegisterTestLoop(Maps, sourceMapPokedexRunner, func() map[string]interface{} {
				return dummyStatusMapPokedexTestLoop
			})
			RegisterTestLoop(Maps, sourceMapLoadRunner, func() map[string]interface{} {
				return dummyStatusMapLoadTestLoop
			})
			RegisterTestLoop(Queues, sourceQueueTweetRunner, func() map[string]interface{} {
				return dummyStatusQueueTweetTestLoop
			})
			RegisterTestLoop(Queues, sourceQueueLoadRunner, func() map[string]interface{} {
				return dummyStatusQueueLoadTestLoop
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level map must contain keys for map and queue test loops"
			checkTopLevelElement(t, Maps, assembledStatus, msg)
			checkTopLevelElement(t, Queues, assembledStatus, msg)

			msg = "\t\tmaps map must contain keys for both registered map-type sources"
			assembledStatusMaps := assembledStatus[Maps].(map[string]interface{})
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
			assembledStatusQueues := assembledStatus[Queues].(map[string]interface{})
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
			assembledStatusMapPokedexTestLoop := assembledStatusMaps[sourceMapPokedexRunner].(map[string]interface{})
			if ok, detail := mapsEqualInContent(dummyStatusMapPokedexTestLoop, assembledStatusMapPokedexTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
			msg = "\t\tvalues contained in assembled status must mirror provided test loop status"
			assembledStatusMapLoadTestLoop := assembledStatusMaps[sourceMapLoadRunner].(map[string]interface{})
			if ok, detail := mapsEqualInContent(dummyStatusMapLoadTestLoop, assembledStatusMapLoadTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tvalues contained in assembled status for queues must mirror provided test loop status"
			assembledStatusQueueTweetTestLoop := assembledStatusQueues[sourceQueueTweetRunner].(map[string]interface{})
			if ok, detail := mapsEqualInContent(dummyStatusQueueTweetTestLoop, assembledStatusQueueTweetTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
			assembledStatusQueueLoadTestLoop := assembledStatusQueues[sourceQueueLoadRunner].(map[string]interface{})
			if ok, detail := mapsEqualInContent(dummyStatusQueueLoadTestLoop, assembledStatusQueueLoadTestLoop); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}

		t.Log("\twhen function for querying status yields empty map")
		{
			resetMaps()

			RegisterTestLoop(Maps, sourceMapPokedexRunner, func() map[string]interface{} {
				return map[string]interface{}{}
			})
			RegisterTestLoop(Queues, sourceQueueTweetRunner, func() map[string]interface{} {
				return map[string]interface{}{}
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level map must contain keys for map and queue test loops"
			checkTopLevelElement(t, Maps, assembledStatus, msg)
			checkTopLevelElement(t, Queues, assembledStatus, msg)

			msg = "\t\tmaps status must contain key for registered map test loop"

			assembledStatusMaps := assembledStatus[Maps].(map[string]interface{})
			if _, ok := assembledStatusMaps[sourceMapPokedexRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			assembledStatusQueues := assembledStatus[Queues].(map[string]interface{})
			if _, ok := assembledStatusQueues[sourceQueueTweetRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			msg = "\t\tstatus map must be empty"
			if len(assembledStatusMaps[sourceMapPokedexRunner].(map[string]interface{})) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			if len(assembledStatusQueues[sourceQueueTweetRunner].(map[string]interface{})) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

		}

		t.Log("\twhen function for querying status yields nil")
		{
			resetMaps()

			RegisterTestLoop(Maps, sourceMapLoadRunner, func() map[string]interface{} {
				return nil
			})
			RegisterTestLoop(Queues, sourceQueueTweetRunner, func() map[string]interface{} {
				return nil
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level element must be equal to source the test loop has been registered with"
			checkTopLevelElement(t, Maps, assembledStatus, msg)
			checkTopLevelElement(t, Queues, assembledStatus, msg)

			msg = "\t\tmaps status must contain key for registered map test loop"

			assembledStatusMaps := assembledStatus[Maps].(map[string]interface{})
			if _, ok := assembledStatusMaps[sourceMapLoadRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			assembledStatusQueues := assembledStatus[Queues].(map[string]interface{})
			if _, ok := assembledStatusQueues[sourceQueueTweetRunner]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceQueueTweetRunner)
			}

			msg = "\t\tstatus map must be empty"
			if len(assembledStatusMaps[sourceMapLoadRunner].(map[string]interface{})) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			if len(assembledStatusQueues[sourceQueueTweetRunner].(map[string]interface{})) == 0 {
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

func checkTopLevelElement(t *testing.T, topLevelElementKey TestLoopType, assembledStatus map[TestLoopType]interface{}, msg string) {

	if _, ok := assembledStatus[topLevelElementKey]; ok {
		t.Log(msg, checkMark)
	} else {
		t.Fatal(msg, ballotX)
	}

}
