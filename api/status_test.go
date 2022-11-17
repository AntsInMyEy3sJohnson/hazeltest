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
			assembledStatus := assembleTestLoopStatus()

			msg := "\t\tassembled status must be empty"

			if len(assembledStatus) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

		}

		t.Log("\twhen non-empty status maps are provided for two runners")
		{
			RegisterTestLoop(sourcePokedexRunner, func() map[string]interface{} {
				return dummyStatusPokedexTestLoop
			})
			RegisterTestLoop(sourceLoadRunner, func() map[string]interface{} {
				return dummyStatusLoadTestLoop
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level elements must be equal to source their test loop has been registered with"
			checkTopLevelElement(t, sourcePokedexRunner, assembledStatus, msg)
			checkTopLevelElement(t, sourceLoadRunner, assembledStatus, msg)

			msg = "\t\tvalues associated to top-level element must mirror provided test loop status"
			checkAssociatedStatusContainsExpectedElements(t, sourcePokedexRunner, assembledStatus, dummyStatusPokedexTestLoop, msg)
			checkAssociatedStatusContainsExpectedElements(t, sourceLoadRunner, assembledStatus, dummyStatusLoadTestLoop, msg)

		}

		t.Log("\twhen function for querying status yields empty map")
		{
			testLoopStatusFunctions = sync.Map{}

			RegisterTestLoop(sourcePokedexRunner, func() map[string]interface{} {
				return map[string]interface{}{}
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level element must be equal to source the test loop has been registered with"
			checkTopLevelElement(t, sourcePokedexRunner, assembledStatus, msg)

			msg = "\t\tassociated element must be empty map"
			checkAssociatedStatusIsEmptyMap(t, sourcePokedexRunner, assembledStatus, msg)

		}

		t.Log("\twhen function for querying status yields nil")
		{
			testLoopStatusFunctions = sync.Map{}

			RegisterTestLoop(sourceLoadRunner, func() map[string]interface{} {
				return nil
			})

			assembledStatus := assembleTestLoopStatus()

			msg := "\t\ttop-level element must be equal to source the test loop has been registered with"
			checkTopLevelElement(t, sourceLoadRunner, assembledStatus, "\t\ttop-level element must be equal to source the test loop has been registered with")

			msg = "\t\tassociated element must be empty map"
			checkAssociatedStatusIsEmptyMap(t, sourceLoadRunner, assembledStatus, msg)

		}

	}

}

func checkTopLevelElement(t *testing.T, topLevelElementKey string, assembledStatus map[string]interface{}, msg string) {

	if _, ok := assembledStatus[topLevelElementKey]; ok {
		t.Log(msg, checkMark)
	} else {
		t.Error(msg, ballotX)
	}

}

func checkAssociatedStatusContainsExpectedElements(t *testing.T, topLevelElementKey string, assembledStatus map[string]interface{}, reference map[string]interface{}, msg string) {

	value, _ := assembledStatus[topLevelElementKey].(map[string]interface{})
	if equal, detail := mapsEqualInContent(reference, value); equal {
		t.Log(msg, checkMark)
	} else {
		t.Error(msg, ballotX, detail)
	}

}

func checkAssociatedStatusIsEmptyMap(t *testing.T, topLevelElementKey string, assembledStatus map[string]interface{}, msg string) {

	if value, _ := assembledStatus[topLevelElementKey]; value != nil && len(value.(map[string]interface{})) == 0 {
		t.Log(msg, checkMark)
	} else {
		t.Error(msg, ballotX)
	}

}
