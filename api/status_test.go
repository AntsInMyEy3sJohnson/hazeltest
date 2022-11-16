package api

import (
	"fmt"
	"sync"
	"testing"
)

const (
	statusKeyNumMaps        = "numMaps"
	statusKeyNumRuns        = "numRuns"
	statusKeyTotalRuns      = "totalRuns"
	statusKeyRunnerFinished = "runnerFinished"
)

const (
	sourcePokedexRunner = "pokedexrunner"
	sourceLoadRunner    = "loadrunner"
)

var (
	dummyStatusPokedexTestLoop = map[string]interface{}{
		statusKeyNumMaps:        10,
		statusKeyNumRuns:        1000,
		statusKeyTotalRuns:      10 * 1000,
		statusKeyRunnerFinished: false,
	}
	dummyStatusLoadTestLoop = map[string]interface{}{
		statusKeyNumMaps:        5,
		statusKeyNumRuns:        100,
		statusKeyTotalRuns:      5 * 100,
		statusKeyRunnerFinished: false,
	}
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

func mapsEqualInContent(reference map[string]interface{}, candidate map[string]interface{}) (bool, string) {

	if len(reference) != len(candidate) {
		return false, "given maps do not have same length, hence cannot have equal content"
	}

	for k1, v1 := range reference {
		if v2, ok := candidate[k1]; !ok {
			return false, fmt.Sprintf("key wanted in candidate map, but not found: %s", k1)
		} else if v1 != v2 {
			return false, fmt.Sprintf("key '%s' associated with different values -- wanted: %v; got: %v", k1, v1, v2)
		}
	}

	return true, ""

}
