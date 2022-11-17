package api

import "fmt"

const (
	sourcePokedexRunner = "pokedexrunner"
	sourceLoadRunner    = "loadrunner"
)
const (
	statusKeyNumMaps        = "numMaps"
	statusKeyNumRuns        = "numRuns"
	statusKeyTotalRuns      = "totalRuns"
	statusKeyRunnerFinished = "runnerFinished"
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
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
