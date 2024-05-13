package api

import "fmt"

const (
	sourceMapPokedexRunner  = "pokedexRunner"
	sourceMapLoadRunner     = "loadRunner"
	statusKeyNumMaps        = "numMaps"
	statusKeyNumRuns        = "numRuns"
	statusKeyTotalRuns      = "totalRuns"
	statusKeyRunnerFinished = "runnerFinished"
)

const (
	sourceChaosMonkeyMemberKiller = "memberKiller"
	statusKeyNumMembersKilled     = "numMembersKilled"
	statusKeyMonkeyFinished       = "monkeyFinished"
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

var (
	dummyStatusMapPokedexTestLoop = map[string]any{
		statusKeyNumMaps:        10,
		statusKeyNumRuns:        1000,
		statusKeyTotalRuns:      10 * 1000,
		statusKeyRunnerFinished: false,
	}
	dummyStatusMapLoadTestLoop = map[string]any{
		statusKeyNumMaps:        5,
		statusKeyNumRuns:        100,
		statusKeyTotalRuns:      5 * 100,
		statusKeyRunnerFinished: false,
	}
	dummyStatusMemberKillerMonkey = map[string]any{
		statusKeyNumRuns:          100,
		statusKeyNumMembersKilled: 15,
		statusKeyMonkeyFinished:   false,
	}
)

func mapsEqualInContent(reference map[string]any, candidate map[string]any) (bool, string) {

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
