package maps

import (
	"fmt"
	"testing"
)

func TestRunMapTests(t *testing.T) {

	hzCluster := "awesome-hz-cluster"
	hzMembers := []string{"awesome-hz-cluster-svc.cluster.local"}

	t.Log("given the need to test running map tests")
	{
		t.Log("\twhen the runner configuration cannot be populated")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError:   true,
				runnerKeyPath: "",
				dummyConfig:   nil,
			}
			r := pokedexRunner{ls: start}

			r.runMapTests(hzCluster, hzMembers)

			msg := fmt.Sprintf("\t\tlast state must be '%s'", start)
			if r.ls == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen runner has been disabled")
		{
			propertyAssigner = testConfigPropertyAssigner{
				returnError:   false,
				runnerKeyPath: runnerKeyPath,
				dummyConfig: map[string]interface{}{
					"maptests.pokedex.enabled": false,
				},
			}
			r := pokedexRunner{ls: start}

			r.runMapTests(hzCluster, hzMembers)

			msg := fmt.Sprintf("\t\tlast state must be '%s'", populateConfigComplete)

			if r.ls == populateConfigComplete {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}
