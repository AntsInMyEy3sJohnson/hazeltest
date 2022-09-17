package maps

import (
	"fmt"
	"testing"
)

func TestRunMapTests(t *testing.T) {

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

			r.runMapTests("awesome-hz-cluster", []string{"awesome-hz-cluster-svc.cluster.local"})

			msg := fmt.Sprintf("\t\tlast state must be '%s'", start)
			if r.ls == start {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}
