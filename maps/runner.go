package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runner interface {
		runMapTests(hzCluster string, hzMembers []string)
	}
	configPropertyAssigner interface {
		Assign(string, func(string, any) error) error
	}
	runnerConfig struct {
		enabled                   bool
		numMaps                   int
		numRuns                   uint32
		mapBaseName               string
		useMapPrefix              bool
		mapPrefix                 string
		appendMapIndexToMapName   bool
		appendClientIdToMapName   bool
		sleepBetweenActionBatches *sleepConfig
		sleepBetweenRuns          *sleepConfig
	}
	sleepConfig struct {
		enabled    bool
		durationMs int
	}
	runnerConfigBuilder struct {
		runnerKeyPath string
		mapBaseName   string
	}
	MapTester struct {
		HzCluster string
		HzMembers []string
	}
	state string
)

// TODO include state in status endpoint
const (
	start                  state = "start"
	populateConfigComplete state = "populateConfigComplete"
	checkEnabledComplete   state = "checkEnabledComplete"
	raiseReadyComplete     state = "raiseReadyComplete"
	testLoopStart          state = "testLoopStart"
	testLoopComplete       state = "testLoopComplete"
)

const (
	templateBoolParseError        = "%s: unable to parse %v to bool"
	templateIntParseError         = "%s: unable to parse %v to int"
	templateStringParseError      = "%s: unable to parse %v into string"
	templateNumberAtLeastOneError = "%s: expected number to be at least 1, got %d"
)

var (
	runners          []runner
	propertyAssigner configPropertyAssigner
)

func register(r runner) {
	runners = append(runners, r)
}

var lp *logging.LogProvider

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
	propertyAssigner = client.DefaultConfigPropertyAssigner{}
}

func (b runnerConfigBuilder) populateConfig() (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				enabled = b
				return nil
			}
		})
	})

	var numMaps int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numMaps", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				numMaps = i
				return nil
			}
		})
	})

	var appendMapIndexToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendMapIndexToMapName", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				appendMapIndexToMapName = b
				return nil
			}
		})
	})

	var appendClientIdToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendClientIdToMapName", func(path string, a any) error {
			// TODO Function "boolAssign" that handles assigning of all booleans once the config value has been extracted?
			// Would be located in config package... config package could provide different assignment functions -- AssignBool, AssignInt, ...
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				appendClientIdToMapName = b
				return nil
			}
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numRuns", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				numRuns = uint32(i)
				return nil
			}
		})
	})

	var useMapPrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".mapPrefix.enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				useMapPrefix = b
				return nil
			}
		})
	})

	var mapPrefix string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".mapPrefix.prefix", func(path string, a any) error {
			if s, ok := a.(string); !ok {
				return fmt.Errorf(templateStringParseError, path, a)
			} else if len(s) == 0 {
				return fmt.Errorf("%s: expected non-empty string", path)
			} else {
				mapPrefix = s
				return nil
			}
		})
	})

	var sleepBetweenActionBatchesEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				sleepBetweenActionBatchesEnabled = b
				return nil
			}
		})
	})

	var sleepBetweenActionBatchesDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.durationMs", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				sleepBetweenActionBatchesDurationMs = i
				return nil
			}
		})
	})

	var sleepBetweenRunsEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				sleepBetweenRunsEnabled = b
				return nil
			}
		})
	})

	var sleepBetweenRunsDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.durationMs", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				sleepBetweenRunsDurationMs = i
				return nil
			}
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &runnerConfig{
		enabled:                   enabled,
		numMaps:                   numMaps,
		numRuns:                   numRuns,
		mapBaseName:               b.mapBaseName,
		useMapPrefix:              useMapPrefix,
		mapPrefix:                 mapPrefix,
		appendMapIndexToMapName:   appendMapIndexToMapName,
		appendClientIdToMapName:   appendClientIdToMapName,
		sleepBetweenActionBatches: &sleepConfig{sleepBetweenActionBatchesEnabled, sleepBetweenActionBatchesDurationMs},
		sleepBetweenRuns:          &sleepConfig{sleepBetweenRunsEnabled, sleepBetweenRunsDurationMs},
	}, nil

}

func (t *MapTester) TestMaps() {

	clientID := client.ID()
	lp.LogInternalStateEvent(fmt.Sprintf("%s: maptester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := runners[i]
			runner.runMapTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}
