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
		enabled          bool
		durationMs       int
		enableRandomness bool
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

var (
	runners          []runner
	propertyAssigner client.ConfigPropertyAssigner
	lp               *logging.LogProvider
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
	propertyAssigner = client.DefaultConfigPropertyAssigner{}
}

func (b runnerConfigBuilder) populateConfig() (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numMaps int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numMaps", client.ValidateInt, func(a any) {
			numMaps = a.(int)
		})
	})

	var appendMapIndexToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendMapIndexToMapName", client.ValidateBool, func(a any) {
			appendMapIndexToMapName = a.(bool)
		})
	})

	var appendClientIdToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendClientIdToMapName", client.ValidateBool, func(a any) {
			appendClientIdToMapName = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numRuns", client.ValidateInt, func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var useMapPrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".mapPrefix.enabled", client.ValidateBool, func(a any) {
			useMapPrefix = a.(bool)
		})
	})

	var mapPrefix string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".mapPrefix.prefix", client.ValidateString, func(a any) {
			mapPrefix = a.(string)
		})
	})

	var sleepBetweenActionBatchesEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.enabled", client.ValidateBool, func(a any) {
			sleepBetweenActionBatchesEnabled = a.(bool)
		})
	})

	var sleepBetweenActionBatchesDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.durationMs", client.ValidateInt, func(a any) {
			sleepBetweenActionBatchesDurationMs = a.(int)
		})
	})

	var sleepBetweenActionBatchesEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.enableRandomness", client.ValidateBool, func(a any) {
			sleepBetweenActionBatchesEnableRandomness = a.(bool)
		})
	})

	var sleepBetweenRunsEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enabled", client.ValidateBool, func(a any) {
			sleepBetweenRunsEnabled = a.(bool)
		})
	})

	var sleepBetweenRunsDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.durationMs", client.ValidateInt, func(a any) {
			sleepBetweenRunsDurationMs = a.(int)
		})
	})

	var sleepBetweenRunsEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enableRandomness", client.ValidateBool, func(a any) {
			sleepBetweenRunsEnableRandomness = a.(bool)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &runnerConfig{
		enabled:                 enabled,
		numMaps:                 numMaps,
		numRuns:                 numRuns,
		mapBaseName:             b.mapBaseName,
		useMapPrefix:            useMapPrefix,
		mapPrefix:               mapPrefix,
		appendMapIndexToMapName: appendMapIndexToMapName,
		appendClientIdToMapName: appendClientIdToMapName,
		sleepBetweenActionBatches: &sleepConfig{
			sleepBetweenActionBatchesEnabled,
			sleepBetweenActionBatchesDurationMs,
			sleepBetweenActionBatchesEnableRandomness,
		},
		sleepBetweenRuns: &sleepConfig{
			sleepBetweenRunsEnabled,
			sleepBetweenRunsDurationMs,
			sleepBetweenRunsEnableRandomness,
		},
	}, nil

}

func (t *MapTester) TestMaps() {

	clientID := client.ID()
	lp.LogRunnerEvent(fmt.Sprintf("%s: map tester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

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
