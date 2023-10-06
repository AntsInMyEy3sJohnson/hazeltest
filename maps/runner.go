package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runnerLoopType string
	runner         interface {
		runMapTests(hzCluster string, hzMembers []string)
	}
	runnerConfig struct {
		enabled                 bool
		numMaps                 uint16
		numRuns                 uint32
		mapBaseName             string
		useMapPrefix            bool
		mapPrefix               string
		appendMapIndexToMapName bool
		appendClientIdToMapName bool
		sleepBetweenRuns        *sleepConfig
		loopType                runnerLoopType
		boundary                *boundaryTestLoopConfig
		batch                   *batchTestLoopConfig
	}
	sleepConfig struct {
		enabled          bool
		durationMs       int
		enableRandomness bool
	}
	runnerConfigBuilder struct {
		assigner      client.ConfigPropertyAssigner
		runnerKeyPath string
		mapBaseName   string
	}
	MapTester struct {
		HzCluster string
		HzMembers []string
	}
	state string
)

type (
	batchTestLoopConfig struct {
		sleepBetweenActionBatches *sleepConfig
	}
)

type (
	boundaryTestLoopConfig struct {
		sleepBetweenOperationChains      *sleepConfig
		resetAfterChain                  bool
		upper                            *boundaryDefinition
		lower                            *boundaryDefinition
		actionTowardsBoundaryProbability float32
	}
	boundaryDefinition struct {
		mapFillPercentage float32
		enableRandomness  bool
	}
)

const (
	batch    runnerLoopType = "batch"
	boundary runnerLoopType = "boundary"
)

// TODO include state in status endpoint
const (
	start                  state = "start"
	populateConfigComplete state = "populateConfigComplete"
	checkEnabledComplete   state = "checkEnabledComplete"
	assignTestLoopComplete state = "assignTestLoopComplete"
	raiseReadyComplete     state = "raiseReadyComplete"
	testLoopStart          state = "testLoopStart"
	testLoopComplete       state = "testLoopComplete"
)

var (
	runners []runner
	lp      *logging.LogProvider
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func populateBatchTestLoopConfig(b runnerConfigBuilder) (*batchTestLoopConfig, error) {

	var assignmentOps []func() error

	var sleepBetweenActionBatchesEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.enabled", client.ValidateBool, func(a any) {
			sleepBetweenActionBatchesEnabled = a.(bool)
		})
	})

	var sleepDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.durationMs", client.ValidateInt, func(a any) {
			sleepDurationMs = a.(int)
		})
	})

	var enableSleepRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.enableRandomness", client.ValidateBool, func(a any) {
			enableSleepRandomness = a.(bool)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &batchTestLoopConfig{
		sleepBetweenActionBatches: &sleepConfig{
			enabled:          sleepBetweenActionBatchesEnabled,
			durationMs:       sleepDurationMs,
			enableRandomness: enableSleepRandomness,
		},
	}, nil

}

func populateBoundaryTestLoopConfig(b runnerConfigBuilder) (*boundaryTestLoopConfig, error) {

	var assignmentOps []func() error

	var sleepBetweenOperationChainsEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.betweenOperationChains.enabled", client.ValidateBool, func(a any) {
			sleepBetweenOperationChainsEnabled = a.(bool)
		})
	})

	var sleepBetweenOperationChainsDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.betweenOperationChains.durationMs", client.ValidateInt, func(a any) {
			sleepBetweenOperationChainsDurationMs = a.(int)
		})
	})

	var sleepBetweenOperationChainsEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.betweenOperationChains.enableRandomness", client.ValidateBool, func(a any) {
			sleepBetweenOperationChainsEnableRandomness = a.(bool)
		})
	})

	var resetAfterChain bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.resetAfterChain", client.ValidateBool, func(a any) {
			resetAfterChain = a.(bool)
		})
	})

	var upperBoundaryMapFillPercentage float32
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.boundaryDefinition.upper.mapFillPercentage", client.ValidatePercentage, func(a any) {
			upperBoundaryMapFillPercentage = float32(a.(float64))
		})
	})

	var upperBoundaryEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.boundaryDefinition.upper.enableRandomness", client.ValidateBool, func(a any) {
			upperBoundaryEnableRandomness = a.(bool)
		})
	})

	var lowerBoundaryMapFillPercentage float32
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.boundaryDefinition.lower.mapFillPercentage", client.ValidatePercentage, func(a any) {
			lowerBoundaryMapFillPercentage = float32(a.(float64))
		})
	})

	var lowerBoundaryEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.boundaryDefinition.lower.enableRandomness", client.ValidateBool, func(a any) {
			lowerBoundaryEnableRandomness = a.(bool)
		})
	})

	var actionTowardsBoundaryProbability float32
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.boundaryDefinition.actionTowardsBoundaryProbability", client.ValidatePercentage, func(a any) {
			actionTowardsBoundaryProbability = float32(a.(float64))
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &boundaryTestLoopConfig{
		sleepBetweenOperationChains: &sleepConfig{
			enabled:          sleepBetweenOperationChainsEnabled,
			durationMs:       sleepBetweenOperationChainsDurationMs,
			enableRandomness: sleepBetweenOperationChainsEnableRandomness,
		},
		resetAfterChain: resetAfterChain,
		upper: &boundaryDefinition{
			mapFillPercentage: upperBoundaryMapFillPercentage,
			enableRandomness:  upperBoundaryEnableRandomness,
		},
		lower: &boundaryDefinition{
			mapFillPercentage: lowerBoundaryMapFillPercentage,
			enableRandomness:  lowerBoundaryEnableRandomness,
		},
		actionTowardsBoundaryProbability: actionTowardsBoundaryProbability,
	}, nil

}

func validateTestLoopType(keyPath string, a any) error {
	if err := client.ValidateString(keyPath, a); err != nil {
		return err
	}
	switch a {
	case string(batch), string(boundary):
		return nil
	default:
		return fmt.Errorf("test loop type expected to be one of '%s' or '%s', got '%v'", batch, boundary, a)
	}
}

func (b runnerConfigBuilder) populateConfig() (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numMaps uint16
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".numMaps", client.ValidateInt, func(a any) {
			numMaps = uint16(a.(int))
		})
	})

	var appendMapIndexToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".appendMapIndexToMapName", client.ValidateBool, func(a any) {
			appendMapIndexToMapName = a.(bool)
		})
	})

	var appendClientIdToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".appendClientIdToMapName", client.ValidateBool, func(a any) {
			appendClientIdToMapName = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".numRuns", client.ValidateInt, func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var useMapPrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".mapPrefix.enabled", client.ValidateBool, func(a any) {
			useMapPrefix = a.(bool)
		})
	})

	var mapPrefix string
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".mapPrefix.prefix", client.ValidateString, func(a any) {
			mapPrefix = a.(string)
		})
	})

	var sleepBetweenRunsEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enabled", client.ValidateBool, func(a any) {
			sleepBetweenRunsEnabled = a.(bool)
		})
	})

	var sleepBetweenRunsDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.durationMs", client.ValidateInt, func(a any) {
			sleepBetweenRunsDurationMs = a.(int)
		})
	})

	var sleepBetweenRunsEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enableRandomness", client.ValidateBool, func(a any) {
			sleepBetweenRunsEnableRandomness = a.(bool)
		})
	})

	var loopType runnerLoopType
	keyPath := b.runnerKeyPath + ".testLoop.type"
	// TODO Refactor into own method and add test
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(keyPath, validateTestLoopType, func(a any) {
			loopType = runnerLoopType(a.(string))
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	var batchConfig *batchTestLoopConfig
	if loopType == batch {
		if bc, err := populateBatchTestLoopConfig(b); err != nil {
			return nil, err
		} else {
			batchConfig = bc
		}
	} else {
		batchConfig = nil
	}

	var boundaryConfig *boundaryTestLoopConfig
	if loopType == boundary {
		if bc, err := populateBoundaryTestLoopConfig(b); err != nil {
			return nil, err
		} else {
			boundaryConfig = bc
		}
	} else {
		boundaryConfig = nil
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
		sleepBetweenRuns: &sleepConfig{
			sleepBetweenRunsEnabled,
			sleepBetweenRunsDurationMs,
			sleepBetweenRunsEnableRandomness,
		},
		loopType: loopType,
		boundary: boundaryConfig,
		batch:    batchConfig,
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
