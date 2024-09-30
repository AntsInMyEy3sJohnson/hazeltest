package maps

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/logging"
	"hazeltest/state"
	"hazeltest/status"
	"sync"
)

type (
	runnerLoopType string
	runner         interface {
		getSourceName() string
		runMapTests(ctx context.Context, hzCluster string, hzMembers []string, gatherer *status.Gatherer)
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
		loopType                runnerLoopType
		preRunClean             *preRunCleanConfig
		sleepBetweenRuns        *sleepConfig
		boundary                *boundaryTestLoopConfig
		batch                   *batchTestLoopConfig
	}
	preRunCleanConfig struct {
		enabled                  bool
		errorBehavior            state.ErrorDuringCleanBehavior
		applyCleanAgainThreshold bool
		cleanAgainThresholdMs    uint64
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
	runnerState     string
	statusKey       string
	newMapStoreFunc func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.MapStore
)

type (
	batchTestLoopConfig struct {
		sleepAfterBatchAction     *sleepConfig
		sleepBetweenActionBatches *sleepConfig
	}
)

type (
	boundaryTestLoopConfig struct {
		sleepBetweenOperationChains      *sleepConfig
		sleepAfterChainAction            *sleepConfig
		sleepUponModeChange              *sleepConfig
		chainLength                      int
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

const (
	start                  runnerState = "start"
	populateConfigComplete runnerState = "populateConfigComplete"
	checkEnabledComplete   runnerState = "checkEnabledComplete"
	assignTestLoopComplete runnerState = "assignTestLoopComplete"
	raiseReadyComplete     runnerState = "raiseReadyComplete"
	testLoopStart          runnerState = "testLoopStart"
	testLoopComplete       runnerState = "testLoopComplete"
)

const (
	statusKeyCurrentState statusKey = "currentState"
)

var (
	runners            []runner
	lp                 *logging.LogProvider
	newDefaultMapStore newMapStoreFunc = func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.MapStore {
		return &hazelcastwrapper.DefaultMapStore{Client: ch.GetClient()}
	}
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = logging.GetLogProviderInstance(client.ID())
}

func populateBatchTestLoopConfig(b runnerConfigBuilder) (*batchTestLoopConfig, error) {

	var assignmentOps []func() error

	var sleepAfterBatchActionEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.afterBatchAction.enabled", client.ValidateBool, func(a any) {
			sleepAfterBatchActionEnabled = a.(bool)
		})
	})

	var sleepAfterBatchActionDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.afterBatchAction.durationMs", client.ValidateInt, func(a any) {
			sleepAfterBatchActionDurationMs = a.(int)
		})
	})

	var sleepAfterBatchActionEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.afterBatchAction.enableRandomness", client.ValidateBool, func(a any) {
			sleepAfterBatchActionEnableRandomness = a.(bool)
		})
	})

	var sleepBetweenActionBatchesEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.enabled", client.ValidateBool, func(a any) {
			sleepBetweenActionBatchesEnabled = a.(bool)
		})
	})

	var sleepBetweenActionBatchesDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.durationMs", client.ValidateInt, func(a any) {
			sleepBetweenActionBatchesDurationMs = a.(int)
		})
	})

	var sleepBetweenActionBatchesEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.batch.sleeps.betweenActionBatches.enableRandomness", client.ValidateBool, func(a any) {
			sleepBetweenActionBatchesEnableRandomness = a.(bool)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &batchTestLoopConfig{
		sleepAfterBatchAction: &sleepConfig{
			enabled:          sleepAfterBatchActionEnabled,
			durationMs:       sleepAfterBatchActionDurationMs,
			enableRandomness: sleepAfterBatchActionEnableRandomness,
		},
		sleepBetweenActionBatches: &sleepConfig{
			enabled:          sleepBetweenActionBatchesEnabled,
			durationMs:       sleepBetweenActionBatchesDurationMs,
			enableRandomness: sleepBetweenActionBatchesEnableRandomness,
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

	var sleepAfterChainActionEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.afterChainAction.enabled", client.ValidateBool, func(a any) {
			sleepAfterChainActionEnabled = a.(bool)
		})
	})

	var sleepAfterChainActionDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.afterChainAction.durationMs", client.ValidateInt, func(a any) {
			sleepAfterChainActionDurationMs = a.(int)
		})
	})

	var sleepAfterChainActionEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.afterChainAction.enableRandomness", client.ValidateBool, func(a any) {
			sleepAfterChainActionEnableRandomness = a.(bool)
		})
	})

	var sleepUponModeChangeEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.uponModeChange.enabled", client.ValidateBool, func(a any) {
			sleepUponModeChangeEnabled = a.(bool)
		})
	})

	var sleepUponModeChangeDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.uponModeChange.durationMs", client.ValidateInt, func(a any) {
			sleepUponModeChangeDurationMs = a.(int)
		})
	})

	var sleepUponModeChangeEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.sleeps.uponModeChange.enableRandomness", client.ValidateBool, func(a any) {
			sleepUponModeChangeEnableRandomness = a.(bool)
		})
	})

	var operationChainLength int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".testLoop.boundary.operationChain.length", client.ValidateInt, func(a any) {
			operationChainLength = a.(int)
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
			if v, ok := a.(float64); ok {
				upperBoundaryMapFillPercentage = float32(v)
			} else if v, ok := a.(float32); ok {
				upperBoundaryMapFillPercentage = v
			} else {
				upperBoundaryMapFillPercentage = float32(a.(int))
			}
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
			if v, ok := a.(float64); ok {
				lowerBoundaryMapFillPercentage = float32(v)
			} else if v, ok := a.(float32); ok {
				lowerBoundaryMapFillPercentage = v
			} else {
				lowerBoundaryMapFillPercentage = float32(a.(int))
			}
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
			if v, ok := a.(float64); ok {
				actionTowardsBoundaryProbability = float32(v)
			} else {
				actionTowardsBoundaryProbability = float32(a.(int))
			}
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	if upperBoundaryMapFillPercentage <= lowerBoundaryMapFillPercentage {
		return nil, fmt.Errorf("upper map fill percentage must be greater than lower map fill percentage, got %f (upper) and %f (lower)", upperBoundaryMapFillPercentage, lowerBoundaryMapFillPercentage)
	}

	return &boundaryTestLoopConfig{
		sleepBetweenOperationChains: &sleepConfig{
			enabled:          sleepBetweenOperationChainsEnabled,
			durationMs:       sleepBetweenOperationChainsDurationMs,
			enableRandomness: sleepBetweenOperationChainsEnableRandomness,
		},
		sleepAfterChainAction: &sleepConfig{
			enabled:          sleepAfterChainActionEnabled,
			durationMs:       sleepAfterChainActionDurationMs,
			enableRandomness: sleepAfterChainActionEnableRandomness,
		},
		sleepUponModeChange: &sleepConfig{
			enabled:          sleepUponModeChangeEnabled,
			durationMs:       sleepUponModeChangeDurationMs,
			enableRandomness: sleepUponModeChangeEnableRandomness,
		},
		chainLength:     operationChainLength,
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

	var performPreRunClean bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".performPreRunClean.enabled", client.ValidateBool, func(a any) {
			performPreRunClean = a.(bool)
		})
	})

	var errorDuringPreRunCleanBehavior state.ErrorDuringCleanBehavior
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".performPreRunClean.errorBehavior", state.ValidateErrorDuringCleanBehavior, func(a any) {
			errorDuringPreRunCleanBehavior = state.ErrorDuringCleanBehavior(a.(string))
		})
	})

	var applyCleanAgainThreshold bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".performPreRunClean.cleanAgainThreshold.enabled", client.ValidateBool, func(a any) {
			applyCleanAgainThreshold = a.(bool)
		})
	})

	var cleanAgainThresholdMs uint64
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".performPreRunClean.cleanAgainThreshold.thresholdMs", client.ValidateInt, func(a any) {
			cleanAgainThresholdMs = uint64(a.(int))
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
		preRunClean: &preRunCleanConfig{
			enabled:                  performPreRunClean,
			errorBehavior:            errorDuringPreRunCleanBehavior,
			applyCleanAgainThreshold: applyCleanAgainThreshold,
			cleanAgainThresholdMs:    cleanAgainThresholdMs,
		},
		loopType: loopType,
		boundary: boundaryConfig,
		batch:    batchConfig,
	}, nil

}

func (t *MapTester) TestMaps() {

	clientID := client.ID()
	lp.LogMapRunnerEvent(fmt.Sprintf("%s: map tester starting %d runner/-s", clientID, len(runners)), "mapTester", log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			gatherer := status.NewGatherer()
			listenReady := make(chan struct{})
			go gatherer.Listen(listenReady)
			<-listenReady

			defer gatherer.StopListen()

			rn := runners[i]

			api.RegisterStatefulActor(api.MapRunners, rn.getSourceName(), gatherer.AssembleStatusCopy)

			rn.runMapTests(context.TODO(), t.HzCluster, t.HzMembers, gatherer)
		}(i)
	}

	wg.Wait()

}
