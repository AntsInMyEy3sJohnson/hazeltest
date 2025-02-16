package maps

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/loadsupport"
	"hazeltest/state"
	"hazeltest/status"
)

type (
	providerFuncs struct {
		mapStore            newMapStoreFunc
		loadElementTestLoop newLoadElementTestLoopFunc
		payloads            newPayloadProviderFunc
	}
	loadRunner struct {
		assigner          client.ConfigPropertyAssigner
		stateList         []runnerState
		name              string
		source            string
		hzClientHandler   hazelcastwrapper.HzClientHandler
		hzMapStore        hazelcastwrapper.MapStore
		gatherer          status.Gatherer
		payloadProvider   loadsupport.PayloadProvider
		l                 looper[loadElement]
		providerFunctions providerFuncs
	}
	loadElement struct {
		Key     string
		Payload *string
	}
	newLoadElementTestLoopFunc func(rc *runnerConfig) (looper[loadElement], error)
	newPayloadProviderFunc     func() loadsupport.PayloadProvider
)

const (
	mapLoadRunnerKeyPath     = "mapTests.load"
	mapLoadRunnerMapBaseName = "load"
	mapLoadRunnerName        = "mapsLoadRunner"
)

var (
	useFixedPayload                                    bool
	fixedPayloadSizeBytes                              int
	useVariablePayload                                 bool
	variablePayloadSizeLowerBoundaryBytes              int
	variablePayloadSizeUpperBoundaryBytes              int
	variablePayloadEvaluateNewSizeAfterNumWriteActions int
)

func init() {
	register(&loadRunner{
		assigner:        &client.DefaultConfigPropertyAssigner{},
		stateList:       []runnerState{},
		name:            mapLoadRunnerName,
		source:          "loadRunner",
		hzClientHandler: &hazelcastwrapper.DefaultHzClientHandler{},
		providerFunctions: providerFuncs{
			mapStore:            newDefaultMapStore,
			loadElementTestLoop: newLoadElementTestLoop,
			payloads:            newDefaultPayloadProvider,
		},
	})
}

func newLoadElementTestLoop(rc *runnerConfig) (looper[loadElement], error) {

	switch rc.loopType {
	case batch:
		return &batchTestLoop[loadElement]{}, nil
	case boundary:
		return &boundaryTestLoop[loadElement]{}, nil
	default:
		return nil, fmt.Errorf("no such runner runnerLoopType: %s", rc.loopType)
	}

}

func newDefaultPayloadProvider() loadsupport.PayloadProvider {

	return &loadsupport.DefaultPayloadProvider{}

}

func (r *loadRunner) getSourceName() string {
	return "loadRunner"
}

func (r *loadRunner) runMapTests(ctx context.Context, hzCluster string, hzMembers []string, gatherer *status.DefaultGatherer) {

	r.gatherer = gatherer
	r.appendState(start)

	config, err := populateLoadConfig(mapLoadRunnerKeyPath, mapLoadRunnerMapBaseName, r.assigner)
	if err != nil {
		lp.LogMapRunnerEvent(fmt.Sprintf("aborting launch of map load runner: unable to populate config: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadRunner from maps/loadRunner
		lp.LogMapRunnerEvent("load runner not enabled -- won't run", r.name, log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	l, err := r.providerFunctions.loadElementTestLoop(config)
	if err != nil {
		lp.LogMapRunnerEvent(fmt.Sprintf("aborting launch of map load runner: unable to initialize test loop: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.l = l

	r.appendState(assignTestLoopComplete)

	r.hzClientHandler.InitHazelcastClient(ctx, r.name, hzCluster, hzMembers)
	defer func() {
		_ = r.hzClientHandler.Shutdown(ctx)
	}()
	r.hzMapStore = r.providerFunctions.mapStore(r.hzClientHandler)
	lp.LogMapRunnerEvent("initialized hazelcast client", r.name, log.InfoLevel)

	r.payloadProvider = r.providerFunctions.payloads()
	if useFixedPayload {
		lp.LogMapRunnerEvent("usage of fixed-size payloads enabled", r.name, log.TraceLevel)
		r.payloadProvider.RegisterPayloadGenerationRequirement(mapLoadRunnerName, loadsupport.PayloadGenerationRequirement{
			UseFixedSize: useFixedPayload,
			FixedSize:    loadsupport.FixedSizePayloadDefinition{SizeBytes: fixedPayloadSizeBytes},
		})
	} else if useVariablePayload {
		lp.LogMapRunnerEvent("usage of variable-size payloads enabled", r.name, log.TraceLevel)
		r.payloadProvider.RegisterPayloadGenerationRequirement(mapLoadRunnerName, loadsupport.PayloadGenerationRequirement{
			UseVariableSize: useVariablePayload,
			VariableSize: loadsupport.VariableSizePayloadDefinition{
				LowerBoundaryBytes: variablePayloadSizeLowerBoundaryBytes,
				UpperBoundaryBytes: variablePayloadSizeUpperBoundaryBytes,
				SameSizeStepsLimit: variablePayloadEvaluateNewSizeAfterNumWriteActions,
			},
		})
	} else {
		lp.LogMapRunnerEvent("neither fixed-size nor variable-size load elements have been enabled -- cannot populate load elements", r.name, log.ErrorLevel)
		return
	}

	lp.LogMapRunnerEvent("registered payload generation requirement", r.name, log.InfoLevel)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogMapRunnerEvent("starting load test loop for maps", r.name, log.InfoLevel)

	tle := &testLoopExecution[loadElement]{
		id:                        uuid.New(),
		runnerName:                r.name,
		source:                    r.source,
		hzClientHandler:           r.hzClientHandler,
		hzMapStore:                r.hzMapStore,
		stateCleanerBuilder:       &state.DefaultSingleMapCleanerBuilder{},
		runnerConfig:              config,
		elements:                  make([]loadElement, 0),
		usePreInitializedElements: false,
		ctx:                       ctx,
		getElementID:              getLoadElementID,
		getOrAssemblePayload:      r.getOrAssemblePayload,
	}

	r.l.init(tle, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogMapRunnerEvent("finished map load test loop", r.name, log.InfoLevel)

}

func (r *loadRunner) appendState(s runnerState) {

	r.stateList = append(r.stateList, s)
	r.gatherer.Gather(status.Update{Key: string(statusKeyCurrentState), Value: string(s)})

}

func (r *loadRunner) getOrAssemblePayload(mapName string, mapNumber uint16, _ string) (*loadsupport.PayloadWrapper, error) {

	actorName := fmt.Sprintf("%s-%s-%d", mapLoadRunnerName, mapName, mapNumber)
	return r.payloadProvider.RetrievePayload(actorName)

}

func getLoadElementID(element any) string {

	l := element.(loadElement)
	return l.Key

}

func assertExactlyOnePayloadModeEnabled(fixedEnabled, variableEnabled bool) error {

	if fixedEnabled == variableEnabled {
		if fixedEnabled {
			return errors.New("instructions unclear: both fixed-size and variable-size payloads have been enabled")

		}
		return errors.New("instructions unclear: neither fixed-size nor variable-size payloads have been enabled")
	}

	return nil

}

func validateVariablePayloadSizeBoundaries(lower, upper int) error {

	if lower >= upper {
		return fmt.Errorf("expected upper boundary to be greater than lower boundary, got %d (upper) and %d (lower)", upper, lower)
	}

	return nil

}

func populateLoadConfig(runnerKeyPath string, mapBaseName string, a client.ConfigPropertyAssigner) (*runnerConfig, error) {

	var assignmentOps []func() error

	var numEntriesPerMap uint32
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".numEntriesPerMap", client.ValidateInt, func(a any) {
			numEntriesPerMap = uint32(a.(int))
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.fixedSize.enabled", client.ValidateBool, func(a any) {
			useFixedPayload = a.(bool)
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.fixedSize.sizeBytes", client.ValidateInt, func(a any) {
			fixedPayloadSizeBytes = a.(int)
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.variableSize.enabled", client.ValidateBool, func(a any) {
			useVariablePayload = a.(bool)
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.variableSize.lowerBoundaryBytes", client.ValidateInt, func(a any) {
			variablePayloadSizeLowerBoundaryBytes = a.(int)
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.variableSize.upperBoundaryBytes", client.ValidateInt, func(a any) {
			variablePayloadSizeUpperBoundaryBytes = a.(int)
		})
	})

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".payload.variableSize.evaluateNewSizeAfterNumWriteActions", client.ValidateInt, func(a any) {
			variablePayloadEvaluateNewSizeAfterNumWriteActions = a.(int)
		})
	})

	for _, fn := range assignmentOps {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	configBuilder := runnerConfigBuilder{
		assigner:      a,
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   mapBaseName,
	}

	cfg, err := configBuilder.populateConfig()

	if err != nil {
		return nil, err
	}

	if cfg.enabled {
		// Perform additional checks only if runner has been enabled
		if err = assertExactlyOnePayloadModeEnabled(useFixedPayload, useVariablePayload); err != nil {
			return nil, err
		}

		if useVariablePayload {
			err = validateVariablePayloadSizeBoundaries(variablePayloadSizeLowerBoundaryBytes, variablePayloadSizeUpperBoundaryBytes)
			if err != nil {
				return nil, err
			}
		}
	}

	// TODO Update test
	cfg.numEntriesPerMap = numEntriesPerMap

	return cfg, nil

}
