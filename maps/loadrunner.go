package maps

import (
	"context"
	"encoding/gob"
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
	"strconv"
)

type (
	loadRunner struct {
		assigner        client.ConfigPropertyAssigner
		stateList       []runnerState
		name            string
		source          string
		hzClientHandler hazelcastwrapper.HzClientHandler
		hzMapStore      hazelcastwrapper.MapStore
		l               looper[loadElement]
		gatherer        *status.Gatherer
		providerFuncs   struct {
			mapStore            newMapStoreFunc
			loadElementTestLoop newLoadElementTestLoopFunc
		}
	}
	loadElement struct {
		Key     string
		Payload string
	}
	newLoadElementTestLoopFunc func(rc *runnerConfig) (looper[loadElement], error)
)

const (
	mapLoadRunnerKeyPath     = "mapTests.load"
	mapLoadRunnerMapBaseName = "load"
	mapLoadRunnerName        = "mapsLoadRunner"
)

var (
	numEntriesPerMap                                   int
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
		providerFuncs: struct {
			mapStore            newMapStoreFunc
			loadElementTestLoop newLoadElementTestLoopFunc
		}{mapStore: newDefaultMapStore, loadElementTestLoop: newLoadElementTestLoop},
	})
	gob.Register(loadElement{})
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

func (r *loadRunner) getSourceName() string {
	return "loadRunner"
}

func (r *loadRunner) runMapTests(ctx context.Context, hzCluster string, hzMembers []string, gatherer *status.Gatherer) {

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

	l, err := r.providerFuncs.loadElementTestLoop(config)
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
	r.hzMapStore = r.providerFuncs.mapStore(r.hzClientHandler)
	lp.LogMapRunnerEvent("initialized hazelcast client", r.name, log.InfoLevel)

	var loadElements []loadElement
	if useFixedPayload {
		lp.LogMapRunnerEvent("usage of fixed-size payloads enabled", r.name, log.TraceLevel)
		loadElements = populateLoadElements(numEntriesPerMap, fixedPayloadSizeBytes)
	} else if useVariablePayload {
		lp.LogMapRunnerEvent("usage of variable-size payloads enabled", r.name, log.TraceLevel)
		// If the user wants variable-sized payloads to be generated, we only generate they keys here, and
		// let the payload be generated on demand by downstream functionality
		loadElements = populateLoadElementKeys(numEntriesPerMap)
		loadsupport.RegisterPayloadGenerationRequirement(mapLoadRunnerName, loadsupport.PayloadGenerationRequirement{
			LowerBoundaryBytes: variablePayloadSizeLowerBoundaryBytes,
			UpperBoundaryBytes: variablePayloadSizeUpperBoundaryBytes,
			SameSizeStepsLimit: variablePayloadEvaluateNewSizeAfterNumWriteActions,
		})
	} else {
		lp.LogMapRunnerEvent("neither fixed-size nor variable-size load elements have been enabled -- cannot populate load elements", r.name, log.ErrorLevel)
		return
	}

	lp.LogMapRunnerEvent("initialized load elements", r.name, log.InfoLevel)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogMapRunnerEvent("starting load test loop for maps", r.name, log.InfoLevel)

	tle := &testLoopExecution[loadElement]{
		id:                   uuid.New(),
		runnerName:           r.name,
		source:               r.source,
		hzClientHandler:      r.hzClientHandler,
		hzMapStore:           r.hzMapStore,
		stateCleanerBuilder:  &state.DefaultSingleMapCleanerBuilder{},
		runnerConfig:         config,
		elements:             loadElements,
		ctx:                  ctx,
		getElementID:         getLoadElementID,
		getOrAssemblePayload: getOrAssemblePayload,
	}

	r.l.init(tle, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogMapRunnerEvent("finished map load test loop", r.name, log.InfoLevel)

}

func (r *loadRunner) appendState(s runnerState) {

	r.stateList = append(r.stateList, s)
	r.gatherer.Updates <- status.Update{Key: string(statusKeyCurrentState), Value: string(s)}

}

func populateLoadElementKeys(numKeysToPopulate int) []loadElement {

	elements := make([]loadElement, numKeysToPopulate)

	for i := 0; i < numKeysToPopulate; i++ {
		elements[i] = loadElement{Key: strconv.Itoa(i)}
	}

	return elements

}

func populateLoadElements(numElementsToPopulate int, payloadSizeBytes int) []loadElement {

	elements := make([]loadElement, numElementsToPopulate)
	// Depending on the value of 'payloadSizeBytes', this string can get very large, and to generate one
	// unique string for each map entry will result in high memory consumption of this Hazeltest client.
	// Thus, we use one random string for each map and reference that string in each load element
	randomPayload := loadsupport.GenerateRandomStringPayload(payloadSizeBytes)

	for i := 0; i < numElementsToPopulate; i++ {
		elements[i] = loadElement{
			Key:     strconv.Itoa(i),
			Payload: randomPayload,
		}
	}

	return elements

}

func getOrAssemblePayload(mapName string, mapNumber uint16, element any) (any, error) {

	l := element.(loadElement)

	if useFixedPayload {
		if len(l.Payload) == 0 {
			return "", errors.New("fixed-size payloads have been enabled, but no payload of fixed size was provided in load element")
		}
		return l.Payload, nil
	}

	if useVariablePayload {
		return loadsupport.GenerateTrackedRandomStringPayloadWithinBoundary(
			fmt.Sprintf("%s-%s-%d", mapLoadRunnerName, mapName, mapNumber),
		)
	}

	return "", errors.New("instructions unclear: neither fixed-size nor variable-size payloads enabled")

}

func getLoadElementID(element any) string {

	l := element.(loadElement)
	return l.Key

}

func populateLoadConfig(runnerKeyPath string, mapBaseName string, a client.ConfigPropertyAssigner) (*runnerConfig, error) {

	var assignmentOps []func() error

	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(runnerKeyPath+".numEntriesPerMap", client.ValidateInt, func(a any) {
			numEntriesPerMap = a.(int)
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
	return configBuilder.populateConfig()

}
