package maps

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/loadsupport"
	"hazeltest/status"
	"strconv"
)

type (
	loadRunner struct {
		assigner  client.ConfigPropertyAssigner
		stateList []runnerState
		name      string
		source    string
		mapStore  hazelcastwrapper.MapStore
		l         looper[loadElement]
		gatherer  *status.Gatherer
	}
	loadElement struct {
		Key     string
		Payload string
	}
)

var (
	numEntriesPerMap int
	payloadSizeBytes int
)

func init() {
	register(&loadRunner{
		assigner:  &client.DefaultConfigPropertyAssigner{},
		stateList: []runnerState{},
		name:      "mapsLoadRunner",
		source:    "loadRunner",
		mapStore:  &hazelcastwrapper.DefaultMapStore{},
		l:         &batchTestLoop[loadElement]{},
	})
	gob.Register(loadElement{})
}

func initializeLoadElementTestLoop(rc *runnerConfig) (looper[loadElement], error) {

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

func (r *loadRunner) runMapTests(hzCluster string, hzMembers []string, gatherer *status.Gatherer) {

	r.gatherer = gatherer
	r.appendState(start)

	config, err := populateLoadConfig(r.assigner)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of map load runner: unable to populate config: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadRunner from maps/loadRunner
		lp.LogRunnerEvent("load runner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	l, err := initializeLoadElementTestLoop(config)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of map load runner: unable to initialize test loop: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.l = l

	r.appendState(assignTestLoopComplete)

	ctx := context.TODO()

	r.mapStore.InitHazelcastClient(ctx, "mapsLoadRunner", hzCluster, hzMembers)
	defer func() {
		_ = r.mapStore.Shutdown(ctx)
	}()

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogRunnerEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogRunnerEvent("starting load test loop for maps", log.InfoLevel)

	lc := &testLoopExecution[loadElement]{uuid.New(), r.source, r.mapStore, config, populateLoadElements(), ctx, getLoadElementID}

	r.l.init(lc, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogRunnerEvent("finished map load test loop", log.InfoLevel)

}

func (r *loadRunner) appendState(s runnerState) {

	r.stateList = append(r.stateList, s)
	r.gatherer.Updates <- status.Update{Key: string(statusKeyCurrentState), Value: string(s)}

}

func populateLoadElements() []loadElement {

	elements := make([]loadElement, numEntriesPerMap)
	// Depending on the value of 'payloadSizeBytes', this string can get very large, and to generate one
	// unique string for each map entry will result in high memory consumption of this Hazeltest client.
	// Thus, we use one random string for each map and reference that string in each load element
	randomPayload := loadsupport.GenerateRandomStringPayload(payloadSizeBytes)

	for i := 0; i < numEntriesPerMap; i++ {
		elements[i] = loadElement{
			Key:     strconv.Itoa(i),
			Payload: randomPayload,
		}
	}

	return elements

}

func getLoadElementID(element any) string {

	loadElement := element.(loadElement)
	return loadElement.Key

}

func populateLoadConfig(a client.ConfigPropertyAssigner) (*runnerConfig, error) {

	runnerKeyPath := "mapTests.load"

	if err := a.Assign(runnerKeyPath+".numEntriesPerMap", client.ValidateInt, func(a any) {
		numEntriesPerMap = a.(int)
	}); err != nil {
		return nil, err
	}

	if err := a.Assign(runnerKeyPath+".payloadSizeBytes", client.ValidateInt, func(a any) {
		payloadSizeBytes = a.(int)
	}); err != nil {
		return nil, err
	}

	configBuilder := runnerConfigBuilder{
		assigner:      a,
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   "load",
	}
	return configBuilder.populateConfig()

}
