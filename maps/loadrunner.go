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
	"hazeltest/loadsupport"
	"hazeltest/status"
	"strconv"
)

type (
	loadRunner struct {
		assigner  client.ConfigPropertyAssigner
		stateList []state
		name      string
		source    string
		mapStore  hzMapStore
		l         looper[loadElement]
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
	register(&loadRunner{assigner: &client.DefaultConfigPropertyAssigner{}, stateList: []state{}, name: "mapsLoadrunner", source: "loadRunner", mapStore: &defaultHzMapStore{}, l: &batchTestLoop[loadElement]{}})
	gob.Register(loadElement{})
}

func (r *loadRunner) runMapTests(hzCluster string, hzMembers []string) {

	r.appendState(start)

	loadRunnerConfig, err := populateLoadConfig(r.assigner)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of map load runner: unable to populate config: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !loadRunnerConfig.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadRunner from maps/loadRunner
		lp.LogRunnerEvent("loadRunner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	ctx := context.TODO()

	r.mapStore.InitHazelcastClient(ctx, "mapsLoadRunner", hzCluster, hzMembers)
	defer func() {
		_ = r.mapStore.Shutdown(ctx)
	}()

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogRunnerEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogRunnerEvent("starting load test loop for maps", log.InfoLevel)

	lc := &batchTestLoopConfig[loadElement]{uuid.New(), r.source, r.mapStore, loadRunnerConfig, populateLoadElements(), ctx, getLoadElementID, deserializeLoadElement}

	r.l.init(lc, &defaultSleeper{}, status.NewGatherer())

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogRunnerEvent("finished map load test loop", log.InfoLevel)

}

func (r *loadRunner) appendState(s state) {

	r.stateList = append(r.stateList, s)

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

func deserializeLoadElement(elementFromHz any) error {

	_, ok := elementFromHz.(loadElement)

	if !ok {
		return errors.New("unable to deserialize value retrieved from hazelcast map into loadelement instance")
	}

	return nil

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
