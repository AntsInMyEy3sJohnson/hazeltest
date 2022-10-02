package maps

import (
	"context"
	"encoding/gob"
	"errors"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/loadsupport"
	"strconv"
)

type (
	loadRunner  struct{}
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
	register(loadRunner{})
	gob.Register(loadElement{})
}

func (r loadRunner) runMapTests(hzCluster string, hzMembers []string) {

	// TODO Handle error
	mapRunnerConfig, _ := populateLoadConfig()

	if !mapRunnerConfig.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	ctx := context.TODO()

	hzClient := client.NewHzClientHelper().InitHazelcastClient(ctx, "maps-loadrunner", hzCluster, hzMembers)

	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting load test loop for maps", log.InfoLevel)

	elements := populateLoadElements()

	testLoop := testLoop[loadElement]{
		id:                     uuid.New(),
		source:                 "loadrunner",
		hzClient:               hzClient,
		config:                 mapRunnerConfig,
		elements:               elements,
		ctx:                    ctx,
		getElementIdFunc:       getLoadElementID,
		deserializeElementFunc: deserializeLoadElement,
	}

	testLoop.run()

	lp.LogInternalStateEvent("finished map load test loop", log.InfoLevel)

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

func getLoadElementID(element interface{}) string {

	loadElement := element.(loadElement)
	return loadElement.Key

}

func deserializeLoadElement(elementFromHz interface{}) error {

	_, ok := elementFromHz.(loadElement)

	if !ok {
		return errors.New("unable to deserialize value retrieved from hazelcast map into loadelement instance")
	}

	return nil

}

func populateLoadConfig() (*runnerConfig, error) {

	runnerKeyPath := "maptests.load"

	a := client.DefaultConfigPropertyAssigner{}

	// TODO Handle error
	_ = a.Assign(runnerKeyPath+".numEntriesPerMap", func(a any) {
		numEntriesPerMap = a.(int)
	})

	_ = a.Assign(runnerKeyPath+".payloadSizeBytes", func(a any) {
		payloadSizeBytes = a.(int)
	})

	configBuilder := runnerConfigBuilder{
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   "load",
	}
	return configBuilder.populateConfig(a)

}
