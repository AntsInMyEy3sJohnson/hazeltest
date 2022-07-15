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

const (
	defaultNumEntriesPerMap = 10000
	defaultPayloadSizeBytes = 1000
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

	mapRunnerConfig := populateLoadConfig()

	if !mapRunnerConfig.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	ctx := context.TODO()

	hzClient := client.NewHzClient().InitHazelcastClient(ctx, "maploadrunner", hzCluster, hzMembers)

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

func populateLoadConfig() *runnerConfig {

	parsedConfig := client.GetParsedConfig()
	runnerKeyPath := "maptests.load"

	keyPath := runnerKeyPath + ".numEntriesPerMap"
	valueFromConfig, err := client.RetrieveConfigValue(parsedConfig, keyPath)
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		numEntriesPerMap = defaultNumEntriesPerMap
	} else {
		numEntriesPerMap = valueFromConfig.(int)
	}

	keyPath = runnerKeyPath + ".payloadSizeBytes"
	valueFromConfig, err = client.RetrieveConfigValue(parsedConfig, keyPath)
	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		payloadSizeBytes = defaultPayloadSizeBytes
	} else {
		payloadSizeBytes = valueFromConfig.(int)
	}

	configBuilder := runnerConfigBuilder{
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   "load",
		parsedConfig:  parsedConfig,
	}
	return configBuilder.populateConfig()

}
