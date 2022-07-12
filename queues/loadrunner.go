package queues

import (
	"context"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/loadsupport"
)

type (
	loadRunner  struct{}
	loadElement struct {
		Payload string
	}
)

const (
	defaultNumLoadEntries   = 5000
	defaultPayloadSizeBytes = 1000
)

var (
	numLoadEntries   int
	payloadSizeBytes int
)

func init() {
	register(loadRunner{})
	gob.Register(loadElement{})
}

func (r loadRunner) runQueueTests(hzCluster string, hzMembers []string) {

	c := PopulateConfig("queuetests.load", "load")

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
	}

	api.RaiseNotReady()

	ctx := context.TODO()

	hzClient := client.NewHzClient().InitHazelcastClient(ctx, "queueloadrunner", hzCluster, hzMembers)
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting load test loop for queues", log.InfoLevel)

}

func populateLoadElements() []loadElement {

	elements := make([]loadElement, numLoadEntries)

	randomPayload := loadsupport.GenerateRandomStringPayload(payloadSizeBytes)

	for i := 0; i < numLoadEntries; i++ {
		elements[i] = loadElement{Payload: randomPayload}
	}

	return elements

}

func populateLoadConfig() *runnerConfig {

	parsedConfig := config.GetParsedConfig()
	runnerKeyPath := "queuetests.load"

	keyPath := runnerKeyPath + ".numLoadEntries"
	valueFromConfig, err := config.ExtractConfigValue(parsedConfig, keyPath)

	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		numLoadEntries = defaultNumLoadEntries
	} else {
		numLoadEntries = valueFromConfig.(int)
	}

	keyPath = runnerKeyPath + ".payloadSizeBytes"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)

	if err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.WarnLevel)
		payloadSizeBytes = defaultPayloadSizeBytes
	} else {
		payloadSizeBytes = valueFromConfig.(int)
	}

	return PopulateConfig(runnerKeyPath, "load")

}
