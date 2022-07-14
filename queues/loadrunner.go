package queues

import (
	"context"
	"encoding/gob"
	"github.com/google/uuid"
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

	c := populateLoadConfig()

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	ctx := context.TODO()

	hzClient := client.NewHzClient().InitHazelcastClient(ctx, "queueloadrunner", hzCluster, hzMembers)
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting load test loop for queues", log.InfoLevel)

	t := testLoop[loadElement]{
		id:       uuid.New(),
		source:   "loadrunner",
		hzClient: hzClient,
		config:   c,
		elements: populateLoadElements(),
		ctx:      ctx,
	}

	t.run()

	lp.LogInternalStateEvent("finished queue load test loop", log.InfoLevel)

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

	runnerKeyPath := "queuetests.load"

	populateConfigProperty(runnerKeyPath+".numLoadEntries", func(a any) {
		numLoadEntries = a.(int)
	}, defaultNumLoadEntries)

	populateConfigProperty(runnerKeyPath+".payloadSizeBytes", func(a any) {
		payloadSizeBytes = a.(int)
	}, defaultPayloadSizeBytes)

	return PopulateConfig(runnerKeyPath, "load")

}

func populateConfigProperty(keyPath string, assignValue func(any), defaultValue any) {

	if value, err := config.ExtractConfigValue(config.GetParsedConfig(), keyPath); err != nil {
		lp.LogErrUponConfigExtraction(keyPath, err, log.FatalLevel)
		assignValue(defaultValue)
	} else {
		assignValue(value)
	}

}
