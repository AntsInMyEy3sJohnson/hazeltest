package queues

import (
	"context"
	"encoding/gob"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/loadsupport"
)

type (
	loadRunner struct {
		stateList []state
		name      string
		source    string
		l         looper[loadElement]
	}
	loadElement struct {
		Payload string
	}
)

var (
	numLoadEntries   int
	payloadSizeBytes int
)

func init() {
	register(loadRunner{stateList: []state{}, name: "queues-loadrunner", source: "loadrunner", l: testLoop[loadElement]{}})
	gob.Register(loadElement{})
}

func (r loadRunner) runQueueTests(hzCluster string, hzMembers []string) {

	c, _ := populateLoadConfig()

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	ctx := context.TODO()

	hzClient := client.NewHzClientHelper().InitHazelcastClient(ctx, "queues-loadrunner", hzCluster, hzMembers)
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting load test loop for queues", log.InfoLevel)

	lc := &testLoopConfig[loadElement]{id: uuid.New(), source: r.source, hzClient: hzClient, runnerConfig: c, elements: populateLoadElements(), ctx: ctx}

	r.l.init(lc)
	r.l.run()

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

func populateLoadConfig() (*runnerConfig, error) {

	runnerKeyPath := "queuetests.load"

	a := client.DefaultConfigPropertyAssigner{}

	// TODO Error handling
	_ = a.Assign(runnerKeyPath+".numLoadEntries", func(a any) {
		numLoadEntries = a.(int)
	})

	_ = a.Assign(runnerKeyPath+".payloadSizeBytes", func(a any) {
		payloadSizeBytes = a.(int)
	})

	return populateConfig(runnerKeyPath, "load")

}
