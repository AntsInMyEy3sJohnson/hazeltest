package queues

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
)

type (
	loadRunner struct {
		assigner        client.ConfigPropertyAssigner
		stateList       []state
		name            string
		source          string
		hzClientHandler hazelcastwrapper.HzClientHandler
		hzQueueStore    hazelcastwrapper.QueueStore
		l               looper[loadElement]
		gatherer        status.Gatherer
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
	register(&loadRunner{
		assigner:        &client.DefaultConfigPropertyAssigner{},
		stateList:       []state{},
		name:            "queuesLoadRunner",
		source:          "loadRunner",
		hzClientHandler: &hazelcastwrapper.DefaultHzClientHandler{},
		l:               &testLoop[loadElement]{},
	})
	gob.Register(loadElement{})
}

func (r *loadRunner) getSourceName() string {
	return r.source
}

func (r *loadRunner) runQueueTests(hzCluster string, hzMembers []string, gatherer status.Gatherer, storeFunc initQueueStoreFunc) {

	r.gatherer = gatherer
	r.appendState(start)

	c, err := populateLoadConfig(r.assigner)
	if err != nil {
		lp.LogQueueRunnerEvent(fmt.Sprintf("aborting launch of queue load runner: unable to populate config due to error: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadRunner from maps/loadRunner
		lp.LogQueueRunnerEvent("load runner not enabled -- won't run", r.name, log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	ctx := context.TODO()

	r.hzClientHandler.InitHazelcastClient(ctx, "queuesLoadRunner", hzCluster, hzMembers)
	defer func() {
		_ = r.hzClientHandler.Shutdown(ctx)
	}()
	r.hzQueueStore = storeFunc(r.hzClientHandler)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogQueueRunnerEvent("initialized hazelcast client", r.name, log.InfoLevel)
	lp.LogQueueRunnerEvent("starting load test loop for queues", r.name, log.InfoLevel)

	lc := &testLoopExecution[loadElement]{id: uuid.New(), runnerName: r.name, source: r.source, hzQueueStore: r.hzQueueStore, runnerConfig: c, elements: populateLoadElements(), ctx: ctx}

	r.l.init(lc, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogQueueRunnerEvent("finished queue load test loop", r.name, log.InfoLevel)

}

func (r *loadRunner) appendState(s state) {

	r.stateList = append(r.stateList, s)
	r.gatherer.Gather(status.Update{Key: string(statusKeyCurrentState), Value: string(s)})

}

func populateLoadElements() []loadElement {

	elements := make([]loadElement, numLoadEntries)

	randomPayload := loadsupport.GenerateRandomStringPayload(payloadSizeBytes)

	for i := 0; i < numLoadEntries; i++ {
		elements[i] = loadElement{Payload: randomPayload}
	}

	return elements

}

func populateLoadConfig(assigner client.ConfigPropertyAssigner) (*runnerConfig, error) {

	runnerKeyPath := "queueTests.load"

	if err := assigner.Assign(runnerKeyPath+".numLoadEntries", client.ValidateInt, func(a any) {
		numLoadEntries = a.(int)
	}); err != nil {
		return nil, err
	}

	if err := assigner.Assign(runnerKeyPath+".payloadSizeBytes", client.ValidateInt, func(a any) {
		payloadSizeBytes = a.(int)
	}); err != nil {
		return nil, err
	}

	return populateConfig(assigner, runnerKeyPath, "load")

}
