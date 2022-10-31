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
		stateList  []state
		name       string
		source     string
		queueStore hzQueueStore
		l          looper[loadElement]
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
	register(&loadRunner{stateList: []state{}, name: "queues-loadrunner", source: "loadrunner", queueStore: &defaultHzQueueStore{}, l: &testLoop[loadElement]{}})
	gob.Register(loadElement{})
}

func (r *loadRunner) runQueueTests(hzCluster string, hzMembers []string) {

	r.appendState(start)

	c, err := populateLoadConfig()
	if err != nil {
		lp.LogInternalStateEvent("unable to populate config for queue load runner -- aborting", log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadrunner from maps/loadrunner
		lp.LogInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	ctx := context.TODO()

	r.queueStore.InitHazelcastClient(ctx, "queues-loadrunner", hzCluster, hzMembers)
	defer r.queueStore.Shutdown(ctx)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting load test loop for queues", log.InfoLevel)

	lc := &testLoopConfig[loadElement]{id: uuid.New(), source: r.source, hzQueueStore: r.queueStore, runnerConfig: c, elements: populateLoadElements(), ctx: ctx}

	r.l.init(lc)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogInternalStateEvent("finished queue load test loop", log.InfoLevel)

}

func (r *loadRunner) appendState(s state) {

	r.stateList = append(r.stateList, s)

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

	if err := propertyAssigner.Assign(runnerKeyPath+".numLoadEntries", client.ValidateInt, func(a any) {
		numLoadEntries = a.(int)
	}); err != nil {
		return nil, err
	}

	if err := propertyAssigner.Assign(runnerKeyPath+".payloadSizeBytes", client.ValidateInt, func(a any) {
		payloadSizeBytes = a.(int)
	}); err != nil {
		return nil, err
	}

	return populateConfig(runnerKeyPath, "load")

}
