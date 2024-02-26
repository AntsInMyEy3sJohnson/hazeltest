package queues

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/loadsupport"
	"hazeltest/status"
)

type (
	loadRunner struct {
		assigner   client.ConfigPropertyAssigner
		stateList  []state
		name       string
		source     string
		queueStore hzQueueStore
		l          looper[loadElement]
		gatherer   *status.Gatherer
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
		assigner:   &client.DefaultConfigPropertyAssigner{},
		stateList:  []state{},
		name:       "queuesLoadRunner",
		source:     "loadRunner",
		queueStore: &defaultHzQueueStore{},
		l:          &testLoop[loadElement]{},
	})
	gob.Register(loadElement{})
}

func (r *loadRunner) getSourceName() string {
	return r.source
}

func (r *loadRunner) runQueueTests(hzCluster string, hzMembers []string, gatherer *status.Gatherer) {

	r.gatherer = gatherer
	r.appendState(start)

	c, err := populateLoadConfig(r.assigner)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of queue load runner: unable to populate config due to error: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !c.enabled {
		// The source field being part of the generated log line can be used to disambiguate queues/loadRunner from maps/loadRunner
		lp.LogRunnerEvent("load runner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	ctx := context.TODO()

	r.queueStore.InitHazelcastClient(ctx, "queuesLoadRunner", hzCluster, hzMembers)
	defer func() {
		_ = r.queueStore.Shutdown(ctx)
	}()

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogRunnerEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogRunnerEvent("starting load test loop for queues", log.InfoLevel)

	lc := &testLoopConfig[loadElement]{id: uuid.New(), source: r.source, hzQueueStore: r.queueStore, runnerConfig: c, elements: populateLoadElements(), ctx: ctx}

	r.l.init(lc, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogRunnerEvent("finished queue load test loop", log.InfoLevel)

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
