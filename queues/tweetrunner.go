package queues

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/status"
	"io/fs"
)

type (
	tweetRunner struct {
		assigner        client.ConfigPropertyAssigner
		stateList       []state
		name            string
		source          string
		hzClientHandler hazelcastwrapper.HzClientHandler
		hzQueueStore    hazelcastwrapper.QueueStore
		l               looper[tweet]
		gatherer        status.Gatherer
	}
	tweetCollection struct {
		Tweets []tweet `json:"Tweets"`
	}
	tweet struct {
		Id        uint64 `json:"Id"`
		CreatedAt string `json:"CreatedAt"`
		Text      string `json:"Text"`
	}
)

const queueOperationLoggingUpdateStep = 10

//go:embed tweets_simple.json
var tweetsFile embed.FS

func init() {
	register(&tweetRunner{
		assigner:        &client.DefaultConfigPropertyAssigner{},
		stateList:       []state{},
		name:            "queuesTweetRunner",
		source:          "tweetRunner",
		hzClientHandler: &hazelcastwrapper.DefaultHzClientHandler{},
		l:               &testLoop[tweet]{},
	})
	gob.Register(tweet{})
}

func (r *tweetRunner) getSourceName() string {
	return "tweetRunner"
}

func (r *tweetRunner) runQueueTests(hzCluster string, hzMembers []string, gatherer status.Gatherer, storeFunc initQueueStoreFunc) {

	r.gatherer = gatherer
	r.appendState(start)

	config, err := populateConfig(r.assigner, "queueTests.tweets", "tweets")
	if err != nil {
		lp.LogQueueRunnerEvent(fmt.Sprintf("aborting launch of queue tweet runner: unable to populate config due to error: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		lp.LogQueueRunnerEvent("tweet runner not enabled -- won't run", r.name, log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	tc, err := parseTweets()
	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse tweets json file: %v", err), log.FatalLevel)
	}

	ctx := context.TODO()

	r.hzClientHandler.InitHazelcastClient(ctx, r.name, hzCluster, hzMembers)
	defer func() {
		_ = r.hzClientHandler.Shutdown(ctx)
	}()
	r.hzQueueStore = storeFunc(r.hzClientHandler)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogQueueRunnerEvent("initialized hazelcast client", r.name, log.InfoLevel)
	lp.LogQueueRunnerEvent("started tweets queue loop", r.name, log.InfoLevel)

	lc := &testLoopExecution[tweet]{id: uuid.New(), runnerName: r.name, source: r.source, hzQueueStore: r.hzQueueStore, runnerConfig: config, elements: tc.Tweets, ctx: ctx}
	r.l.init(lc, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogQueueRunnerEvent("finished tweet test loop", r.name, log.InfoLevel)

}

func (r *tweetRunner) appendState(s state) {
	r.stateList = append(r.stateList, s)

	r.gatherer.Gather(status.Update{Key: string(statusKeyCurrentState), Value: string(s)})
}

func parseTweets() (*tweetCollection, error) {

	// TODO Refactor logic related to file parsing into common file? Parsing json files is required in PokedexRunner, too... redundancy vs. coupling

	tweetsJson, err := tweetsFile.Open("tweets_simple.json")

	if err != nil {
		return nil, err
	}

	defer func(tweetsCsv fs.File) {
		err := tweetsCsv.Close()
		if err != nil {
			lp.LogIoEvent(fmt.Sprintf("unable to close tweets json file: %v", err), log.WarnLevel)
		}
	}(tweetsJson)

	var tc tweetCollection
	err = json.NewDecoder(tweetsJson).Decode(&tc)

	if err != nil {
		return nil, err
	}

	return &tc, nil

}
