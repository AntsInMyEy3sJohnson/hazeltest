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
	"hazeltest/status"
	"io/fs"
)

type (
	tweetRunner struct {
		assigner   client.ConfigPropertyAssigner
		stateList  []state
		name       string
		source     string
		queueStore hzQueueStore
		l          looper[tweet]
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
	register(&tweetRunner{assigner: &client.DefaultConfigPropertyAssigner{}, stateList: []state{}, name: "queuesTweetRunner", source: "tweetRunner", queueStore: &defaultHzQueueStore{}, l: &testLoop[tweet]{}})
	gob.Register(tweet{})
}

func (r *tweetRunner) runQueueTests(hzCluster string, hzMembers []string) {

	r.appendState(start)

	config, err := populateConfig(r.assigner, "queueTests.tweets", "tweets")
	if err != nil {
		lp.LogRunnerEvent("unable to populate config for queue tweet runner -- aborting", log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		lp.LogRunnerEvent("tweet runner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	tc, err := parseTweets()
	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse tweets json file: %v", err), log.FatalLevel)
	}

	ctx := context.TODO()

	r.queueStore.InitHazelcastClient(ctx, r.name, hzCluster, hzMembers)
	defer func() {
		_ = r.queueStore.Shutdown(ctx)
	}()

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogRunnerEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogRunnerEvent("started tweets queue loop", log.InfoLevel)

	lc := &testLoopConfig[tweet]{id: uuid.New(), source: r.source, hzQueueStore: r.queueStore, runnerConfig: config, elements: tc.Tweets, ctx: ctx}
	r.l.init(lc, &defaultSleeper{}, status.NewGatherer())

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogRunnerEvent("finished tweet test loop", log.InfoLevel)

}

func (r *tweetRunner) appendState(s state) {
	r.stateList = append(r.stateList, s)
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
