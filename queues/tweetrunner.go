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
	"io/fs"
)

type (
	tweetRunner     struct{}
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
	register(tweetRunner{})
	gob.Register(tweet{})
}

func (r tweetRunner) runQueueTests(hzCluster string, hzMembers []string) {

	c := PopulateConfig("queuetests.tweets", "tweets")

	if !c.enabled {
		lp.LogInternalStateEvent("tweetrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	tc, err := parseTweets()
	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse tweets json file: %v", err), log.FatalLevel)
	}

	ctx := context.TODO()

	hzClient := client.NewHzClient().InitHazelcastClient(ctx, "tweetrunner", hzCluster, hzMembers)
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("started tweets queue loop", log.InfoLevel)

	t := testLoop[tweet]{
		id:       uuid.New(),
		source:   "tweetrunner",
		hzClient: hzClient,
		config:   c,
		elements: tc.Tweets,
		ctx:      ctx,
	}

	t.run()

	lp.LogInternalStateEvent("finished tweet test loop", log.InfoLevel)

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
