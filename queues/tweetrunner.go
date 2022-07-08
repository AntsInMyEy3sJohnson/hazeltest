package queues

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/client/config"
	"io/fs"
	"sync"
	"time"
)

type queueRunner struct{}

type tweetCollection struct {
	Tweets []tweet `json:"Tweets"`
}

type tweet struct {
	Id        uint64 `json:"Id"`
	CreatedAt string `json:"CreatedAt"`
	Text      string `json:"Text"`
}

const queueOperationLoggingUpdateStep = 10

//go:embed tweets_simple.json
var tweetsFile embed.FS

func init() {
	register(queueRunner{})
	gob.Register(tweet{})
}

func (r queueRunner) runQueueTests(hzCluster string, hzMembers []string) {

	c := populateConfig()

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
	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-tweetrunner", client.ClientID()), hzCluster, hzMembers)

	if err != nil {
		lp.LogHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %v", err), log.FatalLevel)
	}
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("started tweets queue loop", log.InfoLevel)

	var numQueuesWg sync.WaitGroup
	for i := 0; i < c.numQueues; i++ {
		numQueuesWg.Add(1)
		queueName := assembleQueueName(c, i)
		lp.LogInternalStateEvent(fmt.Sprintf("using queue name '%s' in queue goroutine %d", queueName, i), log.InfoLevel)
		q, err := hzClient.GetQueue(ctx, queueName)
		if err != nil {
			lp.LogHzEvent("unable to retrieve queue from hazelcast cluster", log.FatalLevel)
		}
		go func(i int) {
			defer numQueuesWg.Done()

			var putWg sync.WaitGroup
			if c.putConfig.enabled {
				putWg.Add(1)
				go func() {
					defer putWg.Done()
					runTweetLoop(c.putConfig, tc, q, ctx, "put", queueName, i, putTweets)
				}()

			}

			var pollWg sync.WaitGroup
			if c.pollConfig.enabled {
				pollWg.Add(1)
				go func() {
					defer pollWg.Done()
					runTweetLoop(c.pollConfig, tc, q, ctx, "poll", queueName, i, pollTweets)
				}()
			}

			putWg.Wait()
			pollWg.Wait()

		}(i)
	}

	numQueuesWg.Wait()

}

func runTweetLoop(config *operationConfig, tc *tweetCollection, q *hazelcast.Queue, ctx context.Context, operation string, queueName string, queueNumber int, queueFunction func([]tweet, *hazelcast.Queue, context.Context, *operationConfig, string)) {

	sleep(config.initialDelay, "initialDelay", queueName, operation)

	numRuns := config.numRuns
	for i := 0; i < numRuns; i++ {
		if i > 0 {
			sleep(config.sleepBetweenRuns, "betweenRuns", queueName, operation)
		}
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			lp.LogInternalStateEvent(fmt.Sprintf("finished %d of %d %s runs for queue %s in queue goroutine %d", i, numRuns, operation, queueName, queueNumber), log.InfoLevel)
		}
		queueFunction(tc.Tweets, q, ctx, config, queueName)
		lp.LogInternalStateEvent(fmt.Sprintf("finished %sing one set of %d tweets in queue %s after run %d of %d on queue goroutine %d", operation, len(tc.Tweets), queueName, i, numRuns, queueNumber), log.TraceLevel)
	}

	lp.LogInternalStateEvent(fmt.Sprintf("%s test loop done on queue '%s' in queue goroutine %d", operation, queueName, queueNumber), log.InfoLevel)

}

func putTweets(tweets []tweet, q *hazelcast.Queue, ctx context.Context, putConfig *operationConfig, queueName string) {

	for i := 0; i < len(tweets); i++ {
		tweet := tweets[i]
		err := q.Put(ctx, tweet)
		if err != nil {
			lp.LogInternalStateEvent(fmt.Sprintf("unable to put tweet item into queue '%s': %s", queueName, err), log.WarnLevel)
		} else {
			lp.LogInternalStateEvent(fmt.Sprintf("successfully wrote value to queue '%s': %v", queueName, tweet), log.TraceLevel)
		}
		if i > 0 && i%putConfig.batchSize == 0 {
			sleep(putConfig.sleepBetweenActionBatches, "betweenActionBatches", queueName, "put")
		}
	}

}

func pollTweets(tweets []tweet, q *hazelcast.Queue, ctx context.Context, pollConfig *operationConfig, queueName string) {

	for i := 0; i < len(tweets); i++ {
		valueFromQueue, err := q.Poll(ctx)
		if err != nil {
			lp.LogInternalStateEvent(fmt.Sprintf("unable to poll tweet from queue '%s': %s", queueName, err), log.WarnLevel)
		} else if valueFromQueue == nil {
			lp.LogInternalStateEvent(fmt.Sprintf("nothing to poll from queue '%s'", queueName), log.TraceLevel)
		} else {
			lp.LogInternalStateEvent(fmt.Sprintf("retrieved value from queue '%s': %v", queueName, valueFromQueue), log.TraceLevel)
		}
		if i > 0 && i%pollConfig.batchSize == 0 {
			sleep(pollConfig.sleepBetweenActionBatches, "betweenActionBatches", queueName, "poll")
		}
	}

}

func sleep(sleepConfig *sleepConfig, kind string, queueName string, operation string) {

	if sleepConfig.enabled {
		lp.LogInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds for kind '%s' on queue '%s' for operation '%s'", sleepConfig.durationMs, kind, queueName, operation), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.durationMs) * time.Millisecond)
	}

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

func populateConfig() *runnerConfig {

	parsedConfig := config.GetParsedConfig()

	return runnerConfigBuilder{
		runnerKeyPath: "queuetests.tweets",
		queueBaseName: "tweets",
		parsedConfig:  parsedConfig,
	}.populateConfig()

}

func assembleQueueName(config *runnerConfig, queueIndex int) string {

	queueName := config.queueBaseName

	if config.useQueuePrefix && config.queuePrefix != "" {
		queueName = fmt.Sprintf("%s%s", config.queuePrefix, queueName)
	}
	if config.appendQueueIndexToQueueName {
		queueName = fmt.Sprintf("%s-%d", queueName, queueIndex)
	}
	if config.appendClientIdToQueueName {
		queueName = fmt.Sprintf("%s-%s", queueName, client.ClientID())
	}

	return queueName

}
