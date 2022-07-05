package tweets

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/queues"
	"io/fs"
	"sync"
	"time"
)

type Runner struct{}

type tweetCollection struct {
	Tweets []tweet `json:"Tweets"`
}

type tweet struct {
	Id        uint64 `json:"Id"`
	CreatedAt string `json:"CreatedAt"`
	Text      string `json:"Text"`
}

const queueOperationLoggingUpdateStep = 50

//go:embed tweets_simple.json
var tweetsFile embed.FS

func init() {
	queues.Register(Runner{})
	gob.Register(tweet{})
}

func (r Runner) RunQueueTests(hzCluster string, hzMembers []string) {

	c := populateConfig()

	if !c.Enabled {
		logInternalStateEvent("tweetrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	tc, err := parseTweets()
	if err != nil {
		logIoEvent(fmt.Sprintf("unable to parse tweets json file: %v", err), log.FatalLevel)
	}

	ctx := context.TODO()
	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-tweetrunner", client.ClientID()), hzCluster, hzMembers)

	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %v", err), log.FatalLevel)
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("started tweets queue loop", log.InfoLevel)

	var numQueuesWg sync.WaitGroup
	for i := 0; i < c.NumQueues; i++ {
		numQueuesWg.Add(1)
		queueName := assembleQueueName(c, i)
		logInternalStateEvent(fmt.Sprintf("using queue name '%s' in queue goroutine %d", queueName, i), log.InfoLevel)
		q, err := hzClient.GetQueue(ctx, queueName)
		if err != nil {
			logHzEvent("unable to retrieve queue from hazelcast cluster", log.FatalLevel)
		}
		go func(i int) {
			defer numQueuesWg.Done()
			if c.PutConfig.Enabled {
				var putWg sync.WaitGroup
				putWg.Add(1)
				go func() {
					defer putWg.Done()
					runTweetLoop(c.PutConfig, tc, q, ctx, "put", queueName, i, putTweets)
				}()
				putWg.Wait()
			}

			if c.PollConfig.Enabled {
				var pollWg sync.WaitGroup
				pollWg.Add(1)
				go func() {
					defer pollWg.Done()
					runTweetLoop(c.PollConfig, tc, q, ctx, "poll", queueName, i, pollTweets)
				}()
				pollWg.Wait()
			}

		}(i)
	}

	numQueuesWg.Wait()

}

func runTweetLoop(config *queues.OperationConfig, tc *tweetCollection, q *hazelcast.Queue, ctx context.Context, operation string, queueName string, queueNumber int, queueFunction func([]tweet, *hazelcast.Queue, context.Context, *queues.OperationConfig, string)) {

	sleep(config.InitialDelay, "initialDelay", queueName, operation)

	numRuns := config.NumRuns
	for i := 0; i < numRuns; i++ {
		if i > 0 {
			sleep(config.SleepBetweenRuns, "betweenRuns", queueName, operation)
		}
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d of %d %s runs for queue %s in queue goroutine %d", i, numRuns, operation, queueName, queueNumber), log.InfoLevel)
		}
		queueFunction(tc.Tweets, q, ctx, config, queueName)
		logInternalStateEvent(fmt.Sprintf("finished %sing one set of %d tweets in queue %s after run %d of %d on queue goroutine %d", operation, len(tc.Tweets), queueName, i, numRuns, queueNumber), log.TraceLevel)
	}

	logInternalStateEvent(fmt.Sprintf("%s test loop done on queue '%s' in queue goroutine %d", operation, queueName, queueNumber), log.InfoLevel)

}

func putTweets(tweets []tweet, q *hazelcast.Queue, ctx context.Context, putConfig *queues.OperationConfig, queueName string) {

	for i := 0; i < len(tweets); i++ {
		tweet := tweets[i]
		err := q.Put(ctx, tweet)
		if err != nil {
			logInternalStateEvent(fmt.Sprintf("unable to put tweet item into queue '%s': %s", queueName, err), log.WarnLevel)
		} else {
			logInternalStateEvent(fmt.Sprintf("successfully wrote value to queue '%s': %v", queueName, tweet), log.TraceLevel)
		}
		if i > 0 && i%putConfig.BatchSize == 0 {
			sleep(putConfig.SleepBetweenActionBatches, "betweenActionBatches", queueName, "put")
		}
	}

}

func pollTweets(tweets []tweet, q *hazelcast.Queue, ctx context.Context, pollConfig *queues.OperationConfig, queueName string) {

	for i := 0; i < len(tweets); i++ {
		valueFromQueue, err := q.Poll(ctx)
		if err != nil {
			logInternalStateEvent(fmt.Sprintf("unable to poll tweet from queue '%s': %s", queueName, err), log.WarnLevel)
		} else if valueFromQueue == nil {
			logInternalStateEvent(fmt.Sprintf("nothing to poll from queue '%s'", queueName), log.TraceLevel)
		} else {
			logInternalStateEvent(fmt.Sprintf("retrieved value from queue '%s': %v", queueName, valueFromQueue), log.TraceLevel)
		}
		if i > 0 && i%pollConfig.BatchSize == 0 {
			sleep(pollConfig.SleepBetweenActionBatches, "betweenActionBatches", queueName, "poll")
		}
	}

}

func sleep(sleepConfig *queues.SleepConfig, kind string, queueName string, operation string) {

	if sleepConfig.Enabled {
		logInternalStateEvent(fmt.Sprintf("sleeping for %d milliseconds for kind '%s' on queue '%s' for operation '%s'", sleepConfig.DurationMs, kind, queueName, operation), log.TraceLevel)
		time.Sleep(time.Duration(sleepConfig.DurationMs) * time.Millisecond)
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
			logIoEvent(fmt.Sprintf("unable to close tweets json file: %v", err), log.WarnLevel)
		}
	}(tweetsJson)

	var tc tweetCollection
	err = json.NewDecoder(tweetsJson).Decode(&tc)

	if err != nil {
		return nil, err
	}

	return &tc, nil

}

func populateConfig() *queues.RunnerConfig {

	parsedConfig := config.GetParsedConfig()

	return queues.RunnerConfigBuilder{
		RunnerKeyPath: "queuetests.tweets",
		QueueBaseName: "tweets",
		ParsedConfig:  parsedConfig,
	}.PopulateConfig()

}

func assembleQueueName(config *queues.RunnerConfig, queueIndex int) string {

	queueName := config.QueueBaseName

	if config.AppendQueueIndexToQueueName {
		queueName = fmt.Sprintf("%s-%s", queueName, queueIndex)
	}
	if config.AppendClientIdToQueueName {
		queueName = fmt.Sprintf("%s-%s", queueName, client.ClientID())
	}

	return queueName

}

func logInternalStateEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}

	doLog(msg, fields, level)

}

func logIoEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}

	doLog(msg, fields, level)

}

func logHzEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}

	doLog(msg, fields, level)

}

func doLog(msg string, fields log.Fields, level log.Level) {

	if level == log.FatalLevel {
		log.WithFields(fields).Fatal(msg)
	} else if level == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else if level == log.InfoLevel {
		log.WithFields(fields).Info(msg)
	} else {
		log.WithFields(fields).Trace(msg)
	}

}
