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
		queueName := assembleQueueName(c)
		logInternalStateEvent(fmt.Sprintf("using queue name '%s' in queue goroutine %d", queueName, i), log.InfoLevel)
		q, err := hzClient.GetQueue(ctx, assembleQueueName(c))
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
					runPutLoop(c.PutConfig, tc, q, ctx, queueName, i)
				}()
				putWg.Wait()
			}

			if c.PollConfig.Enabled {
				var pollWg sync.WaitGroup
				pollWg.Add(1)
				go func() {
					defer pollWg.Done()
					runPollLoop(c.PollConfig, tc, q, ctx, queueName, i)
				}()
				pollWg.Wait()
			}

		}(i)
	}

	numQueuesWg.Wait()

}

func runPutLoop(config *queues.OperationConfig, tc *tweetCollection, q *hazelcast.Queue, ctx context.Context, queueName string, queueNumber int) {

	numRuns := config.NumRuns
	for i := 0; i < numRuns; i++ {
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d of %d put runs for queue %s in queue goroutine %d", i, numRuns, queueName, queueNumber), log.InfoLevel)
		}
		for j := 0; j < len(tc.Tweets); j++ {
			tweet := tc.Tweets[j]
			err := q.Put(ctx, tweet)
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to put tweet item into queue: %s", err), log.WarnLevel)
			} else {
				logHzEvent(fmt.Sprintf("successfully wrote value to queue: %v", tweet), log.TraceLevel)
			}
			//time.Sleep(time.Duration(1000) * time.Millisecond)
		}
		logInternalStateEvent(fmt.Sprintf("finished putting one batch of tweets in queue %s after run %d of %d on queue goroutine %d", queueName, i, numRuns, queueNumber), log.TraceLevel)
	}

	logInternalStateEvent(fmt.Sprintf("put test loop done on queue '%s' in queue goroutine %d", queueName, queueNumber), log.InfoLevel)

}

func runPollLoop(config *queues.OperationConfig, tc *tweetCollection, q *hazelcast.Queue, ctx context.Context, queueName string, queueNumber int) {

	numRuns := config.NumRuns
	for i := 0; i < config.NumRuns; i++ {
		if i > 0 && i%queueOperationLoggingUpdateStep == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d of %d poll runs for queue %s in queue goroutine %d", i, numRuns, queueName, queueNumber), log.InfoLevel)
		}
		for j := 0; j < len(tc.Tweets); j++ {
			valueFromQueue, err := q.Poll(ctx)
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to poll tweet from queue: %s", err), log.WarnLevel)
			} else if valueFromQueue == nil {
				logHzEvent(fmt.Sprintf("nothing to poll from queue '%s'", queueName), log.TraceLevel)
			} else {
				logHzEvent(fmt.Sprintf("received value from queue: %s", valueFromQueue), log.TraceLevel)
			}
			//time.Sleep(time.Duration(1000) * time.Millisecond)
		}
		logInternalStateEvent(fmt.Sprintf("finished polling one batch of tweets in queue %s after run %d of %d on queue goroutine %d", queueName, i, numRuns, queueNumber), log.TraceLevel)
	}

	logInternalStateEvent(fmt.Sprintf("poll test loop done on queue '%s' in queue goroutine %d", queueName, queueNumber), log.InfoLevel)

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

func assembleQueueName(config *queues.RunnerConfig) string {

	// TODO Make this configurable based on append* properties
	return config.QueueBaseName

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
