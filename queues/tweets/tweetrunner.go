package tweets

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/queues"
	"io/fs"
	"strconv"
	"sync"
	"time"
)

type Runner struct{}

type tweetSet struct {
	tweets []tweet
}

type tweet struct {
	id        uint64
	createdAt string
	text      string
}

//go:embed tweets_simple.csv
var tweetsFile embed.FS

func init() {
	queues.Register(Runner{})
	gob.Register(tweet{})
}

func (r Runner) RunQueueTests(hzCluster string, hzMembers []string) {

	runnerConfig := populateConfig()

	if !runnerConfig.Enabled {
		logInternalStateEvent("tweetrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	ts, err := parseTweets()
	if err != nil {
		logIoEvent(fmt.Sprintf("unable to parse tweets csv file: %s\n", err))
	}

	ctx := context.TODO()
	hzClient := initHazelcastClient(ctx, hzCluster, hzMembers)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("started tweets queue loop", log.InfoLevel)

	queue, err := hzClient.GetQueue(ctx, "awesomeQueue")
	if err != nil {
		logInternalStateEvent("unable to retrieve queue from hazelcast cluster", log.WarnLevel)
	}

	var wg sync.WaitGroup
	// One goroutine to add items, another one to poll them
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < len(ts.tweets); i++ {
			err := queue.Put(ctx, ts.tweets[i])
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to put tweet item into queue: %s\n", err), log.WarnLevel)
			}
			time.Sleep(time.Duration(1000) * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < len(ts.tweets); i++ {
			valueFromQueue, err := queue.PollWithTimeout(ctx, time.Duration(2000)*time.Millisecond)
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to poll tweet from queue: %s\n", err), log.WarnLevel)
			} else {
				logHzEvent(fmt.Sprintf("received value from queue: %s\n", valueFromQueue), log.TraceLevel)
			}
		}
	}()

}

func initHazelcastClient(ctx context.Context, hzCluster string, hzMembers []string) *hazelcast.Client {

	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-tweetrunner", client.ClientID()), hzCluster, hzMembers)
	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s\n", err), log.FatalLevel)
	}
	defer func(hzClient *hazelcast.Client, ctx context.Context) {
		err := hzClient.Shutdown(ctx)
		if err != nil {
			logHzEvent(fmt.Sprintf("unable to shutdown hazelcast client: %s\n", err), log.WarnLevel)
		}
	}(hzClient, ctx)

	return hzClient

}

func parseTweets() (*tweetSet, error) {

	tweetsCsv, err := tweetsFile.Open("tweets_simple.csv")

	if err != nil {
		return nil, err
	}

	defer func(tweetsCsv fs.File) {
		err := tweetsCsv.Close()
		if err != nil {
			logInternalStateEvent(fmt.Sprintf("unable to close tweets csv file: %v\n", err), log.WarnLevel)
		}
	}(tweetsCsv)

	reader := csv.NewReader(tweetsCsv)
	data, err := reader.ReadAll()

	if err != nil {
		return nil, err
	}

	return &tweetSet{
		parseCsv(data),
	}, nil

}

func parseCsv(data [][]string) []tweet {

	var tweets []tweet
	// Skip header line
	for i := 1; i < len(data); i++ {
		t := data[i]

		id, err := strconv.ParseUint(t[8], 10, 64)
		if err != nil {
			id = 0
		}
		createdAt := t[5]
		text := t[1]

		tweets = append(tweets, tweet{id, createdAt, text})
	}

	return tweets

}

func populateConfig() *queues.RunnerConfig {

	parsedConfig := config.GetParsedConfig()

	return queues.RunnerConfigBuilder{
		RunnerKeyPath: "queuetests.tweets",
		ParsedConfig:  parsedConfig,
	}.PopulateConfig()

}

func logInternalStateEvent(msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}

	if logLevel == log.TraceLevel {
		log.WithFields(fields).Trace(msg)
	} else {
		log.WithFields(fields).Info(msg)
	}

}

func logIoEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}).Fatal(msg)

}

func logHzEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}

	if level == log.FatalLevel {
		log.WithFields(fields).Fatal(msg)
	} else {
		log.WithFields(fields).Warn(msg)
	}

}
