package tweets

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
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

//go:embed tweets_simple.json
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
		logIoEvent(fmt.Sprintf("unable to parse tweets json file: %v\n", err), log.FatalLevel)
	}

	ctx := context.TODO()
	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-tweetrunner", client.ClientID()), hzCluster, hzMembers)

	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %v\n", err), log.FatalLevel)
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("started tweets queue loop", log.InfoLevel)

	queue, err := hzClient.GetQueue(ctx, "awesomeQueue")
	if err != nil {
		logHzEvent("unable to retrieve queue from hazelcast cluster", log.FatalLevel)
	}

	err = queue.Put(ctx, ts.Tweets[1])
	value, err := queue.Poll(ctx)

	fmt.Printf("received value from queue: %v\n", value)

	var wg sync.WaitGroup
	// One goroutine to add items, another one to poll them
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < len(ts.Tweets); i++ {
			err := queue.Put(ctx, ts.Tweets[i])
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to put tweet item into queue: %s\n", err), log.WarnLevel)
			}
			time.Sleep(time.Duration(1000) * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < len(ts.Tweets); i++ {
			valueFromQueue, err := queue.PollWithTimeout(ctx, time.Duration(2000)*time.Millisecond)
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to poll tweet from queue: %s\n", err), log.WarnLevel)
			} else {
				logHzEvent(fmt.Sprintf("received value from queue: %s\n", valueFromQueue), log.TraceLevel)
			}
		}
	}()

	wg.Wait()

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
			logIoEvent(fmt.Sprintf("unable to close tweets json file: %v\n", err), log.WarnLevel)
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
		RunnerKeyPath: "queuetests.Tweets",
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

func logIoEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}

	if level == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

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
