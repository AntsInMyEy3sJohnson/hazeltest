package main

import (
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/logging"
	"hazeltest/maps"
	"hazeltest/queues"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {

	if err := client.ParseConfigs(); err != nil {
		logConfigurationError("N/A", "config file", err.Error())
	}

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		logConfigurationError("HZ_CLUSTER", "environment variables", "HZ_CLUSTER environment variable must be provided")
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		logConfigurationError("HZ_MEMBERS", "environment variables", "HZ_MEMBERS environment variable must be provided")
	}

	hzMemberList := strings.Split(hzMembers, ",")

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		api.Serve()
	}()

	rand.Seed(time.Now().UnixNano())

	go func() {
		defer wg.Done()
		mapTester := maps.MapTester{HzCluster: hzCluster, HzMembers: hzMemberList}
		mapTester.TestMaps()
	}()

	go func() {
		defer wg.Done()
		queueTester := queues.QueueTester{HzCluster: hzCluster, HzMembers: hzMemberList}
		queueTester.TestQueues()
	}()

	wg.Wait()

}

func logConfigurationError(configValue string, source string, msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.ConfigurationEvent,
		"value":  configValue,
		"source": source,
		"client": client.ID(),
	}).Fatal(msg)

}
