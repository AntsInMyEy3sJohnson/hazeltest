package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/chaos"
	"hazeltest/client"
	"hazeltest/logging"
	"hazeltest/maps"
	"hazeltest/queues"
	"hazeltest/state"
	"os"
	"strings"
	"sync"
)

func main() {

	lp := &logging.LogProvider{ClientID: client.ID()}

	if err := client.ParseConfigs(); err != nil {
		// Logging with fatal level will cause the application to exit.
		lp.LogConfigEvent("N/A", "config file", fmt.Sprintf("encountered error upon attempt to parse client configs: %v", err), log.FatalLevel)
	}

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		lp.LogConfigEvent("HZ_CLUSTER", "environment variables", "HZ_CLUSTER environment variable must be provided", log.FatalLevel)
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		lp.LogConfigEvent("HZ_MEMBERS", "environment variables", "HZ_MEMBERS environment variable must be provided", log.FatalLevel)
	}

	hzMemberList := strings.Split(hzMembers, ",")

	// Cleaners have to be run synchronously to make sure state has been evicted from
	// target Hazelcast cluster prior to start of load tests
	if err := state.RunCleaners(hzCluster, hzMemberList); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean state in target Hazelcast cluster: %v", err), log.FatalLevel)
	}

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		api.Serve()
	}()

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

	go func() {
		defer wg.Done()
		chaos.RunMonkeys()
	}()

	wg.Wait()

}
