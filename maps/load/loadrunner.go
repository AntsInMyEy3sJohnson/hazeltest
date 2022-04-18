package load

import (
	"context"
	"fmt"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/maps"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type LoadRunner struct {}

var enabled bool
// TODO Read from config
const numMaps = 100
const appendMapIndexToMapName = true
const appendClientIdToMapName = true
const numRuns = 10
const useMapPrefix = true
const mapPrefix = "ht_"

func init() {
	maps.Register(LoadRunner{})
}

func (r LoadRunner) Run(hzCluster string, hzMembers []string) {

	populateConfig()

	if !enabled {
		logInternalStateEvent("loadrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	ctx := context.TODO()

	clientID := client.ClientID()
	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-loadrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s", err))
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("starting load test loop", log.InfoLevel)

	// TODO Lots of functionality very similar to pokedexrunner -- refactor
	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := assembleMapName(i)
			logInternalStateEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			hzLoadMap, err := hzClient.GetMap(ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			logTimingEvent("getMap()", int(elapsed))
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			defer hzLoadMap.Destroy(ctx)
		}(i)

	}
	wg.Wait()

}

// TODO Logic same as pokedexrunner -- refactor
func assembleMapName(mapIndex int) string {

	mapName := "load"
	if useMapPrefix && mapPrefix != ""{
		mapName = fmt.Sprintf("%s%s", mapPrefix, mapName)
	}
	if appendMapIndexToMapName {
		mapName = fmt.Sprintf("%s-%d", mapName, mapIndex)
	}
	if appendClientIdToMapName {
		mapName = fmt.Sprintf("%s-%s", mapName, client.ClientID())
	}

	return mapName

}

func populateConfig() {

	mapTestsConfig, err := config.RetrieveMapTestConfig()

	if err != nil {
		logConfigEvent("maptests", "config file", fmt.Sprintf("cannot populate config: %s", err))
	}

	loadConfig, ok := mapTestsConfig["load"].(map[string]interface{})
	if !ok {
		logConfigEvent("maptests.load", "config file", "unable to read 'maptests.load' object into map for further processing")
	}

	enabled, ok = loadConfig["enabled"].(bool)
	if !ok {
		logConfigEvent("maptests.load.enabled", "config file", "unable to parse 'maptests.load.enabled' into bool")
	}

}

func logConfigEvent(configValue string, source string, msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}).Fatal(msg)

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

func logTimingEvent(operation string, tookMs int) {

	log.WithFields(log.Fields{
		"kind":   logging.TimingInfo,
		"client": client.ClientID(),
		"tookMs": tookMs,
	}).Infof("'%s' took %d ms", operation, tookMs)

}

func logHzEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}).Fatal(msg)

}