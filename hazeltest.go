package main

import (
	"hazeltest/api"
	"hazeltest/client"
	clientConfig "hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/maps"
	_ "hazeltest/maps/load"
	_ "hazeltest/maps/pokedex"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func main() {

	api.Serve()

	clientConfig.ParseCommandLineArgs()

	fileParser := clientConfig.FileParser{ClientID: client.ClientID()}
	fileParser.ParseConfigFile()

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		logConfigurationError("HZ_CLUSTER", "environment variables", "HZ_CLUSTER environment variable must be provided")
	}
	hzMembers := os.Getenv("HZ_MEMBERS")

	if hzMembers == "" {
		logConfigurationError("HZ_MEMBERS", "environment variables", "HZ_MEMBERS environment variable must be provided")
	}

	// TODO Should only be set once all runners have successfully connected to Hazelcast
	api.Ready()

	mapTester := maps.MapTester{HzCluster: hzCluster, HzMembers: strings.Split(hzMembers, ",")}
	mapTester.TestMaps()

}

func logConfigurationError(configValue string, source string, msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}).Fatal(msg)

}
