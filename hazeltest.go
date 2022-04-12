package main

import (
	"hazeltest/maps"
	_ "hazeltest/maps/pokedex"
	"io"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})

	definedLogLevel := os.Getenv("LOG_LEVEL")

	var logLevel log.Level
	var out io.Writer

	switch strings.ToLower(definedLogLevel) {
	case "trace":
		logLevel = log.TraceLevel
		out = os.Stdout
	case "debug":
		logLevel = log.DebugLevel
		out = os.Stdout
	case "info":
		logLevel = log.InfoLevel
		out = os.Stdout
	case "warn":
		logLevel = log.WarnLevel
		out = os.Stderr
	case "error":
		logLevel = log.ErrorLevel
		out = os.Stderr
	default:
		logLevel = log.InfoLevel
		out = os.Stdout
	}

	log.SetLevel(logLevel)
	log.SetOutput(out)
	log.SetReportCaller(true)

}

func main() {

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		log.WithFields(log.Fields{
			"kind": "invalid or incomplete configuration",
		}).Fatal("HZ_CLUSTER environment variable must be provided")
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		log.WithFields(log.Fields{
			"kind": "invalid or incomplete configuration",
		}).Fatal("HZ_MEMBERS environment variable must be provided")
	}

	mapTester := maps.MapTester{HzCluster: hzCluster, HzMembers: strings.Split(hzMembers, ",")}
	mapTester.TestMaps()

}
