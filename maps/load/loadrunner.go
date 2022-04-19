package load

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/maps"
	"math/rand"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type LoadRunner struct{}

type loadElement struct {
	Key     string
	Payload string
}

// Copied from: https://stackoverflow.com/a/31832326
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

const (
	defaultEnabled                 = true
	defaultNumMaps                 = 10
	defaultNumEntriesPerMap        = 10000
	defaultPayloadSizeBytes        = 1000
	defaultAppendMapIndexToMapName = true
	defaultAppendClientIdToMapName = false
	defaultNumRuns                 = 10000
	defaultUseMapPrefix            = true
	defaultMapPrefix               = "ht_"
)

var (
	enabled                 bool
	numMaps                 int
	numEntriesPerMap        int
	payloadSizeBytes        int
	appendMapIndexToMapName bool
	appendClientIdToMapName bool
	numRuns                 int
	useMapPrefix            bool
	mapPrefix               string
)

func init() {
	maps.Register(LoadRunner{})
	gob.Register(loadElement{})
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

	elements := populateLoadElements()

	// TODO This will be pretty much the same for every map runner... why not build a config mechanism that parses the given yaml into this structure?
	runnerConfig := maps.MapRunnerConfig{
		MapBaseName:             "load",
		UseMapPrefix:            useMapPrefix,
		MapPrefix:               mapPrefix,
		AppendMapIndexToMapName: appendMapIndexToMapName,
		AppendClientIdToMapName: appendClientIdToMapName,
	}

	testLoop := maps.TestLoop[loadElement]{
		HzClient:               hzClient,
		RunnerConfig:           &runnerConfig,
		NumMaps:                numMaps,
		NumRuns:                numRuns,
		Elements:               elements,
		Ctx:                    ctx,
		GetElementIdFunc:       getElementID,
		DeserializeElementFunc: deserializeElementFunc,
	}

	testLoop.Run()

	logInternalStateEvent("finished load test loop", log.InfoLevel)

}

func populateLoadElements() *[]loadElement {

	elements := make([]loadElement, numEntriesPerMap)

	for i := 0; i < numEntriesPerMap; i++ {
		elements[i] = loadElement{
			Key:     strconv.Itoa(i),
			Payload: generateRandomPayload(payloadSizeBytes),
		}
	}

	return &elements

}

// Copied from: https://stackoverflow.com/a/31832326
// StackOverflow is such a fascinating place.
func generateRandomPayload(n int) string {

	src := rand.NewSource(time.Now().UnixNano())

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)

}

func getElementID(element interface{}) string {

	loadElement := element.(loadElement)
	return loadElement.Key

}

func deserializeElementFunc(elementFromHz interface{}) error {

	_, ok := elementFromHz.(loadElement)

	if !ok {
		return errors.New("unable to serialize value retrieved from hazelcast map into loadelement instance")
	}

	return nil

}

func populateConfig() {

	parsedConfig := config.GetParsedConfig()

	keyPath := "maptests.load.enabled"
	valueFromConfig, err := config.ExtractConfigValue(parsedConfig, keyPath)

	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = "maptests.load.numMaps"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numMaps = defaultNumMaps
	} else {
		numMaps = valueFromConfig.(int)
	}

	keyPath = "maptests.load.numEntriesPerMap"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numEntriesPerMap = defaultNumEntriesPerMap
	} else {
		numEntriesPerMap = valueFromConfig.(int)
	}

	keyPath = "maptests.load.payloadSizeBytes"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		payloadSizeBytes = defaultPayloadSizeBytes
	} else {
		payloadSizeBytes = valueFromConfig.(int)
	}

	keyPath = "maptests.load.appendMapIndexToMapName"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendMapIndexToMapName = defaultAppendMapIndexToMapName
	} else {
		appendMapIndexToMapName = valueFromConfig.(bool)
	}

	keyPath = "maptests.load.appendClientIdToMapName"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendClientIdToMapName = defaultAppendClientIdToMapName
	} else {
		appendClientIdToMapName = valueFromConfig.(bool)
	}

	keyPath = "maptests.load.numRuns"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = "maptests.load.mapPrefix.enabled"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		useMapPrefix = defaultUseMapPrefix
	} else {
		useMapPrefix = valueFromConfig.(bool)
	}

	keyPath = "maptests.load.mapPrefix.prefix"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		mapPrefix = defaultMapPrefix
	} else {
		mapPrefix = valueFromConfig.(string)
	}

}

func logConfigEvent(configValue string, source string, msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}
	if logLevel == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

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

func logHzEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}).Fatal(msg)

}

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}
