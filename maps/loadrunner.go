package maps

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"hazeltest/client"
	"hazeltest/client/config"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type loadRunner struct{}

type loadElement struct {
	Key     string
	Payload *string
}

// Copied from: https://stackoverflow.com/a/31832326
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

const (
	defaultNumEntriesPerMap = 10000
	defaultPayloadSizeBytes = 1000
)

var (
	numEntriesPerMap int
	payloadSizeBytes int
)

func init() {
	register(loadRunner{})
	gob.Register(loadElement{})
}

func (r loadRunner) runMapTests(hzCluster string, hzMembers []string) {

	mapRunnerConfig := populateLoadConfig()

	if !mapRunnerConfig.enabled {
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

	testLoop := testLoop[loadElement]{
		id:                     uuid.New(),
		source:                 "load",
		hzClient:               hzClient,
		config:                 mapRunnerConfig,
		elements:               elements,
		ctx:                    ctx,
		getElementIdFunc:       getLoadElementID,
		deserializeElementFunc: deserializeLoadElement,
	}

	testLoop.run()

	logInternalStateEvent("finished load test loop", log.InfoLevel)

}

func populateLoadElements() []loadElement {

	elements := make([]loadElement, numEntriesPerMap)
	// Depending on the value of 'payloadSizeBytes', this string can get very large, and to generate one
	// unique string for each map entry will result in high memory consumption of this Hazeltest client.
	// Thus, we use one random string for each map and point to that string in each load element
	randomPayload := generateRandomPayload(payloadSizeBytes)

	for i := 0; i < numEntriesPerMap; i++ {
		elements[i] = loadElement{
			Key:     strconv.Itoa(i),
			Payload: &randomPayload,
		}
	}

	return elements

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

func getLoadElementID(element interface{}) string {

	loadElement := element.(loadElement)
	return loadElement.Key

}

func deserializeLoadElement(elementFromHz interface{}) error {

	_, ok := elementFromHz.(loadElement)

	if !ok {
		return errors.New("unable to serialize value retrieved from hazelcast map into loadelement instance")
	}

	return nil

}

func populateLoadConfig() *runnerConfig {

	parsedConfig := config.GetParsedConfig()
	runnerKeyPath := "maptests.load"

	keyPath := runnerKeyPath + ".numEntriesPerMap"
	valueFromConfig, err := config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numEntriesPerMap = defaultNumEntriesPerMap
	} else {
		numEntriesPerMap = valueFromConfig.(int)
	}

	keyPath = runnerKeyPath + ".payloadSizeBytes"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		payloadSizeBytes = defaultPayloadSizeBytes
	} else {
		payloadSizeBytes = valueFromConfig.(int)
	}

	configBuilder := runnerConfigBuilder{
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   "load",
		parsedConfig:  parsedConfig,
	}
	return configBuilder.populateConfig()

}
