package pokedex

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/maps"

	log "github.com/sirupsen/logrus"
)

type PokedexRunner struct{}

type pokedex struct {
	Pokemon []pokemon `json:"pokemon"`
}

type pokemon struct {
	ID            int             `json:"id"`
	Num           string          `json:"num"`
	Name          string          `json:"name"`
	Img           string          `json:"img"`
	ElementType   []string        `json:"type"`
	Height        string          `json:"height"`
	Weight        string          `json:"weight"`
	Candy         string          `json:"candy"`
	CandyCount    int             `json:"candy_count"`
	EggDistance   string          `json:"egg"`
	SpawnChance   float32         `json:"spawn_chance"`
	AvgSpawns     float32         `json:"avg_spawns"`
	SpawnTime     string          `json:"spawn_time"`
	Multipliers   []float32       `json:"multipliers"`
	Weaknesses    []string        `json:"weaknesses"`
	NextEvolution []nextEvolution `json:"next_evolution"`
}

type nextEvolution struct {
	Num  string `json:"num"`
	Name string `json:"name"`
}

//go:embed pokedex.json
var pokedexFile embed.FS

const defaultEnabled = true
const defaultNumMaps = 10
const defaultAppendMapIndexToMapName = true
const defaultAppendClientIdToMapName = false
const defaultNumRuns = 10000
const defaultUseMapPrefix = true
const defaultMapPrefix = "ht_"

var enabled bool
var numMaps int
var appendMapIndexToMapName bool
var appendClientIdToMapName bool
var numRuns int
var useMapPrefix bool
var mapPrefix string

func init() {
	maps.Register(PokedexRunner{})
	gob.Register(pokemon{})
}

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	populateConfig()

	if !enabled {
		logInternalStateEvent("pokedexrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	pokedex, err := parsePokedexFile()

	clientID := client.ClientID()
	if err != nil {
		logIoEvent(fmt.Sprintf("unable to parse pokedex json file: %s", err))
	}

	ctx := context.TODO()

	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-pokedexrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s", err))
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("starting pokedex maps loop", log.InfoLevel)

	runnerConfig := maps.MapRunnerConfig{
		MapBaseName: "pokedex",
		UseMapPrefix: useMapPrefix,
		MapPrefix: mapPrefix,
		AppendMapIndexToMapName: appendMapIndexToMapName,
		AppendClientIdToMapName: appendClientIdToMapName,
	}

	testLoop := maps.TestLoop[pokemon]{
		HzClient:               hzClient,
		RunnerConfig:           &runnerConfig,		
		NumMaps:                numMaps,
		NumRuns:                numRuns,
		Elements:               &pokedex.Pokemon,
		Ctx:                    ctx,
		GetElementIdFunc:       getElementID,
		DeserializeElementFunc: deserializeElement,
	}

	testLoop.Run()

	logInternalStateEvent("finished pokedex maps loop", log.InfoLevel)

}

func getElementID(element interface{}) string {

	pokemon := element.(pokemon)
	return fmt.Sprintf("%d", pokemon.ID)

}

func deserializeElement(elementFromHZ interface{}) error {

	_, ok := elementFromHZ.(pokemon)
	if !ok {
		return errors.New("unable to serialize value retrieved from hazelcast map into pokemon instance")
	}

	return nil

}

func populateConfig() {

	parsedConfig := config.GetParsedConfig()

	// TODO All of the following is very ugly indeed -- simply parse Yaml into new struct type instead?
	keyPath := "maptests.pokedex.enabled"
	valueFromConfig, err := config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	keyPath = "maptests.pokedex.numMaps"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numMaps = defaultNumMaps
	} else {
		numMaps = valueFromConfig.(int)
	}

	keyPath = "maptests.pokedex.appendMapIndexToMapName"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendMapIndexToMapName = defaultAppendMapIndexToMapName
	} else {
		appendMapIndexToMapName = valueFromConfig.(bool)
	}

	keyPath = "maptests.pokedex.appendClientIdToMapName"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		appendClientIdToMapName = defaultAppendClientIdToMapName
	} else {
		appendClientIdToMapName = valueFromConfig.(bool)
	}

	keyPath = "maptests.pokedex.numRuns"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		numRuns = defaultNumRuns
	} else {
		numRuns = valueFromConfig.(int)
	}

	keyPath = "maptests.pokedex.mapPrefix.enabled"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		useMapPrefix = defaultUseMapPrefix
	} else {
		useMapPrefix = valueFromConfig.(bool)
	}

	keyPath = "maptests.pokedex.mapPrefix.prefix"
	valueFromConfig, err = config.ExtractConfigValue(parsedConfig, keyPath)
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		mapPrefix = defaultMapPrefix
	} else {
		mapPrefix = valueFromConfig.(string)
	}

}

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}

func parsePokedexFile() (*pokedex, error) {

	pokedexJson, err := pokedexFile.Open("pokedex.json")

	if err != nil {
		return nil, err
	}
	defer pokedexJson.Close()

	var pokedex pokedex
	err = json.NewDecoder(pokedexJson).Decode(&pokedex)

	if err != nil {
		return nil, err
	}

	logInternalStateEvent("parsed pokedex file", log.TraceLevel)

	return &pokedex, nil

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

func logIoEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}).Fatal(msg)

}

func logHzEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.HzError,
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
