package maps

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
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hazelcast/hazelcast-go-client"
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

var enabled bool
var numMaps int
var appendMapIndexToMapName bool
var appendClientIdToMapName bool
var numRuns int

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

	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := "pokedex"
			if appendMapIndexToMapName {
				mapName = fmt.Sprintf("%s-%d", mapName, i)
			}
			if appendClientIdToMapName {
				mapName = fmt.Sprintf("%s-%s", mapName, client.ClientID())
			}
			logInternalStateEvent(fmt.Sprintf("using map name '%s' in map goroutine %d", mapName, i), log.InfoLevel)
			start := time.Now()
			hzPokedexMap, err := hzClient.GetMap(ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			logTimingEvent("getMap()", int(elapsed))
			if err != nil {
				logHzEvent(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			defer hzPokedexMap.Destroy(ctx)
			doTestLoop(ctx, hzPokedexMap, pokedex, mapName, i)
		}(i)
	}
	wg.Wait()

	logInternalStateEvent("finished pokedex maps loop", log.InfoLevel)

}

func doTestLoop(ctx context.Context, m *hazelcast.Map, p *pokedex, mapName string, mapNumber int) {

	for i := 0; i < numRuns; i++ {
		if i > 0 && i % 100 == 0 {
			logInternalStateEvent(fmt.Sprintf("finished %d runs for map %s in map goroutine %d", i, mapName, mapNumber), log.InfoLevel)
		}
		logInternalStateEvent(fmt.Sprintf("in run %d on map %s in map goroutine %d", i, mapName, mapNumber), log.TraceLevel)
		err := ingestAll(ctx, m, p, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = readAll(ctx, m, p, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = deleteSome(ctx, m, p, mapNumber)
		if err != nil {
			logHzEvent(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
	}

}

func deleteSome(ctx context.Context, m *hazelcast.Map, p *pokedex, mapNumber int) error {

	numElementsToDelete := rand.Intn(len(p.Pokemon))

	for i := 0; i < numElementsToDelete; i++ {
		pokemonToDelete := p.Pokemon[i]
		containsKey, err := m.ContainsKey(ctx, assembleMapKey(pokemonToDelete.ID, mapNumber))
		if err != nil {
			return err
		}
		if !containsKey {
			continue
		}
		_, err = m.Remove(ctx, pokemonToDelete.ID)
		if err != nil {
			return err
		}
	}

	logInternalStateEvent(fmt.Sprintf("deleted %d elements from pokedex map", numElementsToDelete), log.TraceLevel)

	return nil

}

func readAll(ctx context.Context, m *hazelcast.Map, p *pokedex, mapNumber int) error {

	for _, v := range p.Pokemon {
		valueFromHZ, err := m.Get(ctx, assembleMapKey(v.ID, mapNumber))
		if err != nil {
			return err
		}
		_, ok := valueFromHZ.(pokemon)
		if !ok {
			return errors.New("unable to serialize value retrieved from hazelcast map into pokemon instance")
		}
	}

	logInternalStateEvent(fmt.Sprintf("retrieved %d items from hazelcast map", len(p.Pokemon)), log.TraceLevel)

	return nil

}

func ingestAll(ctx context.Context, m *hazelcast.Map, p *pokedex, mapNumber int) error {

	numNewlyIngested := 0
	for _, v := range p.Pokemon {
		key := assembleMapKey(v.ID, mapNumber)
		containsKey, err := m.ContainsKey(ctx, key)
		if err != nil {
			return err
		}
		if containsKey {
			continue
		}
		if err = m.Set(ctx, key, v); err != nil {
			return err
		}
		numNewlyIngested++
	}

	logInternalStateEvent(fmt.Sprintf("(re-)stored %d items in hazelcast map", numNewlyIngested), log.TraceLevel)

	return nil

}

func assembleMapKey(id int, mapNumber int) string {

	return fmt.Sprintf("%s-%d", client.ClientID(), id)

}

func populateConfig() {

	mapTestsConfig, ok := config.RetrieveConfig("maptests").(map[string]interface{})

	if !ok {
		logConfigEvent("maptests", "config file", "unable to read 'maptests' object into map for further processing")
	}

	pokedexConfig, ok := mapTestsConfig["pokedex"].(map[string]interface{})
	if !ok {
		logConfigEvent("maptests.pokedex", "config file", "unable to read 'maptests.pokedex' object into map for further processing")
	}

	logInternalStateEvent(fmt.Sprintf("using config: %v", pokedexConfig), log.InfoLevel)

	enabled, ok = pokedexConfig["enabled"].(bool)
	if !ok {
		logConfigEvent("maptests.pokedex.enabled", "config file", "unable to parse 'maptests.pokedex.enabled' into bool")
	}

	numMaps, ok = pokedexConfig["numMaps"].(int)
	if !ok {
		logConfigEvent("maptests.pokedex.numMaps", "config file", "unable to parse 'maptests.pokedex.numMaps' into int")
	}

	appendMapIndexToMapName, ok = pokedexConfig["appendMapIndexToMapName"].(bool)
	if !ok {
		logConfigEvent("maptests.pokedex.appendMapIndexToMapName", "config file", "unable to parse 'maptests.pokedex.appendMapIndexToMapName' into bool")
	}

	appendClientIdToMapName, ok = pokedexConfig["appendClientIdToMapName"].(bool)
	if !ok {
		logConfigEvent("maptests.pokedex.appendClientIdToMapName", "config file", "unable to parse 'maptests.pokedex.appendClientIdToMapName' into bool")
	}

	numRuns, ok = pokedexConfig["numRuns"].(int)
	if !ok {
		logConfigEvent("maptests.pokedex.numRuns", "config file", "unable to parse 'maptests.pokedex.numRuns' into int")
	}

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

func logConfigEvent(configValue string, source string, msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.ConfigurationError,
		"value": configValue,
		"source": source,
		"client": client.ClientID(),
	}).Fatal(msg)

}

func logIoEvent(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}).Fatal(msg)

}

func logTimingEvent(operation string, tookMs int) {

	log.WithFields(log.Fields{
		"kind": logging.TimingInfo,
		"client": client.ClientID(),
		"tookMs": tookMs ,
	}).Infof("'%s' took %d ms", operation, tookMs)

}

func logInternalStateEvent(msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind": logging.InternalStateInfo,
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
		"kind": logging.HzError,
		"client": client.ClientID(),
	}).Warn(msg)

}
