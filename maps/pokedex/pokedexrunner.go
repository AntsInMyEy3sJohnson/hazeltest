package maps

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hazeltest/client"
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

const runs = 10000
const numMaps = 4

//go:embed pokedex.json
var pokedexFile embed.FS

func init() {
	maps.Register(PokedexRunner{})
	gob.Register(pokemon{})
}

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	pokedex, err := parsePokedexFile()

	clientID := client.ClientID()
	if err != nil {
		logIoError(fmt.Sprintf("unable to parse pokedex json file: %s", err))
	}

	ctx := context.TODO()

	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-pokedexrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		logHzError(fmt.Sprintf("unable to initialize hazelcast client: %s", err))
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateInfo("initialized hazelcast client", log.InfoLevel)
	logInternalStateInfo("starting pokedex maps loop", log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := fmt.Sprintf("%s-pokedex-%d", client.ClientID(), i)
			start := time.Now()
			hzPokedexMap, err := hzClient.GetMap(ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			logTimingInfo("getMap()", int(elapsed))
			if err != nil {
				logHzError(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			defer hzPokedexMap.Destroy(ctx)
			doTestLoop(ctx, hzPokedexMap, pokedex, mapName)
		}(i)
	}
	wg.Wait()

	logInternalStateInfo("finished pokedex maps loop", log.InfoLevel)

}

func doTestLoop(ctx context.Context, m *hazelcast.Map, p *pokedex, mapName string) {

	for i := 0; i < runs; i++ {
		if i > 0 && i % 100 == 0 {
			logInternalStateInfo(fmt.Sprintf("finished %d runs for map %s", i, mapName), log.InfoLevel)
		}
		logInternalStateInfo(fmt.Sprintf("in run %d on map %s", i, mapName), log.TraceLevel)
		err := ingestAll(ctx, m, p)
		if err != nil {
			logHzError(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = readAll(ctx, m, p)
		if err != nil {
			logHzError(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = deleteSome(ctx, m, p)
		if err != nil {
			logHzError(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}
	}

}

func deleteSome(ctx context.Context, m *hazelcast.Map, p *pokedex) error {

	numElementsToDelete := rand.Intn(len(p.Pokemon))

	for i := 0; i < numElementsToDelete; i++ {
		pokemonToDelete := p.Pokemon[i]
		containsKey, err := m.ContainsKey(ctx, pokemonToDelete.ID)
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

	logInternalStateInfo(fmt.Sprintf("deleted %d elements from pokedex map", numElementsToDelete), log.TraceLevel)

	return nil

}

func readAll(ctx context.Context, m *hazelcast.Map, p *pokedex) error {

	for _, v := range p.Pokemon {
		valueFromHZ, err := m.Get(ctx, v.ID)
		if err != nil {
			return err
		}
		_, ok := valueFromHZ.(pokemon)
		if !ok {
			return errors.New("unable to serialize value retrieved from hazelcast map into pokemon instance")
		}
	}

	logInternalStateInfo(fmt.Sprintf("retrieved %d items from hazelcast map", len(p.Pokemon)), log.TraceLevel)

	return nil

}

func ingestAll(ctx context.Context, m *hazelcast.Map, p *pokedex) error {

	numNewlyIngested := 0
	for _, v := range p.Pokemon {
		containsKey, err := m.ContainsKey(ctx, v.ID)
		if err != nil {
			return err
		}
		if !containsKey {
			m.Set(ctx, v.ID, v)
			numNewlyIngested++
		}
	}

	logInternalStateInfo(fmt.Sprintf("(re-)stored %d items in hazelcast map", numNewlyIngested), log.TraceLevel)

	return nil

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

	logInternalStateInfo("parsed pokedex file", log.TraceLevel)

	return &pokedex, nil

}

func logIoError(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}).Trace(msg)

}

func logTimingInfo(operation string, tookMs int) {

	log.WithFields(log.Fields{
		"kind": logging.TimingInfo,
		"client": client.ClientID(),
		"tookMs": tookMs ,
	}).Infof("'%s' took %d ms", operation, tookMs)

}

func logInternalStateInfo(msg string, logLevel log.Level) {

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

func logHzError(msg string) {

	log.WithFields(log.Fields{
		"kind": logging.HzError,
		"client": client.ClientID(),
	}).Warn(msg)

}
