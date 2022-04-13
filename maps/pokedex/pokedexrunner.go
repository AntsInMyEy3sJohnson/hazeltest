package maps

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hazeltest/client"
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
const internalStateInfo = "internal state info"
const ioError = "io error"
const hzError = "hazelcast error"
const timingInfo = "timing info"

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
		log.WithFields(log.Fields{
			"kind":   ioError,
			"client": client.ClientID(),
		}).Tracef("unable to parse pokedex json file: %s", err)
	}

	ctx := context.TODO()

	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-pokedexrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		log.WithFields(log.Fields{
			"kind":   hzError,
			"client": client.ClientID(),
		}).Fatalf("unable to initialize hazelcast client: %s", err)
	}
	defer hzClient.Shutdown(ctx)

	log.WithFields(log.Fields{
		"kind":   internalStateInfo,
		"client": client.ClientID(),
	}).Trace("initializing hazelcast client")

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Info("starting pokedex maps loop")
	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := fmt.Sprintf("%s-pokedex-%d", client.ClientID(), i)
			start := time.Now()
			hzPokedexMap, err := hzClient.GetMap(ctx, mapName)
			elapsed := time.Since(start).Milliseconds()
			log.WithFields(log.Fields{
				"kind": timingInfo,
				"client": client.ClientID(),
				"tookMs": elapsed ,
			}).Infof("getMap() took %d ms", elapsed)
			if err != nil {
				log.WithFields(log.Fields{
					"kind":   hzError,
					"client": client.ClientID(),
				}).Fatalf("unable to retrieve map '%s' from hazelcast: %s", mapName, err)
			}
			defer hzPokedexMap.Destroy(ctx)
			doTestLoop(ctx, hzPokedexMap, pokedex, mapName)
		}(i)
	}
	wg.Wait()

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Info("finished pokedex maps loop")

}

func doTestLoop(ctx context.Context, m *hazelcast.Map, p *pokedex, mapName string) {

	for i := 0; i < runs; i++ {
		if i % 100 == 0 {
			log.WithFields(log.Fields{
				"kind": internalStateInfo,
				"client": client.ClientID(),
			}).Infof("finished %d runs for map %s", i, mapName)
		}
		log.WithFields(log.Fields{
			"kind": internalStateInfo,
			"client": client.ClientID(),
		}).Tracef("in run %d on map %s", i, mapName)
		err := ingestAll(ctx, m, p)
		if err != nil {
			log.WithFields(log.Fields{
				"kind": hzError,
				"client": client.ClientID(),
			}).Warnf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err)
			continue
		}

		err = readAll(ctx, m, p)
		if err != nil {
			log.WithFields(log.Fields{
				"kind": hzError,
				"client": client.ClientID(),
			}).Warnf("failed to read data from map '%s' in run %d: %s", mapName, i, err)
			continue
		}

		err = deleteSome(ctx, m, p)
		if err != nil {
			log.WithFields(log.Fields{
				"kind": hzError,
				"client": client.ClientID(),
			}).Warnf("failed to delete data from map '%s' in run %d: %s", mapName, i, err)
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

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Tracef("deleted %d elements from pokedex map", numElementsToDelete)

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

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Tracef("retrieved %d items from hazelcast map", len(p.Pokemon))

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

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Tracef("(re-)stored %d items in hazelcast map", numNewlyIngested)

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

	log.WithFields(log.Fields{
		"kind": internalStateInfo,
		"client": client.ClientID(),
	}).Trace("parsed pokedex file")

	return &pokedex, nil

}
