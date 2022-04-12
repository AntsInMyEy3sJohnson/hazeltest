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
	"log"
	"math/rand"
	"os"
	"sync"

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
const numMaps = 1

//go:embed pokedex.json
var pokedexFile embed.FS

var (
	traceLogger   *log.Logger
	infoLogger    *log.Logger
	warnLogger *log.Logger
	errLogger   *log.Logger
)

func init() {
	maps.Register(PokedexRunner{})
	gob.Register(pokemon{})

	flags := log.Ldate | log.Ltime | log.Lshortfile

	traceLogger = log.New(os.Stdout, "TRACE: ", flags)
	infoLogger = log.New(os.Stdout, "INFO: ", flags)
	warnLogger = log.New(os.Stderr, "WARN: ", flags)
	errLogger = log.New(os.Stderr, "ERROR: ", flags)

	logTrace("pokedexrunner initialization complete")
}

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	pokedex, err := parsePokedexFile()

	clientID := client.ClientID()
	if err != nil {
		logErr(fmt.Sprintf("unable to parse pokedex json file: %s", err))
	}

	ctx := context.TODO()

	logTrace("initializing hazelcast client")

	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-pokedexrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		logErr(fmt.Sprintf("unable to initialize hazelcast client: %s", err))
	}
	defer hzClient.Shutdown(ctx)

	logTrace("starting maps loop")
	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := fmt.Sprintf("pokedex-%d", i)
			hzPokedexMap, err := hzClient.GetMap(ctx, mapName)
			if err != nil {
				logErr(fmt.Sprintf("unable to retrieve map '%s' from hazelcast: %s", mapName, err))
			}
			doTestLoop(ctx, hzPokedexMap, pokedex, mapName)
		}(i)
	}
	wg.Wait()

}

func doTestLoop(ctx context.Context, m *hazelcast.Map, p *pokedex, mapName string) {

	for i := 0; i < runs; i++ {
		logTrace(fmt.Sprintf("in run %d on map %s", i, mapName))
		err := ingestAll(ctx, m, p)
		if err != nil {
			logWarn(fmt.Sprintf("failed to ingest data into map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = readAll(ctx, m, p)
		if err != nil {
			logWarn(fmt.Sprintf("failed to read data from map '%s' in run %d: %s", mapName, i, err))
			continue
		}

		err = deleteSome(ctx, m, p)
		if err != nil {
			logWarn(fmt.Sprintf("failed to delete data from map '%s' in run %d: %s", mapName, i, err))
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

	logInfo(fmt.Sprintf("deleted %d elements from pokedex map", numElementsToDelete))

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

	logInfo(fmt.Sprintf("retrieved %d items from hazelcast map", len(p.Pokemon)))

	return nil

}

func ingestAll(ctx context.Context, m *hazelcast.Map, p *pokedex) error {

	for _, v := range p.Pokemon {
		containsKey, err := m.ContainsKey(ctx, v.ID)
		if err != nil {
			return err
		}
		if !containsKey {
			m.Set(ctx, v.ID, v)
		}
	}

	logInfo(fmt.Sprintf("stored %d items in hazelcast map", len(p.Pokemon)))

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

	logTrace("parsed pokedex file")

	return &pokedex, nil

}

func logTrace(msg string) {

	traceLogger.Printf("%s: %s", client.ClientID(), msg)

}

func logInfo(msg string) {

	infoLogger.Printf("%s: %s", client.ClientID(), msg)

}

func logWarn(msg string) {

	warnLogger.Printf("%s: %s", client.ClientID(), msg)

}

func logErr(msg string) {

	errLogger.Fatalf("%s: %s", client.ClientID(), msg)

}
