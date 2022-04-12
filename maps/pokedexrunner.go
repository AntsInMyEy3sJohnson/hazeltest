package maps

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hazeltest/client"
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
const numMaps = 100

//go:embed pokedex.json
var pokedexFile embed.FS

var (
	trace *log.Logger
	info  *log.Logger
	warn  *log.Logger
	err   *log.Logger
)

func init() {
	Register(PokedexRunner{})
	gob.Register(pokemon{})

	flags := log.Ldate|log.Ltime|log.Lshortfile
	trace = log.New(os.Stdout, "TRACE: ", flags)
	info = log.New(os.Stdout, "INFO: ", flags)
	warn = log.New(os.Stderr, "WARN: ", flags)
	err = log.New(os.Stderr, "ERROR: ", flags)
}

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	pokedex, err := parsePokedexFile()

	if err != nil {
		panic(err)
	}

	ctx := context.TODO()

	clientID := client.ClientID()
	trace.Printf("pokedexrunner initializing hazelcast client using client id %s", clientID)
	hzClient, err := client.InitHazelcastClient(ctx, fmt.Sprintf("%s-pokedexrunner", clientID), hzCluster, hzMembers)

	if err != nil {
		panic(err)
	}
	defer hzClient.Shutdown(ctx)

	trace.Printf("starting maps loop in pokedexrunner with client id %s", clientID)
	var wg sync.WaitGroup
	for i := 0; i < numMaps; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapName := fmt.Sprintf("pokedex-%d", i)
			hzPokedexMap, err := hzClient.GetMap(ctx, mapName)
			if err != nil {
				panic(err)
			}
			doTestLoop(ctx, hzPokedexMap, pokedex, mapName)
		}(i)
	}
	wg.Wait()

}

func doTestLoop(ctx context.Context, m *hazelcast.Map, p *pokedex, mapName string) {

	for i := 0; i < runs; i++ {
		trace.Printf("%s: in run %d on map %s", clientID, i, mapName)
		err := ingestAll(ctx, m, p)
		if err != nil {
			warn.Printf("%s: failed to ingest data into map '%s' in run %d: %s", clientID, mapName, i, err)
			continue
		}

		err = readAll(ctx, m, p)
		if err != nil {
			warn.Printf("%s: failed to read data from map '%s' in run %d: %s", clientID, mapName, i, err)
			continue
		}

		err = deleteSome(ctx, m, p)
		if err != nil {
			warn.Printf("%s: failed to delete data from map '%s' in run %d: %s", clientID, mapName, i, err)
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

	trace.Printf("deleted %d elements from pokedex map\n", numElementsToDelete)

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

	fmt.Printf("retrieved %d items from hazelcast map\n", len(p.Pokemon))

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

	fmt.Printf("stored %d items in hazelcast map\n", len(p.Pokemon))

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

	return &pokedex, nil

}
