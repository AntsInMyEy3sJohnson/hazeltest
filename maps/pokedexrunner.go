package maps

import (
	"context"
	"embed"
	"encoding/json"
	"encoding/gob"
	"errors"
	"fmt"
	"hazeltest/client"

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

func init() {
	Register(PokedexRunner{})
	gob.Register(pokemon{})
}

//go:embed pokedex.json
var pokedexFile embed.FS

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	pokedex, err := parsePokedexFile()

	if err != nil {
		panic(err)
	}

	ctx := context.TODO()

	hzClient, err := client.InitHazelcastClient(ctx, hzCluster, hzMembers)

	if err != nil {
		panic(err)
	}
	defer hzClient.Shutdown(ctx)

	hzPokedexMap, err := hzClient.GetMap(ctx, "pokedex")

	if err != nil {
		panic(err)
	}

	err = ingestAll(ctx, hzPokedexMap, pokedex)
	if err != nil {
		panic(err)
	}

	err = readAll(ctx, hzPokedexMap, pokedex)
	if err != nil {
		panic(err)
	}

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
