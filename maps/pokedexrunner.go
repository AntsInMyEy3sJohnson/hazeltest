package maps

import (
	"context"
	"embed"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
)

type (
	pokedexRunner struct{}
	pokedex       struct {
		Pokemon []pokemon `json:"pokemon"`
	}
	pokemon struct {
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
	nextEvolution struct {
		Num  string `json:"num"`
		Name string `json:"name"`
	}
)

//go:embed pokedex.json
var pokedexFile embed.FS

func init() {
	register(pokedexRunner{})
	gob.Register(pokemon{})
}

func (r pokedexRunner) runMapTests(hzCluster string, hzMembers []string) {

	mapRunnerConfig := populatePokedexConfig()

	if !mapRunnerConfig.enabled {
		lp.LogInternalStateEvent("pokedexrunner not enabled -- won't run", log.InfoLevel)
		return
	}

	api.RaiseNotReady()

	pokedex, err := parsePokedexFile()

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse pokedex json file: %s", err), log.FatalLevel)
	}

	ctx := context.TODO()

	hzClient := client.NewHzClient().InitHazelcastClient(ctx, "pokedexrunner", hzCluster, hzMembers)
	defer hzClient.Shutdown(ctx)

	api.RaiseReady()

	lp.LogInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogInternalStateEvent("starting pokedex maps loop", log.InfoLevel)

	testLoop := testLoop[pokemon]{
		id:                     uuid.New(),
		source:                 "pokedexrunner",
		hzClient:               hzClient,
		config:                 mapRunnerConfig,
		elements:               pokedex.Pokemon,
		ctx:                    ctx,
		getElementIdFunc:       getPokemonID,
		deserializeElementFunc: deserializePokemon,
	}

	testLoop.run()

	lp.LogInternalStateEvent("finished pokedex maps loop", log.InfoLevel)

}

func getPokemonID(element interface{}) string {

	pokemon := element.(pokemon)
	return fmt.Sprintf("%d", pokemon.ID)

}

func deserializePokemon(elementFromHZ interface{}) error {

	_, ok := elementFromHZ.(pokemon)
	if !ok {
		return errors.New("unable to serialize value retrieved from hazelcast map into pokemon instance")
	}

	return nil

}

func populatePokedexConfig() *runnerConfig {

	runnerKeyPath := "maptests.pokedex"

	configBuilder := runnerConfigBuilder{
		runnerKeyPath: runnerKeyPath,
		mapBaseName:   "pokedex",
	}
	return configBuilder.populateConfig()

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

	lp.LogInternalStateEvent("parsed pokedex file", log.TraceLevel)

	return &pokedex, nil

}
