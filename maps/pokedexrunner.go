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
	"hazeltest/client"
	"hazeltest/client/config"
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

func init() {
	Register(PokedexRunner{})
	gob.Register(pokemon{})
}

func (r PokedexRunner) RunMapTests(hzCluster string, hzMembers []string) {

	mapRunnerConfig := populatePokedexConfig()

	if !mapRunnerConfig.Enabled {
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

	// TODO This would be a nice spot for something like 'api.RaiseReadiness()'... decrement wait group for every runner that raises readiness, once the counter hits zero, readiness probes should succeed

	if err != nil {
		logHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s", err))
	}
	defer hzClient.Shutdown(ctx)

	logInternalStateEvent("initialized hazelcast client", log.InfoLevel)
	logInternalStateEvent("starting pokedex maps loop", log.InfoLevel)

	testLoop := TestLoop[pokemon]{
		ID:                     uuid.New(),
		Source:                 "pokedexrunner",
		HzClient:               hzClient,
		Config:                 mapRunnerConfig,
		Elements:               pokedex.Pokemon,
		Ctx:                    ctx,
		GetElementIdFunc:       getPokemonID,
		DeserializeElementFunc: deserializeElement,
	}

	testLoop.Run()

	logInternalStateEvent("finished pokedex maps loop", log.InfoLevel)

}

func getPokemonID(element interface{}) string {

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

func populatePokedexConfig() *RunnerConfig {

	parsedConfig := config.GetParsedConfig()
	runnerKeyPath := "maptests.pokedex"

	configBuilder := RunnerConfigBuilder{
		RunnerKeyPath: runnerKeyPath,
		MapBaseName:   "pokedex",
		ParsedConfig:  parsedConfig,
	}
	return configBuilder.PopulateConfig()

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
