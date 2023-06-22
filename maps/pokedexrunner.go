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
	"hazeltest/status"
)

type (
	pokedexRunner struct {
		assigner  client.ConfigPropertyAssigner
		stateList []state
		name      string
		source    string
		mapStore  hzMapStore
		l         looper[pokemon]
	}
	pokedex struct {
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

var (
	//go:embed pokedex.json
	pokedexFile embed.FS
)

func init() {
	register(&pokedexRunner{
		assigner:  &client.DefaultConfigPropertyAssigner{},
		stateList: []state{},
		name:      "mapsPokedexRunner",
		source:    "pokedexRunner",
		mapStore:  &defaultHzMapStore{},
	})
	gob.Register(pokemon{})
}

func initializeTestLoop(rc *runnerConfig) (looper[pokemon], error) {

	switch rc.loopType {
	case batch:
		return &batchTestLoop[pokemon]{}, nil
	case boundary:
		return &boundaryTestLoop[pokemon]{}, nil
	default:
		return nil, fmt.Errorf("no such runner runnerLoopType: %s", rc.loopType)
	}

}

func (r *pokedexRunner) runMapTests(hzCluster string, hzMembers []string) {

	r.appendState(start)

	config, err := populatePokedexConfig(r.assigner)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of map pokedex runner: unable to populate config due to error: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		lp.LogRunnerEvent("pokedexRunner not enabled -- won't run", log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	pokedex, err := parsePokedexFile()

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse pokedex json file: %s", err), log.FatalLevel)
	}

	l, err := initializeTestLoop(config)
	if err != nil {
		lp.LogRunnerEvent(fmt.Sprintf("aborting launch of map pokedex runner: unable to initialize test loop: %s", err.Error()), log.ErrorLevel)
		return
	}
	r.l = l

	r.appendState(assignTestLoopComplete)

	ctx := context.TODO()

	r.mapStore.InitHazelcastClient(ctx, r.name, hzCluster, hzMembers)
	defer func() {
		_ = r.mapStore.Shutdown(ctx)
	}()

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogRunnerEvent("initialized hazelcast client", log.InfoLevel)
	lp.LogRunnerEvent("starting pokedex maps loop", log.InfoLevel)

	lc := &testLoopConfig[pokemon]{uuid.New(), r.source, r.mapStore, config, pokedex.Pokemon, ctx, getPokemonID, deserializePokemon}

	r.l.init(lc, &defaultSleeper{}, status.NewGatherer())

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogRunnerEvent("finished pokedex maps loop", log.InfoLevel)

}

func (r *pokedexRunner) appendState(s state) {

	r.stateList = append(r.stateList, s)

}

func getPokemonID(element any) string {

	pokemon := element.(pokemon)
	return fmt.Sprintf("%d", pokemon.ID)

}

func deserializePokemon(elementFromHZ any) error {

	_, ok := elementFromHZ.(pokemon)
	if !ok {
		return errors.New("unable to serialize value retrieved from hazelcast map into pokemon instance")
	}

	return nil

}

func populatePokedexConfig(a client.ConfigPropertyAssigner) (*runnerConfig, error) {

	runnerKeyPath := "mapTests.pokedex"

	configBuilder := runnerConfigBuilder{
		assigner:      a,
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
	defer func() {
		if err := pokedexJson.Close(); err != nil {
			lp.LogRunnerEvent("unable to close pokedex json file", log.WarnLevel)
		}
	}()

	var pokedex pokedex
	err = json.NewDecoder(pokedexJson).Decode(&pokedex)

	if err != nil {
		return nil, err
	}

	lp.LogRunnerEvent("parsed pokedex file", log.TraceLevel)

	return &pokedex, nil

}
