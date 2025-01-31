package maps

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/state"
	"hazeltest/status"
)

type (
	pokedexRunner struct {
		assigner        client.ConfigPropertyAssigner
		stateList       []runnerState
		name            string
		source          string
		hzMapStore      hazelcastwrapper.MapStore
		hzClientHandler hazelcastwrapper.HzClientHandler
		l               looper[pokemon]
		gatherer        status.Gatherer
		providerFuncs   struct {
			mapStore        newMapStoreFunc
			pokemonTestLoop newPokemonTestLoopFunc
		}
	}
	newPokemonTestLoopFunc func(rc *runnerConfig) (looper[pokemon], error)
	pokedex                struct {
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
		assigner:        &client.DefaultConfigPropertyAssigner{},
		stateList:       []runnerState{},
		name:            "mapsPokedexRunner",
		source:          "pokedexRunner",
		hzClientHandler: &hazelcastwrapper.DefaultHzClientHandler{},
		providerFuncs: struct {
			mapStore        newMapStoreFunc
			pokemonTestLoop newPokemonTestLoopFunc
		}{mapStore: newDefaultMapStore, pokemonTestLoop: initPokedexTestLoop},
	})
}

func initPokedexTestLoop(rc *runnerConfig) (looper[pokemon], error) {

	switch rc.loopType {
	case batch:
		return &batchTestLoop[pokemon]{}, nil
	case boundary:
		return &boundaryTestLoop[pokemon]{}, nil
	default:
		return nil, fmt.Errorf("no such runner runnerLoopType: %s", rc.loopType)
	}

}

func (r *pokedexRunner) getSourceName() string {
	return "pokedexRunner"
}

func (r *pokedexRunner) runMapTests(ctx context.Context, hzCluster string, hzMembers []string, gatherer *status.DefaultGatherer) {

	r.gatherer = gatherer
	r.appendState(start)

	config, err := populatePokedexConfig(r.assigner)
	if err != nil {
		lp.LogMapRunnerEvent(fmt.Sprintf("aborting launch of map pokedex runner: unable to populate config due to error: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.appendState(populateConfigComplete)

	if !config.enabled {
		lp.LogMapRunnerEvent("pokedex runner not enabled -- won't run", r.name, log.InfoLevel)
		return
	}
	r.appendState(checkEnabledComplete)

	api.RaiseNotReady()

	p, err := parsePokedexFile(r.name)

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse pokedex json file: %s", err), log.FatalLevel)
	}

	l, err := r.providerFuncs.pokemonTestLoop(config)
	if err != nil {
		lp.LogMapRunnerEvent(fmt.Sprintf("aborting launch of map pokedex runner: unable to initialize test loop: %s", err.Error()), r.name, log.ErrorLevel)
		return
	}
	r.l = l

	r.appendState(assignTestLoopComplete)

	r.hzClientHandler.InitHazelcastClient(ctx, r.name, hzCluster, hzMembers)
	defer func() {
		_ = r.hzClientHandler.Shutdown(ctx)
	}()
	r.hzMapStore = r.providerFuncs.mapStore(r.hzClientHandler)

	api.RaiseReady()
	r.appendState(raiseReadyComplete)

	lp.LogMapRunnerEvent("initialized hazelcast client", r.name, log.InfoLevel)
	lp.LogMapRunnerEvent("starting pokedex test loop for maps", r.name, log.InfoLevel)

	le := &testLoopExecution[pokemon]{
		id:                   uuid.New(),
		runnerName:           r.name,
		source:               r.source,
		hzClientHandler:      r.hzClientHandler,
		hzMapStore:           r.hzMapStore,
		stateCleanerBuilder:  &state.DefaultSingleMapCleanerBuilder{},
		runnerConfig:         config,
		elements:             p.Pokemon,
		ctx:                  ctx,
		getElementID:         getPokemonID,
		getOrAssemblePayload: returnPokemonPayload,
	}

	r.l.init(le, &defaultSleeper{}, r.gatherer)

	r.appendState(testLoopStart)
	r.l.run()
	r.appendState(testLoopComplete)

	lp.LogMapRunnerEvent("finished pokedex maps loop", r.name, log.InfoLevel)

}

func (r *pokedexRunner) appendState(s runnerState) {

	r.stateList = append(r.stateList, s)
	r.gatherer.Gather(status.Update{Key: string(statusKeyCurrentState), Value: string(s)})

}

func returnPokemonPayload(_ string, _ uint16, element any) (*string, error) {

	if _, ok := element.(pokemon); !ok {
		return nil, fmt.Errorf("given element is not a pokemon: %v", element)
	}

	pJson, err := json.Marshal(element)
	if err != nil {
		// Effectively can't happen -- once we've made sure
		// the given element is a pokemon, the marshal operation
		// must be successful. But keep check in place just in case...
		return nil, err
	}
	pString := string(pJson)
	return &pString, nil
}

func getPokemonID(element any) string {

	p := element.(pokemon)
	return fmt.Sprintf("%d", p.ID)

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

func parsePokedexFile(runnerName string) (*pokedex, error) {

	pokedexJson, err := pokedexFile.Open("pokedex.json")

	if err != nil {
		return nil, err
	}
	defer func() {
		if err := pokedexJson.Close(); err != nil {
			lp.LogMapRunnerEvent("unable to close pokedex json file", runnerName, log.WarnLevel)
		}
	}()

	var pokedex pokedex
	err = json.NewDecoder(pokedexJson).Decode(&pokedex)

	if err != nil {
		return nil, err
	}

	lp.LogMapRunnerEvent("parsed pokedex file", runnerName, log.TraceLevel)

	return &pokedex, nil

}
