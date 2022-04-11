package maps

import (
	"context"
	"embed"
	"encoding/json"
	"hazeltest/client"
)

type PokedexRunner struct {}

func init() {
	Register(PokedexRunner{})
}

//go:embed pokedex.json
var pokedexFile embed.FS

func (r PokedexRunner) Run(hzCluster string, hzMembers []string) {

	pokedexJson, err := pokedexFile.Open("pokedex.json")

	if err != nil {
		panic(err)
	}
	defer pokedexJson.Close()

	var pokedex *pokedex
	err = json.NewDecoder(pokedexJson).Decode(&pokedex)
	
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()

	hzClient, err := client.InitHazelcastClient(ctx, hzCluster, hzMembers)

	if err != nil {
		panic(err)
	}
	defer hzClient.Shutdown(ctx)

	pokedexItems, err := hzClient.GetMap(ctx, "pokedex")

	if err != nil {
		panic(err)
	}

	for _, pokemon := range pokedex.Pokemon {
		containsKey, err := pokedexItems.ContainsKey(ctx, pokemon.ID)
		if err != nil {
			panic(err)
		}
		if !containsKey {
			pokedexItems.Set(ctx, pokemon.ID, pokemon)
		}
	}

}