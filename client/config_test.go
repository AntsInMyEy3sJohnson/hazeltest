package client

import (
	"testing"
)

var mapTestsPokedexWithNumMapsUserSupplied = map[string]interface{}{
	"mapTests": map[string]interface{}{
		"pokedex": map[string]interface{}{
			"numMaps": 10,
		},
	},
}

var mapTestsPokedexWithNumMapsDefault = map[string]interface{}{
	"mapTests": map[string]interface{}{
		"pokedex": map[string]interface{}{
			"numMaps": 5,
		},
	},
}

var mapTestsPokedexWithEnabledDefault = map[string]interface{}{
	"mapTests": map[string]interface{}{
		"pokedex": map[string]interface{}{
			"enabled": true,
		},
	},
}

func TestUserSuppliedValueTakesPrecedenceOverDefault(t *testing.T) {

	defaultConfig = mapTestsPokedexWithNumMapsDefault
	userSuppliedConfig = mapTestsPokedexWithNumMapsUserSupplied

	expected := 10
	actual, err := retrieveConfigValue("mapTests.pokedex.numMaps")

	if err != nil {
		t.Errorf("got non-nil error: %v", err)
	}

	if actual.(int) != expected {
		t.Errorf("expected %d, got %d", expected, actual.(int))
	}

}

func TestExtractNestedNotMap(t *testing.T) {

	sourceMap := map[string]interface{}{
		"mapTests": []int{1, 2, 3, 4, 5},
	}
	defaultConfig = sourceMap

	_, err := retrieveConfigValue("mapTests.pokedex")

	if err == nil {
		t.Error("expected non-nil error value, received nil instead")
	}

}

func TestExtractKeyNotPresent(t *testing.T) {

	defaultConfig = mapTestsPokedexWithNumMapsDefault

	actual, err := retrieveConfigValue("mapTests.load")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	if actual != nil {
		t.Error("expected nil payload value, got non-nil value instead")
	}

}

func TestExtractNestedIntFromDefaultConfig(t *testing.T) {

	defaultConfig = mapTestsPokedexWithNumMapsDefault

	expected := 5
	actual, err := retrieveConfigValue("mapTests.pokedex.numMaps")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	if actual.(int) != expected {
		t.Errorf("Expected: %d; got: %d", expected, actual)
	}

}

func TestExtractNestedBoolFromDefaultConfig(t *testing.T) {

	defaultConfig = mapTestsPokedexWithEnabledDefault

	result, err := retrieveConfigValue("mapTests.pokedex.enabled")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	if !(result.(bool)) {
		t.Error("expected result to be 'true', but was 'false'")
	}

}
