package config_test

import (
	"hazeltest/client/config"
	"testing"
)

var mapTestsPokedexWithNumMaps = map[string]interface{}{
	"mapTests": map[string]interface{} {
		"pokedex": map[string]interface{}{
			"numMaps": 5,
		},
	},
}

var mapTestsPokedexWithEnabled = map[string]interface{}{
	"mapTests": map[string]interface{} {
		"pokedex": map[string]interface{}{
			"enabled": true,
		},
	},
}

func TestExtractNestedNotMap(t *testing.T) {

	sourceMap := map[string]interface{}{
		"mapTests": []int{1, 2, 3, 4, 5},
	}

	_, err := config.ExtractConfigValue(sourceMap, "mapTests.pokedex")

	if err == nil {
		t.Error("Expected non-nil error value, received nil instead")
	}

}

func TestExctractKeyNotPresent(t *testing.T) {

	sourceMap := mapTestsPokedexWithNumMaps

	actual, err := config.ExtractConfigValue(sourceMap, "mapTests.load")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	if actual != nil {
		t.Error("Expected nil payload value, got non-nil value instead")
	}

}

func TestExtractNestedInt(t *testing.T) {

	sourceMap := mapTestsPokedexWithNumMaps

	expectedInt := 5
	actualInt, err := config.ExtractConfigValue(sourceMap, "mapTests.pokedex.numMaps")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}
	
	actualInt = actualInt.(int)

	if expectedInt != actualInt {
		t.Errorf("Expected: %d; got: %d", expectedInt, actualInt)
	}

	

}

func TestExtractNestedBool(t *testing.T) {

	sourceMap := mapTestsPokedexWithEnabled

	expected := true
	actual, err := config.ExtractConfigValue(sourceMap, "mapTests.pokedex.enabled")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	actual = actual.(bool)

	if expected != actual {
		t.Errorf("Expected: %t; got: %t", expected, actual)
	}

}