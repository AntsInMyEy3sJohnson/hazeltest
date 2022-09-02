package client

import (
	"bytes"
	"gopkg.in/yaml.v3"
	"io"
	"testing"
)

type testConfigOpener struct{}

var (
	mapTestsPokedexWithNumMapsUserSupplied = map[string]interface{}{
		"mapTests": map[string]interface{}{
			"pokedex": map[string]interface{}{
				"numMaps": 10,
			},
		},
	}
	mapTestsPokedexWithNumMapsDefault = map[string]interface{}{
		"mapTests": map[string]interface{}{
			"pokedex": map[string]interface{}{
				"numMaps": 5,
			},
		},
	}
	mapTestsPokedexWithEnabledDefault = map[string]interface{}{
		"mapTests": map[string]interface{}{
			"pokedex": map[string]interface{}{
				"enabled": true,
			},
		},
	}
)

func (o testConfigOpener) open(path string) (io.Reader, error) {

	b, _ := yaml.Marshal(mapTestsPokedexWithNumMapsUserSupplied)
	return bytes.NewReader(b), nil

}

func TestParseUserSuppliedConfig(t *testing.T) {

	parseCommandLineArgs()
	// File path does not matter here since we're not reading an actual file
	parseUserSuppliedConfigFile(testConfigOpener{}, "")

	if len(userSuppliedConfig) == 0 {
		t.Errorf("expected populated map with user-supplied config values, but got empty map")
	}

}

func TestUserSuppliedValueTakesPrecedenceOverDefault(t *testing.T) {

	defer t.Cleanup(teardown)

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

	defer t.Cleanup(teardown)

	defaultConfig = map[string]interface{}{
		"mapTests": []int{1, 2, 3, 4, 5},
	}

	_, err := retrieveConfigValue("mapTests.pokedex")

	if err == nil {
		t.Error("expected non-nil error value, received nil instead")
	}

}

func TestExtractKeyNotPresent(t *testing.T) {

	defer t.Cleanup(teardown)

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

	defer t.Cleanup(teardown)

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

	defer t.Cleanup(teardown)

	defaultConfig = mapTestsPokedexWithEnabledDefault

	result, err := retrieveConfigValue("mapTests.pokedex.enabled")

	if err != nil {
		t.Errorf("Got non-nil error value: %s", err)
	}

	if !(result.(bool)) {
		t.Error("expected result to be 'true', but was 'false'")
	}

}

func teardown() {

	defaultConfig = nil
	userSuppliedConfig = nil

}
