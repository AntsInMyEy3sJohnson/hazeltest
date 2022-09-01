package client

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

const (
	customConfigFile = "customConfig.yaml"
)

type (
	testParser struct{}
)

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

func (p testParser) Parse() {

	// Index 0 contains executable path, so start at index 1
	os.Args[1] = fmt.Sprintf("--%s=true", ArgUseUniSocketClient)
	os.Args[2] = fmt.Sprintf("--%s=%s", ArgConfigFilePath, customConfigFile)

	flag.Parse()

}

func TestParseCommandLineArgs(t *testing.T) {

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	parsed := parseCommandLineArgs(testParser{})

	actual := parsed[ArgUseUniSocketClient]
	if true != actual {
		t.Errorf("for '%s', expected '%t', got '%t'", ArgUseUniSocketClient, true, actual)
	}

	actual = parsed[ArgConfigFilePath]
	if customConfigFile != actual {
		t.Errorf("for '%s', expected '%s', got '%s'", ArgConfigFilePath, customConfigFile, actual)
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
