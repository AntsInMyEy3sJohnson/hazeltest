package client

import (
	"bytes"
	"gopkg.in/yaml.v3"
	"io"
	"testing"
)

type testConfigOpener struct{}

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
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

func (o testConfigOpener) open(_ string) (io.Reader, error) {

	b, _ := yaml.Marshal(mapTestsPokedexWithNumMapsUserSupplied)
	return bytes.NewReader(b), nil

}

func TestParseDefaultConfig(t *testing.T) {

	t.Log("given the need to test populating the config state from the default config file")
	{
		t.Log("\twhen providing a fileOpener")
		{
			parseDefaultConfigFile(testConfigOpener{})
			msg := "\t\tdefault config state should be populated"
			if len(defaultConfig) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func TestParseUserSuppliedConfig(t *testing.T) {

	t.Log("given the need to test populating the config state from the user-supplied config file")
	{
		t.Log("\twhen providing the default config file path")
		{
			parseUserSuppliedConfigFile(testConfigOpener{}, defaultConfigFilePath)
			msg := "\t\tuser-provided config state should be empty"
			if len(userSuppliedConfig) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing any path other than the default config file path")
		{
			parseUserSuppliedConfigFile(testConfigOpener{}, "some-user-supplied-config.yaml")
			msg := "\t\tuser-provided config state should be populated"
			if len(userSuppliedConfig) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
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
