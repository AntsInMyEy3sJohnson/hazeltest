package client

import (
	"bytes"
	"errors"
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
			config, err := parseDefaultConfigFile(testConfigOpener{})

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdefault config state should be populated"
			if len(config) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func TestParseUserSuppliedConfig(t *testing.T) {

	t.Log("given the need to test populating the config map from the user-supplied config file")
	{
		t.Log("\twhen providing the default config file path")
		{
			config, err := parseUserSuppliedConfigFile(testConfigOpener{}, defaultConfigFilePath)

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned map should be empty"
			if len(config) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing any path other than the default config file path")
		{
			config, err := parseUserSuppliedConfigFile(testConfigOpener{}, "some-user-supplied-config.yaml")

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned map should be populated"
			if len(config) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func TestDecodeConfigFile(t *testing.T) {

	t.Log("given the need to test decoding the yaml config file")
	{
		t.Log("\twhen providing a target map and a file open function that returns a valid io.Reader")
		{
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.Reader, error) {
				b, _ := yaml.Marshal(mapTestsPokedexWithEnabledDefault)
				return bytes.NewReader(b), nil
			})

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\ttarget map should be populated"
			if len(target) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a target map and a file open function that returns an error")
		{
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.Reader, error) {
				return nil, errors.New("lo and behold, an error")
			})

			msg := "\t\terror should be reported"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned map should be empty"
			if len(target) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a target map and a file open function that returns an io.Reader producing invalid yaml")
		{
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.Reader, error) {
				return bytes.NewReader([]byte("this is not yaml")), nil
			})

			msg := "\t\terror should be reported"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned map should be empty"
			if len(target) == 0 {
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
