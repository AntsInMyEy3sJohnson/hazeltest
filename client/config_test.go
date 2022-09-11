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
)

func (o testConfigOpener) open(_ string) (io.Reader, error) {

	b, _ := yaml.Marshal(mapTestsPokedexWithNumMapsUserSupplied)
	return bytes.NewReader(b), nil

}

func TestRetrieveArgValue(t *testing.T) {

	t.Log("given the need to test the retrieval of values from the commandline-provided config")
	{

		t.Log("\twhen providing an argument contained in the commandline-supplied argument list")
		{
			commandLineArgs = parseCommandLineArgs()

			actual, err := RetrieveArgValue(ArgConfigFilePath)

			msg := "\t\tno error should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\texpected value should be returned"
			expected := "defaultConfig.yaml"

			if actual == expected {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen providing an argument not contained in the commandline-supplied argument list")
		{
			_, err := RetrieveArgValue("some-arg")

			msg := "\t\tan error should be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func TestPopulateConfigProperty(t *testing.T) {

	t.Log("given the need to test populating a config property")
	{
		t.Log("\twhen providing an assignment function and a map containing the desired key")
		{
			defaultConfig = mapTestsPokedexWithNumMapsDefault

			var target int
			err := PopulateConfigProperty("mapTests.pokedex.numMaps", func(a any) {
				target = a.(int)
			})

			msg := "\t\tno error should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			expected := 5
			msg = "\t\tassignment function must have been called"
			if target == expected {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing an assignment function and a map not containing the desired key")
		{
			err := PopulateConfigProperty("mapTests.pokedex.enabled", func(_ any) {
				// No-op
			})

			msg := "\t\terror should be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

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
				b, _ := yaml.Marshal(mapTestsPokedexWithNumMapsDefault)
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

func TestRetrieveConfigValue(t *testing.T) {

	defer t.Cleanup(teardown)

	t.Log("given the need to test config value retrieval")
	{
		t.Log("\twhen providing a default and a user-supplied config map")
		{
			defaultConfig = mapTestsPokedexWithNumMapsDefault
			userSuppliedConfig = mapTestsPokedexWithNumMapsUserSupplied

			expected := 10
			actual, err := retrieveConfigValue("mapTests.pokedex.numMaps")

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tuser-supplied config value should be returned"
			if actual == expected {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a config map not containing a nested map")
		{
			defaultConfig = map[string]interface{}{
				"mapTests": []int{1, 2, 3, 4, 5},
			}
			userSuppliedConfig = nil

			_, err := retrieveConfigValue("mapTests.pokedex")

			msg := "\t\terror should occur"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a config map not containing the desired key")
		{
			defaultConfig = mapTestsPokedexWithNumMapsDefault
			_, err := retrieveConfigValue("mapTests.load")

			msg := "\t\tan error should be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Log(msg, ballotX)
			}
		}

		t.Log("\twhen providing a config map containing the desired key in a nested sub-map")
		{

			defaultConfig = mapTestsPokedexWithNumMapsDefault
			expected := 5
			actual, err := retrieveConfigValue("mapTests.pokedex.numMaps")

			msg := "\t\tno error should occur"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tcorrect value should be returned"
			if actual == expected {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

	}

}

func teardown() {

	defaultConfig = nil
	userSuppliedConfig = nil

}
