package client

import (
	"bytes"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"strings"
	"testing"
)

type (
	testConfigOpener struct {
		m map[string]interface{}
	}
	erroneousTestConfigOpener struct{}
	testReadCloser            struct {
		io.Reader
		io.Closer
	}
	testCloser struct{}
)

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
	defaultArgs = []string{os.Args[0], fmt.Sprintf("--%s=false", ArgUseUniSocketClient), fmt.Sprintf("--%s=%s", ArgConfigFilePath, defaultConfigFilePath)}
)

func (o testConfigOpener) open(_ string) (io.ReadCloser, error) {

	b, _ := yaml.Marshal(o.m)
	return testReadCloser{
		Reader: bytes.NewReader(b),
		Closer: testCloser{},
	}, nil

}

func (o erroneousTestConfigOpener) open(_ string) (io.ReadCloser, error) {

	return nil, errors.New("lo and behold, here i am, a dummy error")

}

func (c testCloser) Close() error {

	return nil

}

func TestValidateString(t *testing.T) {

	t.Log("given the need to test the string validation function")
	{
		path := "maptests.load.mapPrefix.prefix"

		t.Log("\twhen providing a value that can be parsed into a string")
		{
			msg := "\t\tno error should be returned"

			err := ValidateString(path, "ht_")

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		correctTypeOfErrorMsg := "\t\terror of correct type should be returned"
		pathInErrorStringMsg := "\t\tpath should be contained in error string"
		t.Log("\twhen providing a value that can be parsed into a string, but yields an empty string")
		{
			err := ValidateString(path, "")

			if err != nil && errors.As(err, &FailedValueCheck{}) {
				t.Log(correctTypeOfErrorMsg, checkMark)
			} else {
				t.Error(correctTypeOfErrorMsg, ballotX)
			}

			if strings.Contains(err.Error(), path) {
				t.Log(pathInErrorStringMsg, checkMark)
			} else {
				t.Error(pathInErrorStringMsg, ballotX)
			}
		}

		t.Log("\twhen providing a value that cannot be parsed into a string")
		{
			for _, v := range []any{1.0, true, 42, []float32{1.2, 2.1, 4.3}, map[int]string{0: "frodo", 1: "gandalf"}} {

				err := ValidateString(path, v)

				if err != nil && errors.As(err, &FailedParse{}) {
					t.Log(correctTypeOfErrorMsg, checkMark, v)
				} else {
					t.Error(correctTypeOfErrorMsg, ballotX, v)
				}

				if strings.Contains(err.Error(), path) {
					t.Log(pathInErrorStringMsg, checkMark)
				} else {
					t.Error(pathInErrorStringMsg, ballotX)
				}
			}
		}
	}

}

func TestValidateInt(t *testing.T) {

	t.Log("given the need to test the int validation function")
	{
		path := "queuetests.tweets.numQueues"
		t.Log("\twhen providing a semantically correct value that can be parsed into an int")
		{
			for _, v := range []int{1, 5, 42} {
				err := ValidateInt(path, v)

				msg := "\t\tno error should occur"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Error(msg, ballotX)
				}
			}
		}

		correctTypeOfErrorMsg := "\t\terror of correct type should be returned"
		pathInErrorStringMsg := "\t\tpath should be contained in error string"
		t.Log("\twhen providing a value that can be parsed into an int, but is semantically incorrect")
		{
			for _, v := range []int{-1, 0} {
				err := ValidateInt(path, v)

				if err != nil && errors.As(err, &FailedValueCheck{}) {
					t.Log(correctTypeOfErrorMsg, checkMark, v)
				} else {
					t.Error(correctTypeOfErrorMsg, ballotX, v)
				}

				if strings.Contains(err.Error(), path) {
					t.Log(pathInErrorStringMsg, checkMark)
				} else {
					t.Error(pathInErrorStringMsg, ballotX)
				}
			}
		}

		t.Log("\twhen providing a value that cannot be parsed into an int")
		{
			for _, v := range []any{false, "blubb", 1.0, []int{1, 2, 3}, map[string]int{"hello": 1, "goodbye": 2}} {
				err := ValidateInt(path, v)

				if err != nil && errors.As(err, &FailedParse{}) {
					t.Log(correctTypeOfErrorMsg, checkMark, v)
				} else {
					t.Error(correctTypeOfErrorMsg, ballotX, v)
				}

				if strings.Contains(err.Error(), path) {
					t.Log(pathInErrorStringMsg, checkMark)
				} else {
					t.Error(pathInErrorStringMsg, ballotX)
				}
			}
		}

	}

}

func TestValidateBool(t *testing.T) {

	t.Log("given the need to test the bool validation function")
	{
		path := "maptests.pokedex.enabled"
		t.Log("\twhen providing a value that can be parsed into a bool")
		{
			err := ValidateBool(path, true)

			msg := "\t\tno error should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a value that cannot be parsed into a bool")
		{
			err := ValidateBool(path, "not_a_bool_value")

			msg := "\t\terror of correct type should be returned"
			if err != nil && errors.As(err, &FailedParse{}) {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			errorMessage := err.Error()
			msg = "\t\terror message should contain key path"
			if strings.Contains(errorMessage, path) {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			msg = "\t\terror message should contain target type"
			if strings.Contains(errorMessage, "bool") {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}
	}

}

func TestParseConfigs(t *testing.T) {

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	t.Log("given the need to test the parsing of configuration values from config file and commandline")
	{
		t.Log("\twhen providing undefined commandline arguments")
		{
			os.Args = []string{os.Args[0], "--some-undefined-arg=blah"}
			err := ParseConfigs()

			msg := "\t\tcorrect type of error should be returned"
			if err != nil && err == ErrFailedParseCommandLineArgs {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen providing a valid file opener to parse the default config file and no user-supplied config file")
		{
			d = testConfigOpener{m: mapTestsPokedexWithNumMapsDefault}

			os.Args = defaultArgs
			err := ParseConfigs()

			msg := "\t\tno error should occur"

			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdefault config map should be populated"
			if len(defaultConfig) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			msg = "\t\tuser-supplied config map should not be populated"
			if len(userSuppliedConfig) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing an error-throwing file opener to parse the default config file")
		{
			defaultConfig = nil
			d = erroneousTestConfigOpener{}

			err := ParseConfigs()

			msg := "\t\tcorrect type of error should be returned"
			if err != nil && err == ErrFailedParseDefaultConfigFile {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdefault config map should be empty"
			if len(defaultConfig) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing valid file openers for parsing both the default and the user-supplied config file, and a user-supplied config file path")
		{
			d = testConfigOpener{m: mapTestsPokedexWithNumMapsDefault}
			u = testConfigOpener{m: mapTestsPokedexWithNumMapsUserSupplied}

			os.Args = []string{os.Args[0], fmt.Sprintf("--%s=false", ArgUseUniSocketClient), fmt.Sprintf("--%s=%s", ArgConfigFilePath, "a-user-supplied-config-file.yaml")}
			err := ParseConfigs()

			msg := "\t\tno error should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdefault config map should be populated"
			if len(defaultConfig) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}

			msg = "\t\tuser-supplied config map should be populated"
			if len(userSuppliedConfig) > 0 {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

		t.Log("\twhen providing a valid file opener for the default config file, but one that throws an error for the user-supplied config file")
		{
			u = erroneousTestConfigOpener{}

			err := ParseConfigs()

			msg := "\t\tcorrect type of error should be returned"
			if err != nil && err == ErrFailedParseUserSuppliedConfigFile {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

	}

}

func TestRetrieveArgValue(t *testing.T) {

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	t.Log("given the need to test the retrieval of values from the commandline-provided config")
	{

		t.Log("\twhen providing an argument contained in the commandline-supplied argument list")
		{
			os.Args = defaultArgs
			args, err := parseCommandLineArgs()

			msg := "\t\tno error should be returned upon parsing of commandline-provided arguments"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			commandLineArgs = args

			actual := RetrieveArgValue(ArgConfigFilePath)

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
			actual := RetrieveArgValue("some-arg")

			msg := "\t\tnil should be returned"
			if actual == nil {
				t.Log(msg, checkMark)
			} else {
				t.Error(msg, ballotX)
			}
		}

	}

}

func TestPopulateConfigProperty(t *testing.T) {

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	a := DefaultConfigPropertyAssigner{}

	t.Log("given the need to test populating a config property")
	{
		t.Log("\twhen providing an assignment function and a map containing the desired key")
		{
			defaultConfig = mapTestsPokedexWithNumMapsDefault

			var target int
			err := a.Assign("mapTests.pokedex.numMaps", func(_ string, a any) error {
				return nil
			}, func(a any) {
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
			err := a.Assign("mapTests.pokedex.enabled", func(_ string, _ any) error {
				// No-op
				return nil
			}, func(a any) {
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

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	t.Log("given the need to test populating the config state from the default config file")
	{
		t.Log("\twhen providing a fileOpener")
		{
			config, err := parseDefaultConfigFile(testConfigOpener{m: mapTestsPokedexWithNumMapsDefault})

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

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	t.Log("given the need to test populating the config map from the user-supplied config file")
	{
		t.Log("\twhen providing the default config file path")
		{
			config, err := parseUserSuppliedConfigFile(testConfigOpener{m: mapTestsPokedexWithNumMapsUserSupplied}, defaultConfigFilePath)

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
			config, err := parseUserSuppliedConfigFile(testConfigOpener{m: mapTestsPokedexWithNumMapsUserSupplied}, "some-user-supplied-config.yaml")

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

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

	t.Log("given the need to test decoding the yaml config file")
	{
		t.Log("\twhen providing a target map and a file open function that returns a valid io.Reader")
		{
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.ReadCloser, error) {
				b, _ := yaml.Marshal(mapTestsPokedexWithNumMapsDefault)
				return testReadCloser{
					Reader: bytes.NewReader(b),
					Closer: testCloser{},
				}, nil
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
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.ReadCloser, error) {
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
			target, err := decodeConfigFile(defaultConfigFilePath, func(path string) (io.ReadCloser, error) {
				return testReadCloser{
					Reader: bytes.NewReader([]byte("this is not yaml")),
					Closer: testCloser{},
				}, nil
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

	oldArgs := os.Args
	defer t.Cleanup(func() {
		os.Args = oldArgs
		teardown(oldArgs)
	})

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

func teardown(oldArgs []string) {

	os.Args = oldArgs

	defaultConfig = nil
	userSuppliedConfig = nil

}
