package client

import (
	"embed"
	"errors"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"hazeltest/logging"
	"io"
	"io/fs"
	"os"
	"strings"
)

const (
	ArgUseUniSocketClient = "use-unisocket-client"
	ArgConfigFilePath     = "config-file"
	defaultConfigFilePath = "defaultConfig.yaml"
)

type (
	fileOpener interface {
		open(string) (io.Reader, error)
	}
	defaultConfigFileOpener      struct{}
	userSuppliedConfigFileOpener struct{}
)

var (
	FailedParseCommandLineArgs        error
	FailedParseDefaultConfigFile      error
	FailedParseUserSuppliedConfigFile error
)

var (
	commandLineArgs map[string]interface{}
	//go:embed defaultConfig.yaml
	defaultConfigFile  embed.FS
	defaultConfig      map[string]interface{}
	userSuppliedConfig map[string]interface{}
	lp                 *logging.LogProvider
	d                  fileOpener
	u                  fileOpener
)

func init() {
	FailedParseCommandLineArgs = errors.New("unable to parse commandline-supplied arguments")
	FailedParseDefaultConfigFile = errors.New("unable to parse default config file")
	FailedParseUserSuppliedConfigFile = errors.New("unable to parse user-supplied config file")
	lp = &logging.LogProvider{ClientID: ID()}
	d = defaultConfigFileOpener{}
	u = userSuppliedConfigFileOpener{}
}

func (o defaultConfigFileOpener) open(path string) (io.Reader, error) {

	if file, err := defaultConfigFile.Open(path); err != nil {
		return nil, err
	} else {
		defer func(file fs.File) {
			err := file.Close()
			if err != nil {
				lp.LogIoEvent(fmt.Sprintf("unable to close file '%s'", path), log.WarnLevel)
			}
		}(file)
		return file, nil
	}

}

func (o userSuppliedConfigFileOpener) open(path string) (io.Reader, error) {

	if file, err := os.Open(path); err != nil {
		return nil, err
	} else {
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				lp.LogIoEvent(fmt.Sprintf("unable to close file '%s'", path), log.WarnLevel)
			}
		}(file)
		return file, nil
	}

}

func ParseConfigs() error {

	if args, err := parseCommandLineArgs(); err != nil {
		return FailedParseCommandLineArgs
	} else {
		commandLineArgs = args
	}

	if config, err := parseDefaultConfigFile(d); err != nil {
		return FailedParseDefaultConfigFile
	} else {
		defaultConfig = config
	}

	if config, err := parseUserSuppliedConfigFile(u, RetrieveArgValue(ArgConfigFilePath).(string)); err != nil {
		return FailedParseUserSuppliedConfigFile
	} else {
		userSuppliedConfig = config
	}

	return nil

}

func RetrieveArgValue(arg string) interface{} {

	return commandLineArgs[arg]

}

func PopulateConfigProperty(keyPath string, assignValue func(any)) error {

	if value, err := retrieveConfigValue(keyPath); err != nil {
		lp.LogErrUponConfigRetrieval(keyPath, err, log.ErrorLevel)
		return fmt.Errorf("unable to populate config property: could not find value matching key path: %s", keyPath)
	} else {
		assignValue(value)
		return nil
	}

}

func retrieveConfigValue(keyPath string) (any, error) {

	if value, err := retrieveConfigValueFromMap(userSuppliedConfig, keyPath); err == nil {
		lp.LogConfigEvent(keyPath, "config file", "found value in user-supplied config file", log.TraceLevel)
		return value, nil
	}

	if value, err := retrieveConfigValueFromMap(defaultConfig, keyPath); err == nil {
		lp.LogConfigEvent(keyPath, "config file", "found value in default config file", log.TraceLevel)
		return value, nil
	}

	errMsg := fmt.Sprintf("no map provides value for key '%s'", keyPath)
	lp.LogConfigEvent(keyPath, "config file", errMsg, log.WarnLevel)
	return nil, errors.New(errMsg)

}

func retrieveConfigValueFromMap(m map[string]any, keyPath string) (any, error) {

	if m == nil {
		return nil, fmt.Errorf("given config map was nil -- cannot look up key path '%s' in nil map", keyPath)
	}

	pathElements := strings.Split(keyPath, ".")

	if len(pathElements) == 1 {
		if value, ok := m[keyPath]; ok {
			return value, nil
		} else {
			return nil, fmt.Errorf("nested key '%s' not found in map", keyPath)
		}
	}

	currentPathElement := pathElements[0]
	sourceMap, ok := m[currentPathElement].(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("error upon attempt to parse value at '%s' into map for further processing", currentPathElement)
	}

	keyPath = keyPath[strings.Index(keyPath, ".")+1:]

	return retrieveConfigValueFromMap(sourceMap, keyPath)

}

func parseCommandLineArgs() (map[string]interface{}, error) {

	flagSet := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	useUniSocketClient := flagSet.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")
	configFilePath := flagSet.String(ArgConfigFilePath, "defaultConfig.yaml", "File path of the config file to use. If unprovided, the program will use its embedded default config file.")

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	target := make(map[string]interface{})
	target[ArgUseUniSocketClient] = *useUniSocketClient
	target[ArgConfigFilePath] = *configFilePath

	return target, nil

}

func parseDefaultConfigFile(o fileOpener) (map[string]interface{}, error) {

	return decodeConfigFile(defaultConfigFilePath, o.open)

}

func parseUserSuppliedConfigFile(o fileOpener, filePath string) (map[string]interface{}, error) {

	if filePath == defaultConfigFilePath {
		lp.LogInternalStateEvent("user did not supply custom configuration file", log.InfoLevel)
		return map[string]interface{}{}, nil
	}

	return decodeConfigFile(filePath, o.open)

}

func decodeConfigFile(path string, openFileFunc func(path string) (io.Reader, error)) (map[string]interface{}, error) {

	r, err := openFileFunc(path)

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to read configuration file '%s': %v", path, err), log.ErrorLevel)
		return nil, err
	}

	target := make(map[string]interface{})
	if err = yaml.NewDecoder(r).Decode(target); err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse configuration file '%s': %v", path, err), log.ErrorLevel)
		return nil, err
	} else {
		return target, nil
	}

}
