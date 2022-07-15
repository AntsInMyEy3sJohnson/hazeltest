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
	"os"
	"strings"
)

const (
	ArgUseUniSocketClient = "use-unisocket-client"
	ArgConfigFilePath     = "config-file"
	defaultConfigFilePath = "defaultConfig.yaml"
)

var (
	commandLineArgs map[string]interface{}
	//go:embed defaultConfig.yaml
	defaultConfigFile  embed.FS
	defaultConfig      map[string]interface{}
	userSuppliedConfig map[string]interface{}
	lp                 *logging.LogProvider
)

func init() {
	commandLineArgs = make(map[string]interface{})
	lp = &logging.LogProvider{ClientID: ID()}
}

func ParseConfigs() {

	parseCommandLineArgs()
	parseDefaultConfigFile()
	parseUserSuppliedConfigFile()

}

func RetrieveConfigValue(keyPath string) (any, error) {

	pathElements := strings.Split(keyPath, ".")

	if len(pathElements) == 1 {
		return getValueFromMaps(keyPath)
	}

	currentPathElement := pathElements[0]
	sourceMap, err := getValueFromMaps(currentPathElement)

	if err != nil {
		return nil, fmt.Errorf("error upon attempt to parse value at '%s' into map for further processing", currentPathElement)
	}

	sourceMap = sourceMap.(map[string]interface{})

	keyPath = keyPath[strings.Index(keyPath, ".")+1:]

	return RetrieveConfigValue(sourceMap, keyPath)

}

func getValueFromMaps(key string) (any, error) {

	if value, ok := userSuppliedConfig[key]; ok {
		return value, nil
	}

	if value, ok := defaultConfig[key]; ok {
		return value, nil
	}

	return nil, errors.New(fmt.Sprintf("no config map contained value for key '%s'", key))

}

func parseCommandLineArgs() {

	useUniSocketClient := flag.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")
	configFilePath := flag.String(ArgConfigFilePath, "defaultConfig.yaml", "File path of the config file to use. If unprovided, the program will use its embedded default config file.")

	flag.Parse()

	commandLineArgs[ArgUseUniSocketClient] = *useUniSocketClient
	commandLineArgs[ArgConfigFilePath] = *configFilePath

}

func RetrieveArgValue(arg string) interface{} {

	return commandLineArgs[arg]

}

func parseDefaultConfigFile() {

	decodeConfigFile(&defaultConfig, defaultConfigFilePath, func(path string) (io.Reader, error) {
		return defaultConfigFile.Open(path)
	})

}

func parseUserSuppliedConfigFile() {

	configFilePath := RetrieveArgValue(ArgConfigFilePath).(string)

	if configFilePath == defaultConfigFilePath {
		lp.LogInternalStateEvent("user did not supply custom configuration file", log.InfoLevel)
		return
	}

	decodeConfigFile(&userSuppliedConfig, configFilePath, func(path string) (io.Reader, error) {
		return os.Open(path)
	})

}

func decodeConfigFile(target *map[string]interface{}, path string, openFileFunc func(path string) (io.Reader, error)) {

	r, err := openFileFunc(path)

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to read configuration file '%s': %v", path, err), log.FatalLevel)
	}

	if err = yaml.NewDecoder(r).Decode(target); err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse configuration file '%s': %v", path, err), log.FatalLevel)
	}

}

func GetParsedConfig() map[string]interface{} {

	return defaultConfig

}
