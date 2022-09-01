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

type (
	flagParser interface {
		Parse()
	}
	defaultFlagParser struct{}
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
	lp = &logging.LogProvider{ClientID: ID()}
}

func (p defaultFlagParser) Parse() {

	flag.Parse()

}

func ParseConfigs() {

	commandLineArgs = parseCommandLineArgs(defaultFlagParser{})
	parseDefaultConfigFile()
	parseUserSuppliedConfigFile()

}

func RetrieveArgValue(arg string) interface{} {

	return commandLineArgs[arg]

}

func PopulateConfigProperty(keyPath string, assignValue func(any)) {

	if value, err := retrieveConfigValue(keyPath); err != nil {
		lp.LogErrUponConfigRetrieval(keyPath, err, log.FatalLevel)
	} else {
		assignValue(value)
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
		return m[keyPath], nil
	}

	currentPathElement := pathElements[0]
	sourceMap, ok := m[currentPathElement].(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("error upon attempt to parse value at '%s' into map for further processing", currentPathElement)
	}

	keyPath = keyPath[strings.Index(keyPath, ".")+1:]

	return retrieveConfigValueFromMap(sourceMap, keyPath)

}

func parseCommandLineArgs(p flagParser) map[string]interface{} {

	useUniSocketClient := flag.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")
	configFilePath := flag.String(ArgConfigFilePath, "defaultConfig.yaml", "File path of the config file to use. If unprovided, the program will use its embedded default config file.")

	p.Parse()

	commandLineArgs = make(map[string]interface{})
	commandLineArgs[ArgUseUniSocketClient] = *useUniSocketClient
	commandLineArgs[ArgConfigFilePath] = *configFilePath

	return commandLineArgs

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
