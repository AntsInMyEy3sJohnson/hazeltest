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

type DefaultConfigPropertyAssigner struct{}

type (
	ConfigPropertyAssigner interface {
		Assign(keyPath string, validate func(string, any) error, assign func(any)) error
	}
	FailedParse struct {
		target  string
		keyPath string
	}
	FailedValueCheck struct {
		reason  string
		keyPath string
	}
	// TODO Introduce dedicated error types for other error scenarios, too?
)

type (
	fileOpener interface {
		open(string) (io.ReadCloser, error)
	}
	defaultConfigFileOpener      struct{}
	userSuppliedConfigFileOpener struct{}
)

var (
	ErrFailedParseCommandLineArgs        = errors.New("unable to parse commandline-supplied arguments")
	ErrFailedParseDefaultConfigFile      = errors.New("unable to parse default config file")
	ErrFailedParseUserSuppliedConfigFile = errors.New("unable to parse user-supplied config file")
)

var (
	d fileOpener = defaultConfigFileOpener{}
	u fileOpener = userSuppliedConfigFileOpener{}
)

var (
	commandLineArgs map[string]any
	//go:embed defaultConfig.yaml
	defaultConfigFile  embed.FS
	defaultConfig      map[string]any
	userSuppliedConfig map[string]any
	lp                 *logging.LogProvider
)

func init() {
	lp = &logging.LogProvider{ClientID: ID()}
}

func (o defaultConfigFileOpener) open(path string) (io.ReadCloser, error) {

	if file, err := defaultConfigFile.Open(path); err != nil {
		return nil, err
	} else {
		return file, nil
	}

}

func (o userSuppliedConfigFileOpener) open(path string) (io.ReadCloser, error) {

	if file, err := os.Open(path); err != nil {
		return nil, err
	} else {
		return file, nil
	}

}

func (v FailedParse) Error() string {

	return fmt.Sprintf("%s: failed to parse given value into %s", v.keyPath, v.target)

}

func (v FailedValueCheck) Error() string {

	return fmt.Sprintf("%s: given value failed plausibility check: %s", v.keyPath, v.reason)

}

func ValidateBool(path string, a any) error {

	if _, ok := a.(bool); !ok {
		return FailedParse{"bool", path}
	}

	return nil

}

func ValidateInt(path string, a any) error {

	if i, ok := a.(int); !ok {
		return FailedParse{"int", path}
	} else if i <= 0 {
		return FailedValueCheck{"expected this number to be at least 1", path}
	}

	return nil

}

func ValidateString(path string, a any) error {

	if s, ok := a.(string); !ok {
		return FailedParse{"string", path}
	} else if len(s) == 0 {
		return FailedValueCheck{"expected this string to be non-empty", path}
	}

	return nil

}

func ValidatePercentage(path string, a any) error {

	if f, ok := a.(float64); !ok {
		return FailedParse{"float64", path}
	} else if f < 0.0 || f > 1.0 {
		return FailedValueCheck{"expected float expressing percentage, i. e. 0.0 <= <number> <= 1.0", path}
	}

	return nil

}

func ParseConfigs() error {

	if args, err := parseCommandLineArgs(); err != nil {
		return ErrFailedParseCommandLineArgs
	} else {
		commandLineArgs = args
	}

	if config, err := parseDefaultConfigFile(d); err != nil {
		return ErrFailedParseDefaultConfigFile
	} else {
		defaultConfig = config
	}

	if config, err := parseUserSuppliedConfigFile(u, RetrieveArgValue(ArgConfigFilePath).(string)); err != nil {
		lp.LogConfigEvent("N/A", "config file", err.Error(), log.ErrorLevel)
		return ErrFailedParseUserSuppliedConfigFile
	} else {
		userSuppliedConfig = config
	}

	return nil

}

func RetrieveArgValue(arg string) any {

	return commandLineArgs[arg]

}

func (a DefaultConfigPropertyAssigner) Assign(keyPath string, validate func(string, any) error, assign func(any)) error {

	if value, err := retrieveConfigValue(keyPath); err != nil {
		lp.LogErrUponConfigRetrieval(keyPath, err, log.ErrorLevel)
		return fmt.Errorf("unable to populate config property: could not find value matching key path: %s", keyPath)
	} else {
		if err := validate(keyPath, value); err != nil {
			return err
		}
		assign(value)
	}

	return nil

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
	sourceMap, ok := m[currentPathElement].(map[string]any)

	if !ok {
		return nil, fmt.Errorf("error upon attempt to parse value at '%s' into map for further processing", currentPathElement)
	}

	keyPath = keyPath[strings.Index(keyPath, ".")+1:]

	return retrieveConfigValueFromMap(sourceMap, keyPath)

}

func parseCommandLineArgs() (map[string]any, error) {

	flagSet := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

	useUniSocketClient := flagSet.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")
	configFilePath := flagSet.String(ArgConfigFilePath, "defaultConfig.yaml", "File path of the config file to use. If unprovided, the program will use its embedded default config file.")

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	target := make(map[string]any)
	target[ArgUseUniSocketClient] = *useUniSocketClient
	target[ArgConfigFilePath] = *configFilePath

	lp.LogInternalStateEvent(fmt.Sprintf("command line arguments parsed: %v\n", target), log.InfoLevel)

	return target, nil

}

func parseDefaultConfigFile(o fileOpener) (map[string]any, error) {

	return decodeConfigFile(defaultConfigFilePath, o.open)

}

func parseUserSuppliedConfigFile(o fileOpener, filePath string) (map[string]any, error) {

	if filePath == defaultConfigFilePath {
		lp.LogInternalStateEvent("user did not supply custom configuration file", log.InfoLevel)
		return map[string]any{}, nil
	}

	return decodeConfigFile(filePath, o.open)

}

func decodeConfigFile(path string, openFileFunc func(path string) (io.ReadCloser, error)) (map[string]any, error) {

	r, err := openFileFunc(path)

	if err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to read configuration file '%s': %v", path, err), log.ErrorLevel)
		return nil, err
	}
	defer func(r io.ReadCloser) {
		err := r.Close()
		if err != nil {
			lp.LogIoEvent(fmt.Sprintf("unable to close file '%s'", path), log.WarnLevel)
		}
	}(r)

	target := make(map[string]any)
	if err = yaml.NewDecoder(r).Decode(target); err != nil {
		lp.LogIoEvent(fmt.Sprintf("unable to parse configuration file '%s': %v", path, err), log.ErrorLevel)
		return nil, err
	} else {
		return target, nil
	}

}
