package config

import (
	"flag"
)

const ArgUseUniSocketClient = "use-unisocket-client"
const ArgConfigFilePath = "config-file"

var configs map[string]interface{}

func init() {

	configs = make(map[string]interface{})

}

func ParseCommandLineArgs() {

	useUniSocketClient := flag.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")
	configFilePath := flag.String(ArgConfigFilePath, "defaultConfig.yaml", "File path of the config file to use. If unprovided, the program will use its embedded default config file.")

	flag.Parse()

	configs[ArgUseUniSocketClient] = *useUniSocketClient
	configs[ArgConfigFilePath] = *configFilePath

}

func RetrieveArgValue(arg string) interface{} {

	return configs[arg]

}
