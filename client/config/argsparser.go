package config

import (
	"flag"
)

const ArgUseUniSocketClient = "use-unisocket-client"

var configs map[string]interface{}

func init() {

	configs = make(map[string]interface{})

}

func ParseCommandLineArgs() {

	useUnisocketClient := flag.Bool(ArgUseUniSocketClient, false, "Configures whether to use the client in unisocket mode. Using unisocket mode disables smart routing, hence translates to using the client as a \"dumb client\".")

	flag.Parse()

	configs[ArgUseUniSocketClient] = *useUnisocketClient

}

func RetrieveArgValue(arg string) interface{} {

	return configs[arg]

}
