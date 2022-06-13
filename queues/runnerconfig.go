package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
)

type RunnerConfig struct {
	Enabled bool
}

type RunnerConfigBuilder struct {
	RunnerKeyPath string
	ParsedConfig  map[string]interface{}
}

func (b RunnerConfigBuilder) PopulateConfig() *RunnerConfig {

	keyPath := b.RunnerKeyPath + ".enabled"
	valueFromConfig, err := config.ExtractConfigValue(b.ParsedConfig, keyPath)
	var enabled bool
	if err != nil {
		logErrUponConfigExtraction(keyPath, err)
		enabled = defaultEnabled
	} else {
		enabled = valueFromConfig.(bool)
	}

	return &RunnerConfig{Enabled: enabled}

}

const (
	defaultEnabled = true
)

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}

func logConfigEvent(configValue string, source string, msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}
	if logLevel == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

}
