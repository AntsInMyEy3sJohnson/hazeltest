package config

import (
	"embed"
	"fmt"
	"hazeltest/logging"
	"io/fs"
	"os"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type FileParser struct {
	ClientID uuid.UUID
}

//go:embed defaultConfig.yaml
var defaultConfigFile embed.FS

var configMap map[string]interface{}

const defaultConfigFilePath = "defaultConfig.yaml"

func (f *FileParser) ParseConfigFile() {

	configFilePath := RetrieveArgValue(ArgConfigFilePath).(string)

	var fileToRead fs.File
	var err error

	if configFilePath == defaultConfigFilePath {
		logConfigEvent(ArgConfigFilePath, "command line", "using default embedded configuration file", f.ClientID)
		fileToRead, err = defaultConfigFile.Open("defaultConfig.yaml")
	} else {
		fileToRead, err = os.Open(configFilePath)
	}

	if err != nil {
		logIoEvent(fmt.Sprintf("unable to read configuration file: %s", err), f.ClientID, log.FatalLevel)
	}
	defer fileToRead.Close()

	err = yaml.NewDecoder(fileToRead).Decode(&configMap)

	if err != nil {
		logIoEvent("unable to parse configuration file -- aborting", f.ClientID, log.FatalLevel)
	}

}

func GetParsedConfig() map[string]interface{} {

	return configMap

}

func logIoEvent(msg string, clientID uuid.UUID, level log.Level) {

	fields := log.Fields{
		"kind":   logging.IoError,
		"client": clientID,
	}

	if level == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else {
		log.WithFields(fields).Fatal(msg)
	}

}

func logConfigEvent(configValue string, source string, msg string, clientID uuid.UUID) {

	log.WithFields(log.Fields{
		"kind":   "config information",
		"value":  configValue,
		"source": source,
		"client": clientID,
	}).Info(msg)

}
