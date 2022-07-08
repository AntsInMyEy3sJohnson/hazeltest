package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
)

func logIoEvent(msg string) {

	fields := log.Fields{
		"kind":   logging.IoError,
		"client": client.ClientID(),
	}

	doLog(msg, fields, log.FatalLevel)

}

func logTimingEvent(operation string, mapName string, tookMs int) {

	fields := log.Fields{
		"kind":   logging.TimingInfo,
		"client": client.ClientID(),
		"map":    mapName,
		"tookMs": tookMs,
	}

	doLog(fmt.Sprintf("'%s' took %d ms", operation, tookMs), fields, log.InfoLevel)

}

func logInternalStateEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}

	doLog(msg, fields, level)

}

func logHzEvent(msg string) {

	fields := log.Fields{
		"kind":   logging.HzError,
		"client": client.ClientID(),
	}

	doLog(msg, fields, log.WarnLevel)
}

func logErrUponConfigExtraction(keyPath string, err error) {

	logConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), log.WarnLevel)

}

func logConfigEvent(configValue string, source string, msg string, level log.Level) {

	fields := log.Fields{
		"kind":   logging.ConfigurationError,
		"value":  configValue,
		"source": source,
		"client": client.ClientID(),
	}

	doLog(msg, fields, level)

}

func doLog(msg string, fields log.Fields, level log.Level) {

	if level == log.FatalLevel {
		log.WithFields(fields).Fatal(msg)
	} else if level == log.WarnLevel {
		log.WithFields(fields).Warn(msg)
	} else if level == log.InfoLevel {
		log.WithFields(fields).Info(msg)
	} else {
		log.WithFields(fields).Trace(msg)
	}

}
