package logging

import (
	"os"
	"io"
	"strings"
	log "github.com/sirupsen/logrus"
)

const InternalStateInfo = "internal state info"
const TimingInfo = "timing info"
const IoError = "io error"
const HzError = "hazelcast error"
const ConfigurationError = "incorrect or incomplete configuration"

func init () {

	log.SetFormatter(&log.JSONFormatter{})

	definedLogLevel := os.Getenv("LOG_LEVEL")

	var logLevel log.Level
	var out io.Writer

	switch strings.ToLower(definedLogLevel) {
	case "trace":
		logLevel = log.TraceLevel
		out = os.Stdout
	case "debug":
		logLevel = log.DebugLevel
		out = os.Stdout
	case "info":
		logLevel = log.InfoLevel
		out = os.Stdout
	case "warn":
		logLevel = log.WarnLevel
		out = os.Stderr
	case "error":
		logLevel = log.ErrorLevel
		out = os.Stderr
	default:
		logLevel = log.InfoLevel
		out = os.Stdout
	}

	log.SetLevel(logLevel)
	log.SetOutput(out)
	log.SetReportCaller(true)

}