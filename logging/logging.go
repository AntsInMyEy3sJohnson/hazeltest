package logging

import (
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"runtime"
	"strings"
)

const ApiInfo = "api info"
const InternalStateInfo = "internal state info"
const TimingInfo = "timing info"
const IoError = "io error"
const HzError = "hazelcast error"
const ConfigurationError = "incorrect or incomplete configuration"

type LogProvider struct {
	ClientID uuid.UUID
}

func init() {

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

func (lp *LogProvider) LogIoEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": IoError,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogApiEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": ApiInfo,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogTimingEvent(operation string, dataStructureName string, tookMs int, level log.Level) {

	fields := log.Fields{
		"kind":              TimingInfo,
		"operation":         operation,
		"dataStructureName": dataStructureName,
		"tookMs":            tookMs,
	}

	lp.doLog(fmt.Sprintf("'%s' took %d ms", operation, tookMs), fields, level)

}

func (lp *LogProvider) LogInternalStateEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": InternalStateInfo,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogHzEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": HzError,
	}

	lp.doLog(msg, fields, level)
}

func (lp *LogProvider) LogErrUponConfigExtraction(keyPath string, err error, level log.Level) {

	lp.LogConfigEvent(keyPath, "config file", fmt.Sprintf("will use default for property due to error: %s", err), level)

}

func (lp *LogProvider) LogConfigEvent(configValue string, source string, msg string, level log.Level) {

	fields := log.Fields{
		"kind":   ConfigurationError,
		"value":  configValue,
		"source": source,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) doLog(msg string, fields log.Fields, level log.Level) {

	fields["caller"] = getCaller()
	fields["client"] = lp.ClientID

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

func getCaller() string {

	// Skipping three stacks will bring us to the method or function that originally invoked the logging method
	pc, _, _, ok := runtime.Caller(3)

	if !ok {
		return "unknown"
	}

	file, line := runtime.FuncForPC(pc).FileLine(pc)
	return fmt.Sprintf("%s:%d", file, line)

}
