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

const ApiEvent = "api event"
const RunnerEvent = "runner event"
const StateCleanerEvent = "state cleaner event"
const ChaosMonkeyEvent = "chaos monkey event"
const TimingEvent = "timing event"
const IoEvent = "io event"
const HzEvent = "hazelcast event"
const ConfigurationEvent = "configuration event"
const InternalStateEvent = "internal state event"
const PayloadGeneratorEvent = "payload generator event"

type LogProvider struct {
	ClientID uuid.UUID
}

var lp *LogProvider

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
	log.SetReportCaller(false)

}

func GetLogProviderInstance(clientID uuid.UUID) *LogProvider {

	if lp == nil {
		lp = &LogProvider{ClientID: clientID}
	}

	return lp

}

func (lp *LogProvider) LogPayloadGeneratorEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": PayloadGeneratorEvent,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogIoEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": IoEvent,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogApiEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": ApiEvent,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogInternalStateInfo(msg string, level log.Level) {

	fields := log.Fields{
		"kind": InternalStateEvent,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogTimingEvent(operation string, dataStructureName string, tookMs int, level log.Level) {

	fields := log.Fields{
		"kind":              TimingEvent,
		"operation":         operation,
		"dataStructureName": dataStructureName,
		"tookMs":            tookMs,
	}

	lp.doLog(fmt.Sprintf("'%s' took %d ms", operation, tookMs), fields, level)

}

func (lp *LogProvider) LogChaosMonkeyEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": ChaosMonkeyEvent,
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogStateCleanerEvent(msg, hzService string, level log.Level) {
	fields := log.Fields{
		"kind":      StateCleanerEvent,
		"hzService": hzService,
	}

	lp.doLog(msg, fields, level)
}

func (lp *LogProvider) LogMapRunnerEvent(msg, runnerName string, level log.Level) {

	fields := log.Fields{
		"kind":       RunnerEvent,
		"runnerName": runnerName,
		"runnerKind": "map",
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogQueueRunnerEvent(msg, runnerName string, level log.Level) {

	fields := log.Fields{
		"kind":       RunnerEvent,
		"runnerName": runnerName,
		"runnerKind": "queue",
	}

	lp.doLog(msg, fields, level)

}

func (lp *LogProvider) LogHzEvent(msg string, level log.Level) {

	fields := log.Fields{
		"kind": HzEvent,
	}

	lp.doLog(msg, fields, level)
}

func (lp *LogProvider) LogErrUponConfigRetrieval(keyPath string, err error, level log.Level) {

	lp.LogConfigEvent(keyPath, "config file", fmt.Sprintf("encountered error upon attempt to extract config value: %v", err), level)

}

func (lp *LogProvider) LogConfigEvent(configValue string, source string, msg string, level log.Level) {

	fields := log.Fields{
		"kind":   ConfigurationEvent,
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
	} else if level == log.ErrorLevel {
		log.WithFields(fields).Error(msg)
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
