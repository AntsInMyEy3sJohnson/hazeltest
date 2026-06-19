package logging

import (
	"fmt"
	"hash/fnv"
	"runtime"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	ApiEvent              = "api event"
	RunnerEvent           = "runner event"
	StateCleanerEvent     = "state cleaner event"
	ChaosMonkeyEvent      = "chaos monkey event"
	TimingEvent           = "timing event"
	IoEvent               = "io event"
	HzEvent               = "hazelcast event"
	ConfigurationEvent    = "configuration event"
	InternalStateEvent    = "internal state event"
	PayloadGeneratorEvent = "payload generator event"
)

var loggers = make(map[uint64]*LogProvider)

type LogProvider struct {
	ClientID  uuid.UUID
	logger    *zap.Logger
	component string
}

func GetLogProviderInstance(clientID uuid.UUID, component string) (*LogProvider, error) {

	h := fnv.New64a()

	if _, err := h.Write([]byte(clientID.String() + component)); err != nil {
		return nil, err
	}

	sum := h.Sum64()

	if _, ok := loggers[sum]; !ok {
		logger, err := zap.NewProduction()
		if err != nil {
			return nil, err
		}
		loggers[sum] = &LogProvider{
			ClientID:  clientID,
			logger:    logger,
			component: component,
		}
	}

	return loggers[sum], nil

}

func (lp *LogProvider) LogPayloadGeneratorEvent(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(PayloadGeneratorEvent))

}

func (lp *LogProvider) LogIoEvent(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(IoEvent))

}

func (lp *LogProvider) LogApiEvent(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(ApiEvent))

}

func (lp *LogProvider) LogInternalStateInfo(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(InternalStateEvent))

}

func (lp *LogProvider) LogTimingEvent(operation string, dataStructureName, dataStructureKind string, tookMs int64, level zapcore.Level) {

	lp.doLog(
		fmt.Sprintf("'%s' took %d ms", operation, tookMs),
		level,
		assembleLogMessageKindField(TimingEvent),
		zap.String("operation", operation),
		zap.String("dataStructureName", dataStructureName),
		zap.String("dataStructureKind", dataStructureKind),
		zap.Int64("tookMs", tookMs),
	)

}

func (lp *LogProvider) LogChaosMonkeyEvent(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(ChaosMonkeyEvent))

}

func (lp *LogProvider) LogStateCleanerEvent(msg, hzService string, level zapcore.Level) {

	lp.doLog(
		msg,
		level,
		assembleLogMessageKindField(StateCleanerEvent),
		zap.String("hzService", hzService),
	)

}

func (lp *LogProvider) LogMapRunnerEvent(msg, runnerName string, level zapcore.Level) {

	lp.doLog(
		msg,
		level,
		assembleLogMessageKindField(RunnerEvent),
		assembleRunnerNameField(runnerName),
		assembleRunnerKindField("map"),
	)

}

func (lp *LogProvider) LogQueueRunnerEvent(msg, runnerName string, level zapcore.Level) {

	lp.doLog(
		msg,
		level,
		assembleLogMessageKindField(RunnerEvent),
		assembleRunnerNameField(runnerName),
		assembleRunnerKindField("queue"),
	)

}

func (lp *LogProvider) LogHzEvent(msg string, level zapcore.Level) {

	lp.doLog(msg, level, assembleLogMessageKindField(HzEvent))

}

func (lp *LogProvider) LogErrUponConfigRetrieval(keyPath string, err error, level zapcore.Level) {

	lp.LogConfigEvent(
		keyPath,
		"config file",
		fmt.Sprintf("encountered error upon attempt to extract config value: %v", err),
		level,
	)

}

func (lp *LogProvider) LogConfigEvent(configValue string, source string, msg string, level zapcore.Level) {

	lp.doLog(
		msg,
		level,
		assembleLogMessageKindField(ConfigurationEvent),
		zap.String("value", configValue),
		zap.String("source", source),
	)

}

func (lp *LogProvider) doLog(msg string, level zapcore.Level, fields ...zapcore.Field) {

	fieldCaller := zap.String("caller", getCaller())
	fieldClient := zap.String("client", lp.ClientID.String())
	fieldComponent := zap.String("component", lp.component)

	enrichedFields := append([]zapcore.Field{fieldCaller, fieldClient, fieldComponent}, fields...)

	if level == zapcore.FatalLevel {
		lp.logger.Fatal(msg, enrichedFields...)
	} else if level == zapcore.ErrorLevel {
		lp.logger.Error(msg, enrichedFields...)
	} else if level == zapcore.WarnLevel {
		lp.logger.Warn(msg, enrichedFields...)
	} else if level == zapcore.InfoLevel {
		lp.logger.Info(msg, enrichedFields...)
	} else {
		lp.logger.Debug(msg, enrichedFields...)
	}

}

func assembleRunnerKindField(runnerKind string) zap.Field {

	return zap.String("runnerKind", runnerKind)

}

func assembleRunnerNameField(runnerName string) zap.Field {

	return zap.String("runnerName", runnerName)

}

func assembleLogMessageKindField(kind string) zap.Field {

	return zap.String("kind", kind)

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
