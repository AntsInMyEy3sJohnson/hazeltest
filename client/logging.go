package client

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logEventKind string

const (
	ApiEvent              logEventKind = "apiEvent"
	RunnerEvent           logEventKind = "runnerEvent"
	StateCleanerEvent     logEventKind = "stateCleanerEvent"
	ChaosMonkeyEvent      logEventKind = "chaosMonkeyEvent"
	TimingEvent           logEventKind = "timingEvent"
	IoEvent               logEventKind = "ioEvent"
	HzEvent               logEventKind = "hazelcastEvent"
	ConfigurationEvent    logEventKind = "configurationEvent"
	InternalStateEvent    logEventKind = "internalStateEvent"
	PayloadGeneratorEvent logEventKind = "payloadGeneratorEvent"
)

var (
	loggingConfigNotSourcedError = errors.New("logging config hasn't been sourced yet")
	emptyLoggingConfigError      = errors.New("encountered empty logging config")
)

var (
	loggers       = make(map[string]*LogProvider)
	loggingConfig map[string]any
	logLevels     *loggingLevelsConfig
	m             sync.Mutex
)

type (
	LogProvider struct {
		ClientID                             uuid.UUID
		logger                               *zap.Logger
		component                            string
		eventLevels                          map[logEventKind]zapcore.Level
		componentWithoutExplicitLoggingLevel bool
	}
	loggingLevelsConfig struct {
		rootConfig       zapcore.Level
		componentsConfig map[string]*loggingComponentConfig
	}
	loggingComponentConfig struct {
		eventLevels map[logEventKind]zapcore.Level
	}
)

func InitLoggingComponents() error {

	if loggingConfig == nil {
		return loggingConfigNotSourcedError
	}

	if len(loggingConfig) == 0 {
		return emptyLoggingConfigError
	}

	componentsKeyPath := "logging.level.components"
	components, err := retrieveConfigValueFromMap(loggingConfig, componentsKeyPath)

	if err != nil {
		return err
	}

	componentsMap, ok := components.(map[string]any)
	if !ok {
		return fmt.Errorf("encountered malformed logging config for keypath '%s'; expected 'map[string]any'", componentsKeyPath)
	}

	componentsConfig := make(map[string]*loggingComponentConfig)

	for component, levels := range componentsMap {
		eventLevels := make(map[logEventKind]zapcore.Level)

		eventsToLevel, ok := levels.(map[string]any)

		if !ok {
			return fmt.Errorf("encountered malformed logging config for keypath '%s.%s'; expected nested dict mapping log events to log levels", componentsKeyPath, component)
		}

		for k, v := range eventsToLevel {
			// What happens if I pass an event that doesn't exist?
			zapLevel := asInternalLoggingLevel(v)
			if zapLevel == zapcore.InvalidLevel {
				return fmt.Errorf("encountered invalid logging level '%s' at keypath '%s.%s.%s'; must be one of 'DEBUG', 'INFO', 'WARN', or 'ERROR'", v, componentsKeyPath, component, k)
			}
			eventLevels[logEventKind(k)] = zapLevel
		}

		componentsConfig[component] = &loggingComponentConfig{eventLevels: eventLevels}

	}

	rootLevelKeyPath := "logging.level.root"
	level, err := retrieveConfigValueFromMap(loggingConfig, rootLevelKeyPath)

	if err != nil {
		return err
	}

	levelInternal := asInternalLoggingLevel(level)

	if levelInternal == zapcore.InvalidLevel {
		return fmt.Errorf("encountered invalid logging level '%s' at keypath '%s'; must be one of 'DEBUG', 'INFO', 'WARN', or 'ERROR'", level, rootLevelKeyPath)
	}

	logLevels = &loggingLevelsConfig{
		rootConfig:       levelInternal,
		componentsConfig: componentsConfig,
	}

	return nil

}

func getEventLevelsByComponent(component string) map[logEventKind]zapcore.Level {

	if logLevels == nil {
		return nil
	}

	if v, ok := logLevels.componentsConfig[component]; ok {
		return v.eventLevels
	}

	return nil

}

func GetLogProviderInstance(clientID uuid.UUID, component string) (*LogProvider, error) {

	defer m.Unlock()

	m.Lock()
	if _, ok := loggers[component]; !ok {
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		logger, err := config.Build()
		if err != nil {
			return nil, err
		}
		loggers[component] = &LogProvider{
			ClientID:    clientID,
			logger:      logger,
			component:   component,
			eventLevels: getEventLevelsByComponent(component),
		}
	}

	return loggers[component], nil

}

func asInternalLoggingLevel(level any) zapcore.Level {

	switch level {
	case "DEBUG":
		return zapcore.DebugLevel
	case "INFO":
		return zapcore.InfoLevel
	case "WARN":
		return zapcore.WarnLevel
	case "ERROR":
		return zapcore.ErrorLevel
	default:
		return zapcore.InvalidLevel
	}

}

func (lp *LogProvider) Log(msg string, eventKind logEventKind, level zapcore.Level) {

	lp.doLog(msg, eventKind, level)

}

func (lp *LogProvider) LogTimingEvent(operation string, dataStructureName, dataStructureKind string, tookMs int64, level zapcore.Level) {

	lp.doLog(
		fmt.Sprintf("'%s' took %d ms", operation, tookMs),
		TimingEvent,
		level,
		zap.String("operation", operation),
		zap.String("dataStructureName", dataStructureName),
		zap.String("dataStructureKind", dataStructureKind),
		zap.Int64("tookMs", tookMs),
	)

}

func (lp *LogProvider) LogStateCleanerEvent(msg, hzService string, level zapcore.Level) {

	lp.doLog(
		msg,
		StateCleanerEvent,
		level,
		zap.String("hzService", hzService),
	)

}

func (lp *LogProvider) LogMapRunnerEvent(msg, runnerName string, level zapcore.Level) {

	lp.doLog(
		msg,
		RunnerEvent,
		level,
		assembleRunnerNameField(runnerName),
		assembleRunnerKindField("map"),
	)

}

func (lp *LogProvider) LogQueueRunnerEvent(msg, runnerName string, level zapcore.Level) {

	lp.doLog(
		msg,
		RunnerEvent,
		level,
		assembleRunnerNameField(runnerName),
		assembleRunnerKindField("queue"),
	)

}

func (lp *LogProvider) LogConfigEvent(configValue string, source string, msg string, level zapcore.Level) {

	lp.doLog(
		msg,
		ConfigurationEvent,
		level,
		zap.String("value", configValue),
		zap.String("source", source),
	)

}

func (lp *LogProvider) sourceEventLevels() {

	if lp.componentWithoutExplicitLoggingLevel || lp.eventLevels != nil {
		return
	}

	eventLevels := getEventLevelsByComponent(lp.component)
	if eventLevels == nil {
		lp.componentWithoutExplicitLoggingLevel = true
	} else {
		lp.eventLevels = eventLevels
	}

}

func (lp *LogProvider) evaluateLogLevel(event logEventKind) zapcore.Level {

	if v, ok := lp.eventLevels[event]; ok {
		return v
	}

	if logLevels != nil {
		return logLevels.rootConfig
	}

	return zapcore.InfoLevel

}

func (lp *LogProvider) doLog(msg string, eventKind logEventKind, msgLevel zapcore.Level, fields ...zapcore.Field) {

	lp.sourceEventLevels()

	if msgLevel < lp.evaluateLogLevel(eventKind) {
		return
	}

	fieldCaller := zap.String("caller", getCaller())
	fieldClient := zap.String("client", lp.ClientID.String())
	fieldComponent := zap.String("component", lp.component)
	fieldKind := zap.String("eventKind", string(eventKind))

	enrichedFields := append([]zapcore.Field{fieldCaller, fieldClient, fieldComponent, fieldKind}, fields...)

	switch msgLevel {
	case zapcore.FatalLevel:
		lp.logger.Fatal(msg, enrichedFields...)
	case zapcore.ErrorLevel:
		lp.logger.Error(msg, enrichedFields...)
	case zapcore.WarnLevel:
		lp.logger.Warn(msg, enrichedFields...)
	case zapcore.InfoLevel:
		lp.logger.Info(msg, enrichedFields...)
	default:
		lp.logger.Debug(msg, enrichedFields...)
	}

}

func assembleRunnerKindField(runnerKind string) zap.Field {

	return zap.String("runnerKind", runnerKind)

}

func assembleRunnerNameField(runnerName string) zap.Field {

	return zap.String("runnerName", runnerName)

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
