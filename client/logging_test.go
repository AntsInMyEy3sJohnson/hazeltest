package client

import (
	"errors"
	"fmt"
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestInitLoggingComponents(t *testing.T) {

	t.Log("given functionality for initializing logging levels per logging components and nested log event")
	{
		t.Log(oneTab + "when logging config is nil")
		{
			loggingConfig = nil
			err := InitLoggingComponents()

			msg := twoTabs + "error must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = twoTabs + "error must be of expected kind"
			if errors.Is(err, loggingConfigNotSourcedError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log(oneTab + "when logging config is empty")
		{
			loggingConfig = map[string]any{}
			err := InitLoggingComponents()

			msg := twoTabs + "error must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = twoTabs + "error must be of expected kind"
			if errors.Is(err, emptyLoggingConfigError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log(oneTab + "given a populated logging config")
		{
			t.Log(twoTabs + "when logging config content is valid")
			{
				mapRunnerComponent := "mapRunner"
				mapRunnerLoggingEvents := map[string]any{
					"runnerEvent":    "INFO",
					"ioEvent":        "WARN",
					"hazelcastEvent": "WARN",
					"timingEvent":    "DEBUG",
				}
				queueRunnerComponent := "queueRunner"
				queueRunnerLoggingEvents := map[string]any{
					"runnerEvent": "WARN",
					"timingEvent": "DEBUG",
				}
				loggingConfig = map[string]any{
					"logging": map[string]any{
						"level": map[string]any{
							"root": "INFO",
							"components": map[string]any{
								mapRunnerComponent:   mapRunnerLoggingEvents,
								queueRunnerComponent: queueRunnerLoggingEvents,
							},
						},
					},
				}
				err := InitLoggingComponents()

				msg := threeTabs + "no error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = threeTabs + "log levels must have been initialized"

				// Given logging config contains event-to-log-level configuration for
				// two components
				componentsConfig := logLevels.componentsConfig
				if len(componentsConfig) == 2 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				expectedLoggingComponentsConfig := map[string]map[string]any{
					mapRunnerComponent:   mapRunnerLoggingEvents,
					queueRunnerComponent: queueRunnerLoggingEvents,
				}

				msgNumElements := threeTabs + "assembled logging component config must contain expected number of elements"
				msgEventAndLevelMatch := threeTabs + "assembled logging component config must contain expected log-event-to-level mappings"
				for component, expectedConfig := range expectedLoggingComponentsConfig {
					assembledConfig := componentsConfig[component]

					if len(expectedConfig) == len(assembledConfig.eventLevels) {
						t.Log(msgNumElements, checkMark, component)
					} else {
						t.Fatal(msgNumElements, ballotX, component)
					}

					eventLevels := assembledConfig.eventLevels

					for expectedEvent, expectedLevel := range expectedConfig {

						if assembledLevel, ok := eventLevels[logEvent(expectedEvent)]; !ok {
							t.Fatal(msgEventAndLevelMatch, ballotX, component, expectedEvent)
						} else {
							expectedLevelString := expectedLevel.(string)
							var levelMatches func(actual zapcore.Level) bool
							switch expectedLevelString {
							case "DEBUG":
								levelMatches = func(actual zapcore.Level) bool {
									return actual == zapcore.DebugLevel
								}
							case "INFO":
								levelMatches = func(actual zapcore.Level) bool {
									return actual == zapcore.InfoLevel
								}
							case "WARN":
								levelMatches = func(actual zapcore.Level) bool {
									return actual == zapcore.WarnLevel
								}
							case "ERROR":
								levelMatches = func(actual zapcore.Level) bool {
									return actual == zapcore.ErrorLevel
								}
							default:
								t.Fatal(msg, ballotX, fmt.Sprintf("erroneous test setup; no support for expected logging level '%s'", expectedLevelString))
							}
							if levelMatches(assembledLevel) {
								t.Log(msg, checkMark, component, expectedEvent, assembledLevel)
							} else {
								t.Fatal(msg, ballotX, component, expectedEvent, assembledLevel)
							}
						}
					}
				}
			}
		}

	}

}
