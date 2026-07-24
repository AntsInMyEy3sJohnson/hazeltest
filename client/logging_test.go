package client

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
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
							"root": "WARN",
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

				msg = threeTabs + "root log level must have been initialized"
				if logLevels.rootConfig == zapcore.WarnLevel {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = threeTabs + "component log levels must have been initialized"

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

						if assembledLevel, ok := eventLevels[logEventKind(expectedEvent)]; !ok {
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

			t.Log(twoTabs + "when logging config content contains unsupported log level")
			{
				unsupportedLoggingLevel := "DPanicLevel"
				keyPathLogging := "logging"
				keyPathLevel := "level"
				keyPathComponents := "components"
				keyPathSomeComponent := "someComponent"
				keyPathSomeEvent := "someEvent"
				loggingConfig = map[string]any{
					keyPathLogging: map[string]any{
						keyPathLevel: map[string]any{
							"root": "INFO",
							keyPathComponents: map[string]any{
								keyPathSomeComponent: map[string]any{
									keyPathSomeEvent: "DPanicLevel",
								},
							},
						},
					},
				}
				err := InitLoggingComponents()

				msg := twoTabs + "error must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = twoTabs + "error message must contain information about given unsupported log level"
				errorMessage := err.Error()
				if strings.Contains(errorMessage, unsupportedLoggingLevel) {
					t.Log(msg, checkMark, errorMessage)
				} else {
					t.Fatal(msg, ballotX, errorMessage)
				}

				msg = twoTabs + "error message must contain path to key configured with log level in question"
				if strings.Contains(errorMessage, fmt.Sprintf("%s.%s.%s.%s.%s", keyPathLogging, keyPathLevel, keyPathComponents, keyPathSomeComponent, keyPathSomeEvent)) {
					t.Log(msg, checkMark, errorMessage)
				} else {
					t.Fatal(msg, ballotX, errorMessage)
				}
			}
		}
	}

}

func TestGetEventLevelsByComponent(t *testing.T) {

	t.Log("given functionality to look up event levels for a component")
	{
		t.Log(oneTab + "when log levels struct hasn't been initialized yet")
		{
			logLevels = nil
			levels := getEventLevelsByComponent("mapRunner")

			msg := twoTabs + "retrieved event levels map must be nil"

			if levels == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log(oneTab + "when log levels struct has been initialized, but doesn't contain entry for given component")
		{
			logLevels = &loggingLevelsConfig{componentsConfig: map[string]*loggingComponentConfig{
				"mapRunner": {
					eventLevels: nil,
				},
			}}

			levels := getEventLevelsByComponent("queueRunner")

			msg := twoTabs + "retrieved event levels map must be nil"

			if levels == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log(oneTab + "when log levels struct has been initialized and contains entry for given component")
		{
			component := "mapRunner"
			logLevels = &loggingLevelsConfig{componentsConfig: map[string]*loggingComponentConfig{
				component: {
					eventLevels: map[logEventKind]zapcore.Level{
						RunnerEvent: zapcore.InfoLevel,
					},
				},
			}}

			levels := getEventLevelsByComponent(component)

			msg := twoTabs + "corresponding entry must be returned"

			if v, ok := levels[RunnerEvent]; ok && v == zapcore.InfoLevel {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestAssembleLogProviderInstance(t *testing.T) {

	t.Log("given functionality to assemble a log provider instance for a given component")
	{
		t.Log(oneTab + "when actors from various components ask for logging providers concurrently")
		{
			components := []string{"mapRunner", "queueRunner", "chaosMonkey", "api", "hzClientAssembler"}

			numConcurrentActors := 5 * len(components)

			type assembledLogProviderWithComponent struct {
				lp        *LogProvider
				component string
			}

			lpChan := make(chan *assembledLogProviderWithComponent)
			errChan := make(chan error)

			var wg sync.WaitGroup
			for i := 0; i < numConcurrentActors; i++ {

				go func() {
					defer wg.Done()
					wg.Add(1)

					randomComponent := components[rand.Intn(len(components))]
					lpInstanceForComponent, err := AssembleLogProviderInstance(ID(), randomComponent)

					lpChan <- &assembledLogProviderWithComponent{lpInstanceForComponent, randomComponent}
					errChan <- err

				}()

			}

			msgLogProviderMustBeAssembled := twoTabs + "log provider must be assembled"

			assembledLogProviders := make(map[string]*LogProvider)

			go func() {
				defer wg.Done()
				wg.Add(1)

				for i := 0; i < numConcurrentActors; i++ {
					lpWithComponent := <-lpChan

					currentComponent := lpWithComponent.component
					if lpWithComponent != nil && currentComponent == lpWithComponent.lp.component {
						if _, ok := assembledLogProviders[currentComponent]; !ok {
							assembledLogProviders[currentComponent] = lpWithComponent.lp
						}
						t.Log(msgLogProviderMustBeAssembled, checkMark, currentComponent)
					} else {
						t.Error(msgLogProviderMustBeAssembled, ballotX, currentComponent)
						return
					}
				}
			}()

			msgNoErrorMustOccur := twoTabs + "no error must occur"

			go func() {

				for {
					select {
					case err := <-errChan:
						if err != nil {
							t.Error(msgNoErrorMustOccur, ballotX, err)
							return
						}
					}
				}

			}()

			wg.Wait()

			close(lpChan)
			close(errChan)

			t.Log(msgNoErrorMustOccur, checkMark)

			msg := twoTabs + "a log provider must have been assembled for every component"

			for _, c := range components {
				if _, ok := assembledLogProviders[c]; ok {
					t.Log(msg, checkMark, c)
				} else {
					t.Fatal(msg, ballotX, c)
				}
			}

		}

	}

}

func TestAsInternalLoggingLevel(t *testing.T) {

	t.Log("given functionality to turn a configuration-sourced property representing a log level to an internally usable log level")
	{
		t.Log(oneTab + "when a valid log level is provided")
		{
			givenToExpected := map[string]zapcore.Level{
				"DEBUG": zapcore.DebugLevel,
				"INFO":  zapcore.InfoLevel,
				"WARN":  zapcore.WarnLevel,
				"ERROR": zapcore.ErrorLevel,
			}

			msg := twoTabs + "expected internal log level must be returned"
			for k, v := range givenToExpected {
				actual := asInternalLoggingLevel(k)

				if actual == v {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", actual, v))
				}
			}
		}

		t.Log(oneTab + "when invalid log levels are provided")
		{
			invalid := []any{42, "blubb", "Gandalf", false, -1}

			msg := twoTabs + "log level representing invalid log level must be returned"
			expected := zapcore.InvalidLevel
			for _, v := range invalid {
				actual := asInternalLoggingLevel(v)

				if actual == expected {
					t.Log(msg, checkMark, fmt.Sprintf("%v -> %s", v, expected))
				} else {
					t.Fatal(msg, ballotX, fmt.Sprintf("%s != %s", actual, expected))
				}
			}

		}
	}

}
