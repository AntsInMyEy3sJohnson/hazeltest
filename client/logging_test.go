package client

import (
	"errors"
	"testing"
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

		t.Log("\t\tgiven a populated logging config")
		{
			t.Log("\t\t\twhen logging config content is valid")
			{
				loggingConfig = map[string]any{
					"logging": map[string]any{
						"level": map[string]any{
							"root": "INFO",
							"components": map[string]any{
								"mapRunner": map[string]any{
									"runnerEvent":    "INFO",
									"ioEvent":        "WARN",
									"hazelcastEvent": "WARN",
									"timingEvent":    "DEBUG",
								},
								"queueRunner": map[string]any{
									"runnerEvent": "WARN",
									"timingEvent": "DEBUG",
								},
							},
						},
					},
				}
				err := InitLoggingComponents()

				msg := "\t\t\t\tno error must be returned"

				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\t\tlog levels must have been initialized"

				// Given logging config contains event-to-log-level configuration for
				// two components
				if len(logLevels.componentsConfig) == 2 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}
		}

	}

}
