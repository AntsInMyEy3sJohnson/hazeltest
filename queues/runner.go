package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	QueueTester struct {
		HzCluster string
		HzMembers []string
	}
	runner interface {
		runQueueTests(hzCluster string, hzMembers []string)
	}
	configPropertyAssigner interface {
		Assign(string, func(string, any) error) error
	}
	runnerConfig struct {
		enabled                     bool
		numQueues                   int
		queueBaseName               string
		appendQueueIndexToQueueName bool
		appendClientIdToQueueName   bool
		useQueuePrefix              bool
		queuePrefix                 string
		putConfig                   *operationConfig
		pollConfig                  *operationConfig
	}
	operationConfig struct {
		enabled                   bool
		numRuns                   uint32
		batchSize                 int
		initialDelay              *sleepConfig
		sleepBetweenActionBatches *sleepConfig
		sleepBetweenRuns          *sleepConfig
	}
	sleepConfig struct {
		enabled    bool
		durationMs int
	}
	runnerConfigBuilder struct {
		runnerKeyPath string
		queueBaseName string
	}
	state string
)

const (
	start                  state = "start"
	populateConfigComplete state = "populateConfigComplete"
	checkEnabledComplete   state = "checkEnabledComplete"
	raiseReadyComplete     state = "raiseReadyComplete"
	testLoopStart          state = "testLoopStart"
	testLoopComplete       state = "testLoopComplete"
)

const (
	templateBoolParseError        = "%s: unable to parse %v to bool"
	templateIntParseError         = "%s: unable to parse %v to int"
	templateStringParseError      = "%s: unable to parse %v into string"
	templateNumberAtLeastOneError = "%s: expected number to be at least 1, got %d"
)

var (
	runners          []runner
	lp               *logging.LogProvider
	propertyAssigner configPropertyAssigner
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
	propertyAssigner = client.DefaultConfigPropertyAssigner{}
}

func (b runnerConfigBuilder) populateConfig() (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				enabled = b
				return nil
			}
		})
	})

	var numQueues int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numQueues", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				numQueues = i
				return nil
			}
		})
	})

	var appendQueueIndexToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendQueueIndexToQueueName", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				appendQueueIndexToQueueName = b
				return nil
			}
		})
	})

	var appendClientIdToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendClientIdToQueueName", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				appendClientIdToQueueName = b
				return nil
			}
		})
	})

	var useQueuePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				useQueuePrefix = b
				return nil
			}
		})
	})

	var queuePrefix string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.prefix", func(path string, a any) error {
			if s, ok := a.(string); !ok {
				return fmt.Errorf(templateStringParseError, path, a)
			} else if len(s) == 0 {
				return fmt.Errorf("%s: expected non-empty string", path)
			} else {
				queuePrefix = s
				return nil
			}
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	putConfig, err := b.populateOperationConfig("put")
	if err != nil {
		return nil, err
	}
	pollConfig, err := b.populateOperationConfig("poll")
	if err != nil {
		return nil, err
	}

	return &runnerConfig{
		enabled:                     enabled,
		numQueues:                   numQueues,
		queueBaseName:               b.queueBaseName,
		appendQueueIndexToQueueName: appendQueueIndexToQueueName,
		appendClientIdToQueueName:   appendClientIdToQueueName,
		useQueuePrefix:              useQueuePrefix,
		queuePrefix:                 queuePrefix,
		putConfig:                   putConfig,
		pollConfig:                  pollConfig,
	}, nil

}

func (b runnerConfigBuilder) populateOperationConfig(operation string) (*operationConfig, error) {

	c := b.runnerKeyPath + "." + fmt.Sprintf("%sConfig", operation)

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".enabled", func(path string, a any) error {
			if b, ok := a.(bool); !ok {
				return fmt.Errorf(templateBoolParseError, path, a)
			} else {
				enabled = b
				return nil
			}
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".numRuns", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				numRuns = uint32(i)
				return nil
			}
		})
	})

	var batchSizePoll int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".batchSize", func(path string, a any) error {
			if i, ok := a.(int); !ok {
				return fmt.Errorf(templateIntParseError, path, a)
			} else if i <= 0 {
				return fmt.Errorf(templateNumberAtLeastOneError, path, i)
			} else {
				batchSizePoll = i
				return nil
			}
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	initialDelay, err := b.populateSleepConfig(c + ".sleeps.initialDelay")
	if err != nil {
		return nil, err
	}

	sleepBetweenActionBatches, err := b.populateSleepConfig(c + ".sleeps.betweenActionBatches")
	if err != nil {
		return nil, err
	}

	sleepBetweenRuns, err := b.populateSleepConfig(c + ".sleeps.betweenRuns")
	if err != nil {
		return nil, err
	}

	return &operationConfig{
		enabled:                   enabled,
		numRuns:                   numRuns,
		batchSize:                 batchSizePoll,
		initialDelay:              initialDelay,
		sleepBetweenActionBatches: sleepBetweenActionBatches,
		sleepBetweenRuns:          sleepBetweenRuns,
	}, nil

}

func (b runnerConfigBuilder) populateSleepConfig(configBasePath string) (*sleepConfig, error) {

	var enabled bool
	if err := propertyAssigner.Assign(configBasePath+".enabled", func(path string, a any) error {
		if b, ok := a.(bool); !ok {
			return fmt.Errorf(templateBoolParseError, path, a)
		} else {
			enabled = b
			return nil

		}
	}); err != nil {
		return nil, err
	}

	var durationMs int
	if err := propertyAssigner.Assign(configBasePath+".durationMs", func(path string, a any) error {
		if i, ok := a.(int); !ok {
			return fmt.Errorf(templateIntParseError, path, a)
		} else if i <= 0 {
			return fmt.Errorf(templateNumberAtLeastOneError, path, i)
		} else {
			durationMs = i
			return nil
		}
	}); err != nil {
		return nil, err
	}

	return &sleepConfig{enabled, durationMs}, nil

}

func populateConfig(runnerKeyPath string, queueBaseName string) (*runnerConfig, error) {

	return runnerConfigBuilder{
		runnerKeyPath,
		queueBaseName,
	}.populateConfig()

}

func (t *QueueTester) TestQueues() {

	clientID := client.ID()
	logInternalStateInfo(fmt.Sprintf("%s: queuetester starting %d runner/-s", clientID, len(runners)))

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := runners[i]
			runner.runQueueTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}

func logInternalStateInfo(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.InternalStateEvent,
		"client": client.ID(),
	}).Trace(msg)

}
