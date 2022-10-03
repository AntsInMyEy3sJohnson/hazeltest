package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runner interface {
		runQueueTests(hzCluster string, hzMembers []string)
	}
	configPropertyAssigner interface {
		Assign(string, func(any)) error
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
	QueueTester struct {
		HzCluster string
		HzMembers []string
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
		return propertyAssigner.Assign(b.runnerKeyPath+".enabled", func(a any) {
			enabled = a.(bool)
		})
	})

	var numQueues int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numQueues", func(a any) {
			numQueues = a.(int)
		})
	})

	var appendQueueIndexToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendQueueIndexToQueueName", func(a any) {
			appendQueueIndexToQueueName = a.(bool)
		})
	})

	var appendClientIdToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendClientIdToQueueName", func(a any) {
			appendClientIdToQueueName = a.(bool)
		})
	})

	var useQueuePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.enabled", func(a any) {
			useQueuePrefix = a.(bool)
		})
	})

	var queuePrefix string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.prefix", func(a any) {
			queuePrefix = a.(string)
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
		return propertyAssigner.Assign(c+".enabled", func(a any) {
			enabled = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".numRuns", func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var batchSizePoll int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".batchSize", func(a any) {
			batchSizePoll = a.(int)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &operationConfig{
		enabled:                   enabled,
		numRuns:                   numRuns,
		batchSize:                 batchSizePoll,
		initialDelay:              b.populateSleepConfig(c + ".sleeps.initialDelay"),
		sleepBetweenActionBatches: b.populateSleepConfig(c + ".sleeps.betweenActionBatches"),
		sleepBetweenRuns:          b.populateSleepConfig(c + ".sleeps.betweenRuns"),
	}, nil

}

func (b runnerConfigBuilder) populateSleepConfig(configBasePath string) *sleepConfig {

	keyPath := configBasePath + ".enabled"

	var enabled bool
	_ = propertyAssigner.Assign(keyPath, func(a any) {
		enabled = a.(bool)
	})

	keyPath = configBasePath + ".durationMs"
	var durationMs int
	_ = propertyAssigner.Assign(keyPath, func(a any) {
		durationMs = a.(int)
	})

	return &sleepConfig{enabled, durationMs}

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
