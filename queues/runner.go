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
		enabled          bool
		durationMs       int
		enableRandomness bool
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

var (
	runners          []runner
	lp               *logging.LogProvider
	propertyAssigner client.ConfigPropertyAssigner
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
		return propertyAssigner.Assign(b.runnerKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numQueues int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".numQueues", client.ValidateInt, func(a any) {
			numQueues = a.(int)
		})
	})

	var appendQueueIndexToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendQueueIndexToQueueName", client.ValidateBool, func(a any) {
			appendQueueIndexToQueueName = a.(bool)
		})
	})

	var appendClientIdToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".appendClientIdToQueueName", client.ValidateBool, func(a any) {
			appendClientIdToQueueName = a.(bool)
		})
	})

	var useQueuePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.enabled", client.ValidateBool, func(a any) {
			useQueuePrefix = a.(bool)
		})
	})

	var queuePrefix string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.runnerKeyPath+".queuePrefix.prefix", client.ValidateString, func(a any) {
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
		return propertyAssigner.Assign(c+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".numRuns", client.ValidateInt, func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var batchSizePoll int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(c+".batchSize", client.ValidateInt, func(a any) {
			batchSizePoll = a.(int)
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
	if err := propertyAssigner.Assign(configBasePath+".enabled", client.ValidateBool, func(a any) {
		enabled = a.(bool)
	}); err != nil {
		return nil, err
	}

	var durationMs int
	if err := propertyAssigner.Assign(configBasePath+".durationMs", client.ValidateInt, func(a any) {
		durationMs = a.(int)
	}); err != nil {
		return nil, err
	}

	var enableRandomness bool
	if err := propertyAssigner.Assign(configBasePath+".enableRandomness", client.ValidateBool, func(a any) {
		enableRandomness = a.(bool)
	}); err != nil {
		return nil, err
	}

	return &sleepConfig{enabled, durationMs, enableRandomness}, nil

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
		"kind":   logging.RunnerEvent,
		"client": client.ID(),
	}).Trace(msg)

}
