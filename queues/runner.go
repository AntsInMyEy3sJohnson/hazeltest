package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/logging"
	"hazeltest/status"
	"sync"
)

type (
	QueueTester struct {
		HzCluster string
		HzMembers []string
	}
	runner interface {
		getSourceName() string
		runQueueTests(hzCluster string, hzMembers []string, gatherer *status.Gatherer, storeFunc initQueueStoreFunc)
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
		assigner      client.ConfigPropertyAssigner
		runnerKeyPath string
		queueBaseName string
	}
	state              string
	statusKey          string
	initQueueStoreFunc func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.QueueStore
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
	statusKeyCurrentState statusKey = "currentState"
)

var (
	runners               []runner
	lp                    *logging.LogProvider
	initDefaultQueueStore initQueueStoreFunc = func(ch hazelcastwrapper.HzClientHandler) hazelcastwrapper.QueueStore {
		return &hazelcastwrapper.DefaultQueueStore{Client: ch.GetClient()}
	}
)

func register(r runner) {
	runners = append(runners, r)
}

func init() {
	lp = logging.GetLogProviderInstance(client.ID())
}

func (b runnerConfigBuilder) populateConfig() (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numQueues int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".numQueues", client.ValidateInt, func(a any) {
			numQueues = a.(int)
		})
	})

	var appendQueueIndexToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".appendQueueIndexToQueueName", client.ValidateBool, func(a any) {
			appendQueueIndexToQueueName = a.(bool)
		})
	})

	var appendClientIdToQueueName bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".appendClientIdToQueueName", client.ValidateBool, func(a any) {
			appendClientIdToQueueName = a.(bool)
		})
	})

	var useQueuePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".queuePrefix.enabled", client.ValidateBool, func(a any) {
			useQueuePrefix = a.(bool)
		})
	})

	var queuePrefix string
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(b.runnerKeyPath+".queuePrefix.prefix", client.ValidateString, func(a any) {
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
		return b.assigner.Assign(c+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(c+".numRuns", client.ValidateInt, func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var batchSizePoll int
	assignmentOps = append(assignmentOps, func() error {
		return b.assigner.Assign(c+".batchSize", client.ValidateInt, func(a any) {
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
	if err := b.assigner.Assign(configBasePath+".enabled", client.ValidateBool, func(a any) {
		enabled = a.(bool)
	}); err != nil {
		return nil, err
	}

	var durationMs int
	if err := b.assigner.Assign(configBasePath+".durationMs", client.ValidateInt, func(a any) {
		durationMs = a.(int)
	}); err != nil {
		return nil, err
	}

	var enableRandomness bool
	if err := b.assigner.Assign(configBasePath+".enableRandomness", client.ValidateBool, func(a any) {
		enableRandomness = a.(bool)
	}); err != nil {
		return nil, err
	}

	return &sleepConfig{enabled, durationMs, enableRandomness}, nil

}

func populateConfig(assigner client.ConfigPropertyAssigner, runnerKeyPath string, queueBaseName string) (*runnerConfig, error) {

	return runnerConfigBuilder{
		assigner:      assigner,
		runnerKeyPath: runnerKeyPath,
		queueBaseName: queueBaseName,
	}.populateConfig()

}

func (t *QueueTester) TestQueues() {

	clientID := client.ID()
	lp.LogInternalStateInfo(fmt.Sprintf("%s: queue tester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			gatherer := status.NewGatherer()
			go gatherer.Listen()
			defer gatherer.StopListen()

			runner := runners[i]

			api.RegisterStatefulActor(api.QueueRunners, runner.getSourceName(), gatherer.AssembleStatusCopy)
			runner.runQueueTests(t.HzCluster, t.HzMembers, gatherer, initDefaultQueueStore)
		}(i)
	}

	wg.Wait()

}
