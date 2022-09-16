package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	runner interface {
		runMapTests(hzCluster string, hzMembers []string)
	}
	configPropertyAssigner interface {
		Assign(string, func(any)) error
	}
	runnerConfig struct {
		enabled                   bool
		numMaps                   int
		numRuns                   int
		mapBaseName               string
		useMapPrefix              bool
		mapPrefix                 string
		appendMapIndexToMapName   bool
		appendClientIdToMapName   bool
		sleepBetweenActionBatches *sleepConfig
		sleepBetweenRuns          *sleepConfig
	}
	sleepConfig struct {
		enabled    bool
		durationMs int
	}
	runnerConfigBuilder struct {
		runnerKeyPath string
		mapBaseName   string
	}
	MapTester struct {
		HzCluster string
		HzMembers []string
	}
)

var runners []runner

func register(r runner) {
	runners = append(runners, r)
}

var lp *logging.LogProvider

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func (b runnerConfigBuilder) populateConfig(a configPropertyAssigner) (*runnerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".enabled", func(a any) {
			enabled = a.(bool)
		})
	})

	var numMaps int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".numMaps", func(a any) {
			numMaps = a.(int)
		})
	})

	var appendMapIndexToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".appendMapIndexToMapName", func(a any) {
			appendMapIndexToMapName = a.(bool)
		})
	})

	var appendClientIdToMapName bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".appendClientIdToMapName", func(a any) {
			appendClientIdToMapName = a.(bool)
		})
	})

	var numRuns int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".numRuns", func(a any) {
			numRuns = a.(int)
		})
	})

	var useMapPrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".mapPrefix.enabled", func(a any) {
			useMapPrefix = a.(bool)
		})
	})

	var mapPrefix string
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".mapPrefix.prefix", func(a any) {
			mapPrefix = a.(string)
		})
	})

	var sleepBetweenActionBatchesEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.enabled", func(a any) {
			sleepBetweenActionBatchesEnabled = a.(bool)
		})
	})

	var sleepBetweenActionBatchesDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".sleeps.betweenActionBatches.durationMs", func(a any) {
			sleepBetweenActionBatchesDurationMs = a.(int)
		})
	})

	var sleepBetweenRunsEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".sleeps.betweenRuns.enabled", func(a any) {
			sleepBetweenRunsEnabled = a.(bool)
		})
	})

	var sleepBetweenRunsDurationMs int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.runnerKeyPath+".sleeps.betweenRuns.durationMs", func(a any) {
			sleepBetweenRunsDurationMs = a.(int)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &runnerConfig{
		enabled:                   enabled,
		numMaps:                   numMaps,
		numRuns:                   numRuns,
		mapBaseName:               b.mapBaseName,
		useMapPrefix:              useMapPrefix,
		mapPrefix:                 mapPrefix,
		appendMapIndexToMapName:   appendMapIndexToMapName,
		appendClientIdToMapName:   appendClientIdToMapName,
		sleepBetweenActionBatches: &sleepConfig{sleepBetweenActionBatchesEnabled, sleepBetweenActionBatchesDurationMs},
		sleepBetweenRuns:          &sleepConfig{sleepBetweenRunsEnabled, sleepBetweenRunsDurationMs},
	}, nil

}

func (t *MapTester) TestMaps() {

	clientID := client.ID()
	lp.LogInternalStateEvent(fmt.Sprintf("%s: maptester starting %d runner/-s", clientID, len(runners)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := runners[i]
			runner.runMapTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}
