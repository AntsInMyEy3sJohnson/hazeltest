package chaos

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type (
	hzMember struct {
		identifier string
	}
	hzMemberChooser interface {
		choose() (hzMember, error)
	}
	hzMemberKiller interface {
		kill(member hzMember) error
	}
	monkey interface {
		causeChaos()
	}
	monkeyConfigBuilder struct {
		monkeyKeyPath string
	}
	monkeyConfig struct {
		enabled                     bool
		sleepTimeSeconds            int
		chaosProbability            float32
		gracePeriodSeconds          int
		gracePeriodEnableRandomness bool
	}
)

var (
	monkeys          []monkey
	propertyAssigner client.ConfigPropertyAssigner
	lp               *logging.LogProvider
)

func init() {
	lp = &logging.LogProvider{ClientID: client.ID()}
	propertyAssigner = client.DefaultConfigPropertyAssigner{}
}

func register(m monkey) {
	monkeys = append(monkeys, m)
}

func (b monkeyConfigBuilder) populateConfig() (*monkeyConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var sleepTimeSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".sleepTimeSeconds", client.ValidateInt, func(a any) {
			sleepTimeSeconds = a.(int)
		})
	})

	var chaosProbability float32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".chaosProbability", client.ValidatePercentage, func(a any) {
			chaosProbability = a.(float32)
		})
	})

	var memberGracePeriodSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".memberGrace.periodSeconds", client.ValidateInt, func(a any) {
			memberGracePeriodSeconds = a.(int)
		})
	})

	var memberGraceEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".memberGrace.enableRandomness", client.ValidateBool, func(a any) {
			memberGraceEnableRandomness = a.(bool)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &monkeyConfig{
		enabled:                     enabled,
		sleepTimeSeconds:            sleepTimeSeconds,
		chaosProbability:            chaosProbability,
		gracePeriodSeconds:          memberGracePeriodSeconds,
		gracePeriodEnableRandomness: memberGraceEnableRandomness,
	}, nil

}

func RunMonkeys() {

	clientID := client.ID()
	lp.LogInternalStateEvent(fmt.Sprintf("%s: starting %d chaos monkey/-s", clientID, len(monkeys)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(monkeys); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := monkeys[i]
			m.causeChaos()
		}(i)
	}

	wg.Wait()

}
