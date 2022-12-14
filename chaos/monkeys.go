package chaos

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"math/rand"
	"sync"
	"time"
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
		init(c hzMemberChooser, k hzMemberKiller)
		causeChaos()
	}
	memberKillerMonkey struct {
		chooser hzMemberChooser
		killer  hzMemberKiller
	}
	monkeyConfigBuilder struct {
		monkeyKeyPath string
	}
	memberAccessConfig struct {
		mode   string
		config map[string]any
	}
	sleepConfig struct {
		enabled          bool
		durationSeconds  int
		enableRandomness bool
	}
	monkeyConfig struct {
		enabled                 bool
		stopWhenRunnersFinished bool
		chaosProbability        float32
		accessConfig            *memberAccessConfig
		sleep                   *sleepConfig
		memberGrace             *sleepConfig
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
	register(&memberKillerMonkey{})
}

func register(m monkey) {
	monkeys = append(monkeys, m)
}

func (m *memberKillerMonkey) init(c hzMemberChooser, k hzMemberKiller) {

	m.chooser = c
	m.killer = k

}

func (m *memberKillerMonkey) causeChaos() {

	// TODO Add state transitions
	mc, err := populateMemberKillerMonkeyConfig()
	if err != nil {
		lp.LogInternalStateEvent("unable to populate config for member killer chaos monkey -- aborting", log.ErrorLevel)
	}

	if !mc.enabled {
		lp.LogInternalStateEvent("member killer monkey not enabled -- won't run", log.InfoLevel)
		return
	}

	// TODO Make API readiness dependent on chaos monkey state?

	for {
		sleep(mc.sleep)

		f := rand.Float32()
		if f <= mc.chaosProbability {
			member, err := m.chooser.choose()
			if err != nil {
				lp.LogInternalStateEvent("unable to choose hazelcast member to kill -- will try again in next iteration", log.WarnLevel)
				continue
			}

			err = m.killer.kill(member)
			if err != nil {
				lp.LogInternalStateEvent(fmt.Sprintf("unable to kill chosen hazelcast member '%s' -- will try again in next iteration", member.identifier), log.WarnLevel)
			}
		}
	}

}

func sleep(sc *sleepConfig) {

	if sc.enabled {
		var sleepDuration int
		if sc.enableRandomness {
			sleepDuration = rand.Intn(sc.durationSeconds + 1)
		} else {
			sleepDuration = sc.durationSeconds
		}
		lp.LogInternalStateEvent(fmt.Sprintf("sleeping for '%d' seconds", sleepDuration), log.TraceLevel)
		time.Sleep(time.Duration(sleepDuration) * time.Second)
	}

}

func populateMemberKillerMonkeyConfig() (*monkeyConfig, error) {

	monkeyKeyPath := "chaosMonkeys.memberKiller"

	configBuilder := monkeyConfigBuilder{monkeyKeyPath: monkeyKeyPath}

	return configBuilder.populateConfig()

}

func (b monkeyConfigBuilder) populateConfig() (*monkeyConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var stopWhenRunnersFinished bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".stopWhenRunnersFinished", client.ValidateBool, func(a any) {
			stopWhenRunnersFinished = a.(bool)
		})
	})

	var chaosProbability float32
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".chaosProbability", client.ValidatePercentage, func(a any) {
			chaosProbability = a.(float32)
		})
	})

	var sleepEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".sleep.enabled", client.ValidateBool, func(a any) {
			sleepEnabled = a.(bool)
		})
	})

	var sleepDurationSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".sleep.durationSeconds", client.ValidateInt, func(a any) {
			sleepDurationSeconds = a.(int)
		})
	})

	var sleepEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".sleep.enableRandomness", client.ValidateBool, func(a any) {
			sleepEnableRandomness = a.(bool)
		})
	})

	var memberGraceEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".memberGrace.enabled", client.ValidateBool, func(a any) {
			memberGraceEnabled = a.(bool)
		})
	})

	var memberGraceDurationSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".memberGrace.durationSeconds", client.ValidateInt, func(a any) {
			memberGraceDurationSeconds = a.(int)
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
		enabled:                 enabled,
		stopWhenRunnersFinished: stopWhenRunnersFinished,
		chaosProbability:        chaosProbability,
		sleep: &sleepConfig{
			enabled:          sleepEnabled,
			durationSeconds:  sleepDurationSeconds,
			enableRandomness: sleepEnableRandomness,
		},
		memberGrace: &sleepConfig{
			enabled:          memberGraceEnabled,
			durationSeconds:  memberGraceDurationSeconds,
			enableRandomness: memberGraceEnableRandomness,
		},
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
			// The only mode for accessing hazelcast members is currently through kubernetes, and as long as that's the
			// case, we can safely hard-code the member chooser and member killer
			m.init(&k8sHzMemberChooser{}, &k8sHzMemberKiller{})
			m.causeChaos()
		}(i)
	}

	wg.Wait()

}
