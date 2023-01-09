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
		choose(ac memberAccessConfig) (hzMember, error)
	}
	hzMemberKiller interface {
		kill(member hzMember, ac memberAccessConfig, memberGrace sleepConfig) error
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
	k8sOutOfClusterMemberAccess struct {
		kubeconfig, namespace, labelSelector string
	}
	k8sInClusterMemberAccess struct {
		labelSelector string
	}
	memberAccessConfig struct {
		memberAccessMode string
		targetOnlyActive bool
		k8sOutOfCluster  k8sOutOfClusterMemberAccess
		k8sInCluster     k8sInClusterMemberAccess
	}
	sleepConfig struct {
		enabled          bool
		durationSeconds  int
		enableRandomness bool
	}
	monkeyConfig struct {
		enabled                 bool
		stopWhenRunnersFinished bool
		chaosProbability        float64
		accessConfig            *memberAccessConfig
		sleep                   *sleepConfig
		memberGrace             *sleepConfig
	}
)

const (
	k8sOutOfClusterAccessMode = "k8sOutOfCluster"
	k8sInClusterAccessMode    = "k8sInCluster"
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

		f := rand.Float64()
		if f <= mc.chaosProbability {
			member, err := m.chooser.choose(*mc.accessConfig)
			if err != nil {
				var msg string
				if err == noMemberFoundError {
					msg = "no hazelcast member available to be killed -- will try again in next iteration"
				} else {
					msg = "unable to choose hazelcast member to kill -- will try again in next iteration"
				}
				lp.LogInternalStateEvent(msg, log.WarnLevel)
				continue
			}

			err = m.killer.kill(member, *mc.accessConfig, *mc.memberGrace)
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

	var chaosProbability float64
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".chaosProbability", client.ValidatePercentage, func(a any) {
			chaosProbability = a.(float64)
		})
	})

	var hzMemberAccessMode string
	assignmentOps = append(assignmentOps, func() error {
		return propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess.mode", client.ValidateString, func(a any) {
			hzMemberAccessMode = a.(string)
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

	ac, err := b.populateMemberAccessConfig(hzMemberAccessMode)
	if err != nil {
		return nil, err
	}

	return &monkeyConfig{
		enabled:                 enabled,
		stopWhenRunnersFinished: stopWhenRunnersFinished,
		chaosProbability:        chaosProbability,
		accessConfig:            ac,
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

func (b monkeyConfigBuilder) populateMemberAccessConfig(accessMode string) (*memberAccessConfig, error) {

	var assignmentOps []func() error

	ac := &memberAccessConfig{
		memberAccessMode: accessMode,
	}
	if err := propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess.targetOnlyActive", client.ValidateBool, func(a any) {
		ac.targetOnlyActive = a.(bool)
	}); err != nil {
		return nil, err
	}

	switch accessMode {
	case k8sOutOfClusterAccessMode:
		var kubeconfig string
		assignmentOps = append(assignmentOps, func() error {
			return propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".kubeconfig", client.ValidateString, func(a any) {
				kubeconfig = a.(string)
			})
		})
		var namespace string
		assignmentOps = append(assignmentOps, func() error {
			return propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".namespace", client.ValidateString, func(a any) {
				namespace = a.(string)
			})
		})
		var labelSelector string
		assignmentOps = append(assignmentOps, func() error {
			return propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".labelSelector", client.ValidateString, func(a any) {
				labelSelector = a.(string)
			})
		})
		for _, f := range assignmentOps {
			if err := f(); err != nil {
				return nil, err
			}
		}
		ac.k8sOutOfCluster = k8sOutOfClusterMemberAccess{
			kubeconfig:    kubeconfig,
			namespace:     namespace,
			labelSelector: labelSelector,
		}
	case k8sInClusterAccessMode:
		var labelSelector string
		if err := propertyAssigner.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".labelSelector", client.ValidateString, func(a any) {
			labelSelector = a.(string)
		}); err != nil {
			return nil, err
		}
		for _, f := range assignmentOps {
			if err := f(); err != nil {
				return nil, err
			}
		}
		ac.k8sInCluster = k8sInClusterMemberAccess{
			labelSelector: labelSelector,
		}
	default:
		return nil, fmt.Errorf("unknown hazelcast member access mode: %s", accessMode)
	}

	return ac, nil

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
			// Member chooser and member killer share the same Kubernetes clientset
			clientsetProvider := &defaultK8sClientsetProvider{
				configBuilder:        &defaultK8sConfigBuilder{},
				clientsetInitializer: &defaultK8sClientsetInitializer{},
			}
			namespaceDiscoverer := &defaultK8sNamespaceDiscoverer{}
			m.init(
				&k8sHzMemberChooser{
					clientsetProvider:   clientsetProvider,
					namespaceDiscoverer: namespaceDiscoverer,
					podLister:           &defaultK8sPodLister{},
				},
				&k8sHzMemberKiller{
					clientsetProvider:   clientsetProvider,
					namespaceDiscoverer: namespaceDiscoverer,
					podDeleter:          &defaultK8sPodDeleter{},
				},
			)
			m.causeChaos()
		}(i)
	}

	wg.Wait()

}
