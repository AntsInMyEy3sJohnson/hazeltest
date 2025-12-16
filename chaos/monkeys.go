package chaos

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/logging"
	"hazeltest/status"
	"math/rand"
	"sync"
	"time"
)

const (
	absoluteMemberSelectionMode = "absolute"
	relativeMemberSelectionMode = "relative"
)

const (
	perMemberActivityEvaluation activityEvaluationMode = "perMember"
	perRunActivityEvaluation    activityEvaluationMode = "perRun"
)

const (
	k8sOutOfClusterAccessMode = "k8sOutOfCluster"
	k8sInClusterAccessMode    = "k8sInCluster"
)

const (
	start                  state = "start"
	populateConfigComplete state = "populateConfigComplete"
	checkEnabledComplete   state = "checkEnabledComplete"
	raiseReadyComplete     state = "raiseReadyComplete"
	chaosStart             state = "chaosStart"
	chaosComplete          state = "chaosComplete"
)

const (
	statusKeyNumRuns          = "numRuns"
	statusKeyNumMembersKilled = "numMembersKilled"
)

var (
	monkeys []monkey
	lp      *logging.LogProvider
)

var (
	sleepTimeFunc evaluateTimeToSleep = func(sc *sleepConfig) int {
		var sleepDuration int
		if sc.enableRandomness {
			sleepDuration = rand.Intn(sc.durationSeconds + 1)
		} else {
			sleepDuration = sc.durationSeconds
		}
		return sleepDuration
	}
	readyFunc raiseReady = func() {
		api.RaiseReady()
	}
	notReadyFunc raiseNotReady = func() {
		api.RaiseNotReady()
	}
)

type (
	evaluateTimeToSleep func(sc *sleepConfig) int
	hzMemberChooser     interface {
		choose(ac *memberAccessConfig, sc *memberSelectionConfig) ([]hzMember, error)
	}
	hzMemberKiller interface {
		kill(members []hzMember, ac *memberAccessConfig, memberGrace *sleepConfig, cc *chaosProbabilityConfig) error
	}
	sleeper interface {
		sleep(sc *sleepConfig, sf evaluateTimeToSleep)
	}
	monkey interface {
		init(a client.ConfigPropertyAssigner, s sleeper, c hzMemberChooser, k hzMemberKiller, g status.Gatherer,
			readyFunc raiseReady, notReadyFunc raiseNotReady)
		causeChaos()
	}
	hzMember struct {
		identifier string
	}
	memberKillerMonkey struct {
		a                client.ConfigPropertyAssigner
		stateList        []state
		s                sleeper
		chooser          hzMemberChooser
		killer           hzMemberKiller
		g                status.Gatherer
		readyFunc        raiseReady
		notReadyFunc     raiseNotReady
		numMembersKilled uint32
	}
	monkeyConfigBuilder struct {
		monkeyKeyPath string
	}
	sleepConfig struct {
		enabled          bool
		durationSeconds  int
		enableRandomness bool
	}
	chaosProbabilityConfig struct {
		percentage     float64
		evaluationMode activityEvaluationMode
	}
	monkeyConfig struct {
		enabled         bool
		numRuns         uint32
		chaosConfig     *chaosProbabilityConfig
		selectionConfig *memberSelectionConfig
		accessConfig    *memberAccessConfig
		sleep           *sleepConfig
		memberGrace     *sleepConfig
	}
	defaultSleeper         struct{}
	raiseReady             func()
	raiseNotReady          func()
	state                  string
	activityEvaluationMode string
)

func init() {
	lp = logging.GetLogProviderInstance(client.ID())
	register(&memberKillerMonkey{})
}

func register(m monkey) {
	monkeys = append(monkeys, m)
}

func (s *defaultSleeper) sleep(sc *sleepConfig, sf evaluateTimeToSleep) {

	if sc.enabled {
		sleepDuration := sf(sc)
		lp.LogChaosMonkeyEvent(fmt.Sprintf("sleeping for '%d' seconds", sleepDuration), log.TraceLevel)
		time.Sleep(time.Duration(sleepDuration) * time.Second)
	}

}

func (m *memberKillerMonkey) init(a client.ConfigPropertyAssigner, s sleeper, c hzMemberChooser, k hzMemberKiller,
	g status.Gatherer, readyFunc raiseReady, notReadyFunc raiseNotReady) {

	m.a = a
	m.s = s
	m.chooser = c
	m.killer = k
	m.g = g
	m.numMembersKilled = 0
	m.readyFunc = readyFunc
	m.notReadyFunc = notReadyFunc

	api.RegisterStatefulActor(api.ChaosMonkeys, "memberKiller", m.g.AssembleStatusCopy)

}

func (m *memberKillerMonkey) causeChaos() {

	defer m.g.StopListen()

	listenReady := make(chan struct{})
	go m.g.Listen(listenReady)
	<-listenReady

	m.insertInitialStatus()

	m.appendState(start)

	mc, err := populateMemberKillerMonkeyConfig(m.a)
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("aborting member killer monkey launch: unable to populate config due to error: %s", err.Error()), log.ErrorLevel)
		return
	}
	m.appendState(populateConfigComplete)
	m.g.Gather(status.Update{Key: statusKeyNumRuns, Value: mc.numRuns})

	if !mc.enabled {
		lp.LogChaosMonkeyEvent("member killer monkey not enabled -- won't run", log.InfoLevel)
		return
	}
	m.notReadyFunc()
	m.appendState(checkEnabledComplete)

	m.appendState(raiseReadyComplete)
	m.appendState(chaosStart)

	m.readyFunc()

	updateStep := uint32(50)
	for i := uint32(0); i < mc.numRuns; i++ {
		m.s.sleep(mc.sleep, sleepTimeFunc)
		if i > 0 && i%updateStep == 0 {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("finished %d of %d runs for member killer monkey", i, mc.numRuns), log.InfoLevel)
		}
		lp.LogChaosMonkeyEvent(fmt.Sprintf("member killer monkey in run %d", i), log.TraceLevel)
		if monkeyInvocationNecessary(mc.chaosConfig) {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("member killer monkey active in run %d", i), log.TraceLevel)
			members, err := m.chooser.choose(mc.accessConfig, mc.selectionConfig)
			if err != nil {
				var msg string
				if errors.Is(err, noMembersFoundError) {
					msg = "no hazelcast members available to be killed -- will try again in next iteration"
				} else if errors.Is(err, noReadyMembersFoundError) {
					msg = "no suitable hazelcast members available to be killed -- will try again in next iteration"
				} else {
					msg = "unable to choose hazelcast members to kill -- will try again in next iteration"
				}
				lp.LogChaosMonkeyEvent(msg, log.WarnLevel)
				continue
			}

			err = m.killer.kill(members, mc.accessConfig, mc.memberGrace, mc.chaosConfig)
			if err != nil {
				lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to kill chosen hazelcast members (%s) -- will try again in next iteration", members), log.WarnLevel)
			} else {
				m.updateNumMembersKilled(uint32(len(members)))
			}
		} else {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("member killer monkey inactive in run %d", i), log.InfoLevel)
		}
	}

	m.appendState(chaosComplete)
	lp.LogChaosMonkeyEvent(fmt.Sprintf("member killer monkey done after %d loop/-s", mc.numRuns), log.InfoLevel)

}

func (m *memberKillerMonkey) updateNumMembersKilled(num uint32) {

	m.numMembersKilled += num
	m.g.Gather(status.Update{Key: statusKeyNumMembersKilled, Value: m.numMembersKilled})

}

func (m *memberKillerMonkey) insertInitialStatus() {

	m.g.Gather(status.Update{Key: statusKeyNumRuns, Value: uint32(0)})
	m.g.Gather(status.Update{Key: statusKeyNumMembersKilled, Value: uint32(0)})

}

func (m *memberKillerMonkey) appendState(s state) {

	m.stateList = append(m.stateList, s)

}

func monkeyInvocationNecessary(chaosConfig *chaosProbabilityConfig) bool {

	if chaosConfig.evaluationMode == perMemberActivityEvaluation {
		return true
	}

	if chaosConfig.evaluationMode == perRunActivityEvaluation {
		f := rand.Float64()
		return f <= chaosConfig.percentage
	}

	return false

}

func populateMemberKillerMonkeyConfig(a client.ConfigPropertyAssigner) (*monkeyConfig, error) {

	monkeyKeyPath := "chaosMonkeys.memberKiller"

	configBuilder := monkeyConfigBuilder{monkeyKeyPath: monkeyKeyPath}

	return configBuilder.populateConfig(a)

}

func (b monkeyConfigBuilder) populateConfig(a client.ConfigPropertyAssigner) (*monkeyConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var numRuns uint32
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".numRuns", client.ValidateInt, func(a any) {
			numRuns = uint32(a.(int))
		})
	})

	var hzMemberSelectionMode string
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberSelection.mode", client.ValidateString, func(a any) {
			hzMemberSelectionMode = a.(string)
		})
	})

	var hzMemberAccessMode string
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberAccess.mode", client.ValidateString, func(a any) {
			hzMemberAccessMode = a.(string)
		})
	})

	var sleepEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".sleep.enabled", client.ValidateBool, func(a any) {
			sleepEnabled = a.(bool)
		})
	})

	var sleepDurationSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".sleep.durationSeconds", client.ValidateInt, func(a any) {
			sleepDurationSeconds = a.(int)
		})
	})

	var sleepEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".sleep.enableRandomness", client.ValidateBool, func(a any) {
			sleepEnableRandomness = a.(bool)
		})
	})

	var memberGraceEnabled bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberGrace.enabled", client.ValidateBool, func(a any) {
			memberGraceEnabled = a.(bool)
		})
	})

	var memberGraceDurationSeconds int
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberGrace.durationSeconds", client.ValidateInt, func(a any) {
			memberGraceDurationSeconds = a.(int)
		})
	})

	var memberGraceEnableRandomness bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberGrace.enableRandomness", client.ValidateBool, func(a any) {
			memberGraceEnableRandomness = a.(bool)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	sc, err := b.populateMemberSelectionConfig(a, hzMemberSelectionMode)
	if err != nil {
		return nil, err
	}

	ac, err := b.populateMemberAccessConfig(a, hzMemberAccessMode)
	if err != nil {
		return nil, err
	}

	cc, err := b.populateChaosProbabilityConfig(a)
	if err != nil {
		return nil, err
	}

	return &monkeyConfig{
		enabled:         enabled,
		numRuns:         numRuns,
		chaosConfig:     cc,
		selectionConfig: sc,
		accessConfig:    ac,
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

func (b monkeyConfigBuilder) populateChaosProbabilityConfig(a client.ConfigPropertyAssigner) (*chaosProbabilityConfig, error) {

	var assignmentOps []func() error

	var percentage float64
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".chaosProbability.percentage", client.ValidatePercentage, func(a any) {
			if v, ok := a.(int); ok {
				percentage = float64(v)
			} else if v, ok := a.(float32); ok {
				percentage = float64(v)
			} else if v, ok := a.(float64); ok {
				percentage = v
			}
		})
	})

	var evaluationMode string
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".chaosProbability.evaluationMode", client.ValidateString, func(a any) {
			evaluationMode = a.(string)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &chaosProbabilityConfig{
		percentage:     percentage,
		evaluationMode: activityEvaluationMode(evaluationMode),
	}, nil

}

func (b monkeyConfigBuilder) populateMemberSelectionConfig(a client.ConfigPropertyAssigner, selectionMode string) (*memberSelectionConfig, error) {

	var assignmentOps []func() error

	sc := &memberSelectionConfig{
		selectionMode: selectionMode,
	}

	var absoluteNumMembersToKill uint8
	var relativePercentageOfMembersToKill float32

	var targetOnlyActive bool
	assignmentOps = append(assignmentOps, func() error {
		return a.Assign(b.monkeyKeyPath+".memberSelection.targetOnlyActive", client.ValidateBool, func(a any) {
			targetOnlyActive = a.(bool)
		})
	})

	switch selectionMode {
	case absoluteMemberSelectionMode:
		assignmentOps = append(assignmentOps, func() error {
			return a.Assign(b.monkeyKeyPath+".memberSelection.absolute.numMembersToKill", client.ValidateInt, func(a any) {
				absoluteNumMembersToKill = uint8(a.(int))
			})
		})
	case relativeMemberSelectionMode:
		assignmentOps = append(assignmentOps, func() error {
			return a.Assign(b.monkeyKeyPath+".memberSelection.relative.percentageOfMembersToKill", client.ValidatePercentage, func(a any) {
				if v, ok := a.(float64); ok {
					relativePercentageOfMembersToKill = float32(v)
				} else if v, ok := a.(float32); ok {
					relativePercentageOfMembersToKill = v
				} else {
					relativePercentageOfMembersToKill = float32(a.(int))
				}
			})
		})
	}

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	sc.targetOnlyActive = targetOnlyActive
	sc.absoluteNumMembersToKill = absoluteNumMembersToKill
	sc.relativePercentageOfMembersToKill = relativePercentageOfMembersToKill

	return sc, nil

}

func (b monkeyConfigBuilder) populateMemberAccessConfig(a client.ConfigPropertyAssigner, accessMode string) (*memberAccessConfig, error) {

	var assignmentOps []func() error

	ac := &memberAccessConfig{
		accessMode: accessMode,
	}

	switch accessMode {
	case k8sOutOfClusterAccessMode:
		var kubeconfig string
		assignmentOps = append(assignmentOps, func() error {
			return a.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".kubeconfig", client.ValidateString, func(a any) {
				kubeconfig = a.(string)
			})
		})
		var namespace string
		assignmentOps = append(assignmentOps, func() error {
			return a.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".namespace", client.ValidateString, func(a any) {
				namespace = a.(string)
			})
		})
		var labelSelector string
		assignmentOps = append(assignmentOps, func() error {
			return a.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".labelSelector", client.ValidateString, func(a any) {
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
		if err := a.Assign(b.monkeyKeyPath+".memberAccess."+accessMode+".labelSelector", client.ValidateString, func(a any) {
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
	lp.LogChaosMonkeyEvent(fmt.Sprintf("%s: starting %d chaos monkey/-s", clientID, len(monkeys)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(monkeys); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := monkeys[i]
			// The only mode for accessing hazelcastwrapper members is currently through kubernetes, and as long as that's the
			// case, we can safely hard-code the member chooser and member killer
			// Member chooser and member killer share the same Kubernetes clientset
			clientsetProvider := &defaultK8sClientsetProvider{
				configBuilder:        &defaultK8sConfigBuilder{},
				clientsetInitializer: &defaultK8sClientsetInitializer{},
			}
			namespaceDiscoverer := &defaultK8sNamespaceDiscoverer{}
			m.init(
				&client.DefaultConfigPropertyAssigner{},
				&defaultSleeper{},
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
				status.NewGatherer(),
				readyFunc,
				notReadyFunc,
			)
			m.causeChaos()
		}(i)
	}

	wg.Wait()

}
