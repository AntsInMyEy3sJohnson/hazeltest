package chaos

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

type (
	memberKillerMonkey struct {
		chooser hzMemberChooser
		killer  hzMemberKiller
	}
)

func init() {
	register(&memberKillerMonkey{})
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
