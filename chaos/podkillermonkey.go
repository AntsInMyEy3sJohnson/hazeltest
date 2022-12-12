package chaos

type (
	podKillerMonkey struct{}
)

func init() {
	register(&podKillerMonkey{})
}

func (m *podKillerMonkey) causeChaos() {

	// TODO Add state transitions

}

func populatePodKillerMonkeyConfig() (*monkeyConfig, error) {

	monkeyKeyPath := "chaosMonkeys.podKiller"

	configBuilder := monkeyConfigBuilder{monkeyKeyPath: monkeyKeyPath}

	return configBuilder.populateConfig()

}
