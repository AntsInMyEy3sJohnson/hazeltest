package maps

type MapTester struct {
	HzCluster string
	HzMembers []string
}

func (tester *MapTester) TestMaps() {

	for _, runner := range MapRunners {
		runner.Run(tester.HzCluster, tester.HzMembers)
	}

}
