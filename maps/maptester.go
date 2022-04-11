package maps

type MapTester struct {
	HzCluster			string
	HzMemberAddresses	[]string
}

func (tester *MapTester) TestMaps() {

	for _, runner := range MapRunners {
		runner.Run(tester.HzCluster, tester.HzMemberAddresses)
	}

}

