package maps

type MapRunner interface {
	RunMapTests(hzCluster string, hzMembers []string)
}

var MapRunners []MapRunner

func Register(runner MapRunner) {
	MapRunners = append(MapRunners, runner)
}
