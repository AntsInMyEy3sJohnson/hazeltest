package maps

type MapRunner interface {
	Run(hzCluster string, hzMembers []string)
}

var MapRunners []MapRunner

func Register(runner MapRunner) {
	MapRunners = append(MapRunners, runner)
}
