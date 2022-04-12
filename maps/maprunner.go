package maps

type MapRunner interface {
	Run(hzCluster string, hzMembers []string)
}

var MapRunners []MapRunner

func Register(r MapRunner) {
	MapRunners = append(MapRunners, r)
}
