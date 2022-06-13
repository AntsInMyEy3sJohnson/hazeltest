package queues

type QueueRunner interface {
	Run(hzCluster string, hzMembers []string)
}

var QueueRunners []QueueRunner

func Register(runner QueueRunner) {
	QueueRunners = append(QueueRunners, runner)
}
