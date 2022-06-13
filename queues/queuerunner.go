package queues

type QueueRunner interface {
	RunQueueTests(hzCluster string, hzMembers []string)
}

var QueueRunners []QueueRunner

func Register(runner QueueRunner) {
	QueueRunners = append(QueueRunners, runner)
}
