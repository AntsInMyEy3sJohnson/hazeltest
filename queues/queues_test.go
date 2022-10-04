package queues

type (
	testConfigPropertyAssigner struct {
		returnError bool
		dummyConfig map[string]interface{}
	}
)

const (
	checkMark     = "\u2713"
	ballotX       = "\u2717"
	runnerKeyPath = "testQueueRunner"
	queuePrefix   = "t_"
	queueBaseName = "test"
)
