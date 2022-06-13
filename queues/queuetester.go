package queues

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"sync"
)

type QueueTester struct {
	HzCluster string
	HzMembers []string
}

func (t *QueueTester) TestQueues() {

	clientID := client.ClientID()
	logInternalStateInfo(fmt.Sprintf("%s: queuetester starting %d runner/-s", clientID, len(QueueRunners)))

	var wg sync.WaitGroup
	for i := 0; i < len(QueueRunners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := QueueRunners[i]
			runner.Run(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}

func logInternalStateInfo(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}).Trace(msg)

}
