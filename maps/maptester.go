package maps

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"sync"
)

type MapTester struct {
	HzCluster string
	HzMembers []string
}

func (t *MapTester) TestMaps() {

	clientID := client.ClientID()
	logInternalStateEvent(fmt.Sprintf("%s: maptester starting %d runner/-s", clientID, len(Runners)), log.InfoLevel)

	var wg sync.WaitGroup
	for i := 0; i < len(Runners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := Runners[i]
			runner.RunMapTests(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}
