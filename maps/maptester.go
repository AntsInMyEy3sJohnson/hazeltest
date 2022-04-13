package maps

import (
	"hazeltest/client"
	"log"
	"os"
	"sync"
)

type MapTester struct {
	HzCluster string
	HzMembers []string
}

var trace *log.Logger

func init() {
	trace = log.New(os.Stdout, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func (t *MapTester) TestMaps() {

	clientID := client.ClientID()
	trace.Printf("%s: maptester starting %d runner/-s", clientID, len(MapRunners))

	var wg sync.WaitGroup
	for i := 0; i < len(MapRunners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := MapRunners[i]
			runner.Run(t.HzCluster, t.HzMembers)
		}(i)
	}

	wg.Wait()

}
