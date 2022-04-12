package maps

import (
	"sync"
)

type MapTester struct {
	HzCluster string
	HzMembers []string
}

func (tester *MapTester) TestMaps() {

	var wg sync.WaitGroup
	for i := 0; i < len(MapRunners); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			runner := MapRunners[i]
			runner.Run(tester.HzCluster, tester.HzMembers)
		}(i)
	}

	wg.Wait()

}
