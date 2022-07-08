package api

import (
	"github.com/google/uuid"
	"sync"
)

type TestLoopStatus struct {
	Source            string
	NumMaps           int
	NumRuns           int
	TotalRuns         int
	TotalRunsFinished int
}

type status struct {
	TestLoops []TestLoopStatus
}

var (
	Loops      map[uuid.UUID]*TestLoopStatus
	loopsMutex sync.Mutex
)

func init() {

	Loops = make(map[uuid.UUID]*TestLoopStatus)

}

func InsertInitialTestLoopStatus(testLoopID uuid.UUID, status *TestLoopStatus) {

	Loops[testLoopID] = status

}

func IncreaseTotalNumRunsCompleted(testLoopID uuid.UUID, increase int) {

	loopsMutex.Lock()
	{
		Loops[testLoopID].TotalRunsFinished += increase
	}
	loopsMutex.Unlock()

}
