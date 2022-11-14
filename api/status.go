package api

import (
	"github.com/google/uuid"
	"sync"
)

type TestLoopStatus struct {
	Source            string
	NumMaps           int
	NumRuns           uint32
	TotalRuns         uint32
	TotalRunsFinished uint32
}

type status struct {
	TestLoops []TestLoopStatus
}

var (
	Loops                 map[uuid.UUID]*TestLoopStatus
	runnerStatusFunctions sync.Map
)

func init() {

	Loops = make(map[uuid.UUID]*TestLoopStatus)

}

func RegisterRunner(id uuid.UUID, queryStatusFunc func() *sync.Map) {

	runnerStatusFunctions.Store(id, queryStatusFunc())

}
