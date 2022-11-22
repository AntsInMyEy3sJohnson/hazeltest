package status

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

type (
	testLocker struct {
		m                    sync.Mutex
		numLocks, numUnlocks int
	}
	stateExposingWaitGroup struct {
		sync.WaitGroup
		count int32
	}
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

func (wg *stateExposingWaitGroup) add(delta int) {
	atomic.AddInt32(&wg.count, int32(delta))
	wg.WaitGroup.Add(delta)
}

func (wg *stateExposingWaitGroup) done() {
	atomic.AddInt32(&wg.count, -1)
	wg.WaitGroup.Done()
}

func (wg *stateExposingWaitGroup) waitingCount() int {
	return int(atomic.LoadInt32(&wg.count))
}

func (l *testLocker) lock() {

	l.m.Lock()
	l.numLocks++

}

func (l *testLocker) unlock() {

	l.numUnlocks++
	l.m.Unlock()

}

func TestGatherer_Listen(t *testing.T) {

	t.Log("given the need to test the gatherer's ability to listen for status updates")
	{
		t.Log("\twhen listener runs on goroutine")
		{
			l := &testLocker{
				m:          sync.Mutex{},
				numLocks:   0,
				numUnlocks: 0,
			}
			g := &Gatherer{
				l:       l,
				status:  map[string]interface{}{},
				updates: make(chan Update),
			}

			wg := &stateExposingWaitGroup{
				WaitGroup: sync.WaitGroup{},
				count:     0,
			}
			wg.add(1)
			go func() {
				defer wg.done()
				g.Listen()
			}()

			// Listen performs initial insertion of key in question synchronously, so we can wait for the insert
			// to be finished by watching the number of locks
			for {
				if l.numLocks == 1 && l.numUnlocks == 1 {
					break
				}
			}

			msg := "\t\trunner finished must be set to false in status"

			if !(g.status[updateKeyRunnerFinished].(bool)) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			t.Log("\t\tupon stop signal")
			{
				msg = "\t\t\tlistener must set runner finished to true"
				g.updates <- quitStatusGathering

				// Wait for gatherer to finish status update
				for {
					if l.numLocks == 2 && l.numUnlocks == 2 {
						break
					}
				}

				if g.status[updateKeyRunnerFinished].(bool) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tlistener must close channel"

				if _, channelOpen := <-g.updates; !channelOpen {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tlistener must return, thus ending the goroutine it has been running on"
				if wg.waitingCount() == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}

		}

	}

}

func TestGatherer_AssembleStatusCopy(t *testing.T) {

	t.Log("given the need to test retrieving a copy of the gatherer's current status")
	{
		t.Log("\twhen status is empty")
		{
			g := NewGatherer()

			statusCopy := g.AssembleStatusCopy()
			msg := "\t\tcopy must be empty, too"

			if len(statusCopy) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen status contains elements")
		{
			g := NewGatherer()

			u1 := Update{"awesomeKey", "awesomeValue"}
			g.status[u1.Key] = u1.Value

			u2 := Update{"anotherKey", "anotherValue"}
			g.status[u2.Key] = u2.Value

			statusCopy := g.AssembleStatusCopy()

			msg := "\t\tcopy and underlying status must contain same elements"
			if equal, detail := mapsEqualInContent(g.status, statusCopy); equal {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			statusCopy[u1.Key] = "anotherAwesomeValue"
			msg = "\t\tchange to copy must not be reflected in source map"

			if g.status[u1.Key] == u1.Value {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestGatherer_InsertSynchronously(t *testing.T) {

	t.Log("given the need to test synchronous inserts of status updates")
	{
		t.Log("\twhen update is inserted")
		{
			key := "awesomeKey"
			value := "awesomeValue"
			u := Update{
				Key:   key,
				Value: value,
			}

			g := NewGatherer()
			g.InsertSynchronously(u)

			msg := "\t\tinserted update must be present in status map"
			if v, ok := g.status[key]; ok && v == value {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen multiple updates are performed simultaneously")
		{
			key := "someNumberKey"

			l := &testLocker{
				m:          sync.Mutex{},
				numLocks:   0,
				numUnlocks: 0,
			}
			g := &Gatherer{
				l:       l,
				status:  map[string]interface{}{},
				updates: make(chan Update),
			}
			upper := 100
			wg := sync.WaitGroup{}
			for i := 0; i < upper; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					g.InsertSynchronously(Update{
						Key:   key,
						Value: rand.Intn(100),
					})
				}()
			}
			wg.Wait()

			msg := "\t\tnumber of mutex locks and unlocks must be equal"
			if l.numLocks == l.numUnlocks {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("num locks: %d; num unlocks: %d", l.numLocks, l.numUnlocks))
			}

		}
	}

}

func mapsEqualInContent(reference map[string]interface{}, candidate map[string]interface{}) (bool, string) {

	if len(reference) != len(candidate) {
		return false, "given maps do not have same length, hence cannot have equal content"
	}

	for k1, v1 := range reference {
		if v2, ok := candidate[k1]; !ok {
			return false, fmt.Sprintf("key wanted in candidate map, but not found: %s", k1)
		} else if v1 != v2 {
			return false, fmt.Sprintf("key '%s' associated with different values -- wanted: %v; got: %v", k1, v1, v2)
		}
	}

	return true, ""

}
