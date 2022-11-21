package status

import (
	"math/rand"
	"sync"
	"testing"
)

type (
	testLocker struct {
		m                    sync.Mutex
		numLocks, numUnlocks int
	}
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

func (l *testLocker) lock() {

	l.m.Lock()
	l.numLocks++

}

func (l *testLocker) unlock() {

	l.numUnlocks++
	l.m.Unlock()

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
			for i := 0; i < upper; i++ {
				go func() {
					g.InsertSynchronously(Update{
						Key:   key,
						Value: rand.Intn(100),
					})
				}()
			}

			msg := "\t\tnumber of mutex locks and unlocks must be equal"
			if l.numLocks == l.numUnlocks {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}
