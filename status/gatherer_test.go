package status

import (
	"fmt"
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
