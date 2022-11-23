package status

import (
	"sync"
)

type (
	Update struct {
		Key   string
		Value interface{}
	}
	Gatherer struct {
		l      locker
		status map[string]interface{}
		// Not strictly required as of current status gathering needs, but foundation for more sophisticated gathering
		// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/20
		Updates chan Update
	}
	locker interface {
		lock()
		unlock()
	}
	mutexLocker struct {
		m sync.Mutex
	}
)

const (
	updateKeyRunnerFinished = "runnerFinished"
)

var (
	quitStatusGathering = Update{}
)

func (l *mutexLocker) lock() {

	l.m.Lock()

}

func (l *mutexLocker) unlock() {

	l.m.Unlock()

}

func NewGatherer() *Gatherer {

	return &Gatherer{
		l: &mutexLocker{
			m: sync.Mutex{},
		},
		status:  map[string]interface{}{},
		Updates: make(chan Update),
	}

}

func (g *Gatherer) AssembleStatusCopy() map[string]interface{} {

	mapCopy := make(map[string]interface{}, len(g.status))

	g.l.lock()
	{
		for k, v := range g.status {
			mapCopy[k] = v
		}
	}
	g.l.unlock()

	return mapCopy

}

func (g *Gatherer) Listen() {

	g.insertSynchronously(Update{Key: updateKeyRunnerFinished, Value: false})

	for {
		update := <-g.Updates
		if update == quitStatusGathering {
			g.insertSynchronously(Update{Key: updateKeyRunnerFinished, Value: true})
			close(g.Updates)
			return
		} else {
			g.insertSynchronously(update)
		}

	}

}

func (g *Gatherer) StopListen() {

	g.Updates <- quitStatusGathering

}

func (g *Gatherer) ListeningStopped() bool {

	var result bool
	g.l.lock()
	{
		if v, ok := g.status[updateKeyRunnerFinished]; ok {
			result = v.(bool)
		} else {
			result = false
		}
	}
	g.l.unlock()

	return result

}

func (g *Gatherer) insertSynchronously(u Update) {

	g.l.lock()
	{
		g.status[u.Key] = u.Value
	}
	g.l.unlock()

}
