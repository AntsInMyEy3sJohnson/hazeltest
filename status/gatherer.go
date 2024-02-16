package status

import (
	"sync"
)

type (
	Update struct {
		Key   string
		Value any
	}
	Gatherer struct {
		l      locker
		status map[string]any
		// Not strictly required as of current status gathering needs, but foundation for more sophisticated gathering
		// --> https://github.com/AntsInMyEy3sJohnson/hazeltest/issues/20
		Updates chan Update
	}
	locker interface {
		lock()
		unlock()
	}
	mutexLocker struct {
		// TODO Use sync.RWMutex instead
		m sync.Mutex
	}
)

const (
	updateKeyFinished = "finished"
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
		status: map[string]any{},
		// TODO Make buffer size configurable
		Updates: make(chan Update, 10),
	}

}

func (g *Gatherer) AssembleStatusCopy() map[string]any {

	mapCopy := make(map[string]any, len(g.status))

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

	g.insertSynchronously(Update{Key: updateKeyFinished, Value: false})

	for {
		update := <-g.Updates
		if update == quitStatusGathering {
			g.insertSynchronously(Update{Key: updateKeyFinished, Value: true})
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
		if v, ok := g.status[updateKeyFinished]; ok {
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
