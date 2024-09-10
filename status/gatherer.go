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
		l       locker
		status  map[string]any
		Updates chan Update
	}
	locker interface {
		rLock()
		rUnlock()
		lock()
		unlock()
	}
	mutexLocker struct {
		m sync.RWMutex
	}
)

const (
	updateKeyFinished = "finished"
)

var (
	quitStatusGathering = Update{}
)

func (l *mutexLocker) rLock() {

	l.m.RLock()

}

func (l *mutexLocker) rUnlock() {

	l.m.RUnlock()

}

func (l *mutexLocker) lock() {

	l.m.Lock()

}

func (l *mutexLocker) unlock() {

	l.m.Unlock()

}

func NewGatherer() *Gatherer {

	return &Gatherer{
		l: &mutexLocker{
			m: sync.RWMutex{},
		},
		status: map[string]any{},
		// TODO Make buffer size configurable
		Updates: make(chan Update, 10),
	}

}

func (g *Gatherer) AssembleStatusCopy() map[string]any {

	mapCopy := make(map[string]any, len(g.status))

	g.l.rLock()
	{
		for k, v := range g.status {
			mapCopy[k] = v
		}
	}
	g.l.rUnlock()

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
	g.l.rLock()
	{
		if v, ok := g.status[updateKeyFinished]; ok {
			result = v.(bool)
		} else {
			result = false
		}
	}
	g.l.rUnlock()

	return result

}

func (g *Gatherer) insertSynchronously(u Update) {

	// No-op for testing dev4 image
	/*g.l.lock()
	{
		g.status[u.Key] = u.Value
	}
	g.l.unlock()*/

}
