package status

import (
	"sync"
)

type (
	Update struct {
		Key   string
		Value any
	}
	Gatherer interface {
		AssembleStatusCopy() map[string]any
		Listen(ready chan struct{})
		StopListen()
		ListeningStopped() bool
		Gather(u Update)
	}
	DefaultGatherer struct {
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

func NewGatherer() *DefaultGatherer {

	return &DefaultGatherer{
		l: &mutexLocker{
			m: sync.RWMutex{},
		},
		status: map[string]any{},
		// TODO Make buffer size configurable
		Updates: make(chan Update, 10),
	}

}

func (g *DefaultGatherer) AssembleStatusCopy() map[string]any {

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

func (g *DefaultGatherer) Listen(ready chan struct{}) {

	g.insertSynchronously(Update{Key: updateKeyFinished, Value: false})

	// Caller is thus forced to receive on the channel -- receive operation, in turn, can be used
	// to ensure the goroutine on which the Listen method is running has been successfully scheduled
	// and started to run prior to any other goroutines the caller might spawn and whose code
	// expects this status.DefaultGatherer instance to be listening
	ready <- struct{}{}

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

func (g *DefaultGatherer) StopListen() {

	g.Updates <- quitStatusGathering

}

func (g *DefaultGatherer) Gather(u Update) {

	g.Updates <- u

}

func (g *DefaultGatherer) ListeningStopped() bool {

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

func (g *DefaultGatherer) insertSynchronously(u Update) {

	g.l.lock()
	{
		g.status[u.Key] = u.Value
	}
	g.l.unlock()

}
