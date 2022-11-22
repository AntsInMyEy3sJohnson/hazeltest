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
		l       locker
		status  map[string]interface{}
		updates chan Update
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
		updates: make(chan Update),
	}

}

func (g *Gatherer) InsertSynchronously(u Update) {

	g.l.lock()
	{
		g.status[u.Key] = u.Value
	}
	g.l.unlock()

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

	g.InsertSynchronously(Update{Key: updateKeyRunnerFinished, Value: false})

	for {
		update := <-g.updates
		if update == quitStatusGathering {
			g.l.lock()
			{
				g.status[updateKeyRunnerFinished] = true
			}
			g.l.unlock()
			close(g.updates)
			return
		} else {
			g.l.lock()
			{
				g.status[update.Key] = update.Value
			}
			g.l.unlock()
		}

	}

}

func (g *Gatherer) StopListen() {

	g.updates <- quitStatusGathering

}

func (g *Gatherer) ListeningStopped() bool {

	var result bool
	g.l.lock()
	{
		result = g.status[updateKeyRunnerFinished].(bool)
	}
	g.l.unlock()

	return result

}
