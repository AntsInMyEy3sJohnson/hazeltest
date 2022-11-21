package status

import "sync"

type (
	Update struct {
		Key   string
		Value interface{}
	}
	Gatherer struct {
		m       sync.Mutex
		status  map[string]interface{}
		updates chan Update
	}
)

const (
	updateKeyRunnerFinished = "runnerFinished"
)

var (
	quitStatusGathering = Update{}
)

func NewGatherer() *Gatherer {

	return &Gatherer{
		status:  map[string]interface{}{},
		updates: make(chan Update),
	}

}

func (g *Gatherer) InsertSynchronously(u Update) {

	g.m.Lock()
	{
		g.status[u.Key] = u.Value
	}
	g.m.Unlock()

}

func (g *Gatherer) GetStatusCopy() map[string]interface{} {

	mapCopy := make(map[string]interface{}, len(g.status))

	g.m.Lock()
	{
		for k, v := range g.status {
			mapCopy[k] = v
		}
	}
	g.m.Unlock()

	return mapCopy

}

func (g *Gatherer) Listen() {

	g.InsertSynchronously(Update{Key: updateKeyRunnerFinished, Value: false})

	for {
		update := <-g.updates
		if update == quitStatusGathering {
			g.m.Lock()
			{
				g.status[updateKeyRunnerFinished] = true
			}
			g.m.Unlock()
			close(g.updates)
			return
		} else {
			g.m.Lock()
			{
				g.status[update.Key] = update.Value
			}
			g.m.Unlock()
		}

	}

}

func (g *Gatherer) StopListen() {

	g.updates <- quitStatusGathering

}

func (g *Gatherer) ListeningStopped() bool {

	var result bool
	g.m.Lock()
	{
		result = g.status[updateKeyRunnerFinished].(bool)
	}
	g.m.Unlock()

	return result

}
