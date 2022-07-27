package api

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"net/http"
	"sync"
)

const methodGet = "GET"

type liveness struct {
	Up bool
}
type readiness struct {
	Up                         bool
	atLeastOneRunnerRegistered bool
	numNonReadyRunners         int
}

var (
	l  *liveness
	r  *readiness
	s  status
	lp *logging.LogProvider
	m  sync.Mutex
)

func init() {

	l = &liveness{true}
	r = &readiness{false, false, 0}
	s = status{[]TestLoopStatus{}}

	lp = &logging.LogProvider{ClientID: client.ID()}

}

func Serve() {

	go func() {
		server := &http.Server{
			Addr: ":8080",
		}
		http.HandleFunc("/liveness", livenessHandler)
		http.HandleFunc("/readiness", readinessHandler)
		http.HandleFunc("/status", statusHandler)
		server.ListenAndServe()
	}()

}

func RaiseNotReady() {

	m.Lock()
	{
		r.numNonReadyRunners++
		if !r.atLeastOneRunnerRegistered {
			r.atLeastOneRunnerRegistered = true
		}
		lp.LogApiEvent(fmt.Sprintf("runner has raised 'not ready', number of non-ready runners now %d", r.numNonReadyRunners), log.InfoLevel)
	}
	m.Unlock()

}

func RaiseReady() {

	m.Lock()
	{
		r.numNonReadyRunners--
		lp.LogApiEvent(fmt.Sprintf("runner has raised readiness, number of non-ready runners now %d", r.numNonReadyRunners), log.InfoLevel)
		if r.numNonReadyRunners == 0 && r.atLeastOneRunnerRegistered && !r.Up {
			r.Up = true
			lp.LogApiEvent("all runners ready", log.InfoLevel)
		}
	}
	m.Unlock()

}

func statusHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		updateStatus(&s)
		bytes, _ := json.Marshal(s)
		w.Write(bytes)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}

func updateStatus(s *status) {

	if len(Loops) > 0 {
		values := make([]TestLoopStatus, 0, len(Loops))
		for _, v := range Loops {
			values = append(values, *v)
		}
		s.TestLoops = values
	}

}

func livenessHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		bytes, _ := json.Marshal(l)
		w.Write(bytes)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}

func readinessHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		if r.Up {
			bytes, _ := json.Marshal(r)
			w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}
