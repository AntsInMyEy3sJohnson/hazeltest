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
	atLeastOneClientRegistered bool
	numClientsNotReady         int
}

var (
	l        *liveness
	r        *readiness
	s        status
	lp       *logging.LogProvider
	apiMutex sync.Mutex
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

	apiMutex.Lock()
	{
		r.numClientsNotReady++
		if !r.atLeastOneClientRegistered {
			r.atLeastOneClientRegistered = true
		}
		lp.LogApiEvent(fmt.Sprintf("client has raised 'not ready', number of non-ready clients now %d", r.numClientsNotReady), log.InfoLevel)
	}
	apiMutex.Unlock()

}

func RaiseReady() {

	apiMutex.Lock()
	{
		r.numClientsNotReady--
		lp.LogApiEvent(fmt.Sprintf("client has raised readiness, number of non-ready clients now %d", r.numClientsNotReady), log.InfoLevel)
		if r.numClientsNotReady == 0 && r.atLeastOneClientRegistered && !r.Up {
			r.Up = true
			lp.LogApiEvent("all clients ready", log.InfoLevel)
		}
	}
	apiMutex.Unlock()

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
