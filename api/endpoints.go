package api

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"net/http"
	"strconv"
	"sync"
)

const methodGet = "GET"

type liveness struct {
	Up bool
}
type readiness struct {
	Up bool
	// "actors" can be runners and chaos monkeys
	atLeastOneActorRegistered bool
	numNonReadyActors         int
}

var (
	l  *liveness
	r  *readiness
	lp *logging.LogProvider
	m  sync.Mutex
)

func init() {

	l = &liveness{true}
	r = &readiness{false, false, 0}

	lp = &logging.LogProvider{ClientID: client.ID()}

}

func Serve() {

	port := 8080
	server := &http.Server{
		Addr: ":" + strconv.Itoa(port),
	}
	http.HandleFunc("/liveness", livenessHandler)
	http.HandleFunc("/readiness", readinessHandler)
	http.HandleFunc("/status", statusHandler)
	err := server.ListenAndServe()
	if err != nil {
		lp.LogApiEvent(fmt.Sprintf("unable to serve api on port %d", port), log.ErrorLevel)
		return
	}

}

func RaiseNotReady() {

	m.Lock()
	{
		r.Up = false
		r.numNonReadyActors++
		if !r.atLeastOneActorRegistered {
			r.atLeastOneActorRegistered = true
		}
		lp.LogApiEvent(fmt.Sprintf("actor has raised 'not ready', number of non-ready actors now %d", r.numNonReadyActors), log.InfoLevel)
	}
	m.Unlock()

}

func RaiseReady() {

	m.Lock()
	{
		r.numNonReadyActors--
		lp.LogApiEvent(fmt.Sprintf("actor has raised readiness, number of non-ready actors now %d", r.numNonReadyActors), log.InfoLevel)
		if r.numNonReadyActors == 0 && r.atLeastOneActorRegistered && !r.Up {
			r.Up = true
			lp.LogApiEvent("all actors ready", log.InfoLevel)
		}
	}
	m.Unlock()

}

func statusHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		actorStatus := assembleActorStatus()
		bytes, _ := json.Marshal(actorStatus)
		_, _ = w.Write(bytes)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}

func livenessHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		bytes, _ := json.Marshal(l)
		_, _ = w.Write(bytes)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}

func readinessHandler(w http.ResponseWriter, req *http.Request) {

	switch req.Method {
	case methodGet:
		if r.Up {
			bytes, _ := json.Marshal(r)
			_, _ = w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

}
