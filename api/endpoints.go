package api

import (
	"encoding/json"
	"fmt"
	"hazeltest/client"
	"net/http"
	"strconv"
	"sync"

	log "go.uber.org/zap/zapcore"
)

const (
	methodGet        = "GET"
	loggingComponent = "api"
)

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
	lp *client.LogProvider
	m  sync.Mutex
)

func init() {

	l = &liveness{true}
	r = &readiness{false, false, 0}

	var err error
	lp, err = client.GetLogProviderInstance(client.ID(), loggingComponent)

	if err != nil {
		panic(err)
	}

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
		lp.Log(fmt.Sprintf("unable to serve api on port %d", port), client.ApiEvent, log.ErrorLevel)
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
		lp.Log(fmt.Sprintf("actor has raised 'not ready', number of non-ready actors now %d", r.numNonReadyActors), client.ApiEvent, log.InfoLevel)
	}
	m.Unlock()

}

func RaiseReady() {

	m.Lock()
	{
		r.numNonReadyActors--
		lp.Log(fmt.Sprintf("actor has raised readiness, number of non-ready actors now %d", r.numNonReadyActors), client.ApiEvent, log.InfoLevel)
		if r.numNonReadyActors == 0 && r.atLeastOneActorRegistered && !r.Up {
			r.Up = true
			lp.Log("all actors ready", client.ApiEvent, log.InfoLevel)
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
