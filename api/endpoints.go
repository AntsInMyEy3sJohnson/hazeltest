package api

import (
	"encoding/json"
	"net/http"
	"sync"
)

const methodGet = "GET"

type liveness struct {
	Up bool
}
type readiness struct {
	Up bool
}

var (
	l     *liveness
	r     *readiness
	mutex sync.Mutex
)

func init() {

	l = &liveness{true}
	r = &readiness{false}

}

func Expose() {

	go func() {
		server := &http.Server{
			Addr: ":8080",
		}
		http.HandleFunc("/liveness", livenessHandler)
		http.HandleFunc("/readiness", readinessHandler)
		server.ListenAndServe()
	}()

}

func Ready() {

	mutex.Lock()
	{
		r.Up = true
	}
	mutex.Unlock()

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
