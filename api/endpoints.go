package api

import (
	"encoding/json"
	"hazeltest/maps"
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
type status struct {
	TestLoops []maps.TestLoopStatus
}

var (
	l     *liveness
	r     *readiness
	s     status
	mutex sync.Mutex
)

func init() {

	l = &liveness{true}
	r = &readiness{false}
	s = status{[]maps.TestLoopStatus{}}

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

func Ready() {

	mutex.Lock()
	{
		r.Up = true
	}
	mutex.Unlock()

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

	loops := maps.Loops

	if len(loops) > 0 {
		values := make([]maps.TestLoopStatus, 0, len(loops))
		for _, v := range loops {
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
