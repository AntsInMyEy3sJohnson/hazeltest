package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusHandler(t *testing.T) {

	t.Log("given the need to test the status handler, serving the application's status endpoint")
	{
		t.Log("\twhen http method other than get is passed")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/status", nil))
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusMethodNotAllowed
			msg := fmt.Sprintf("\t\tstatus handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen no test loops have registered yet")
		{
			request := httptest.NewRequest(http.MethodGet, "localhost:8080/status", nil)
			recorder := httptest.NewRecorder()

			statusHandler(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\tstatus handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			data, err := tryResponseRead(response.Body)
			msg = "\t\tresponse must be readable"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tresponse body must be valid json"
			var decodedData map[string]interface{}
			err = json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdecoded map must contain top-level keys for map and queue test loops"
			if len(decodedData) == 2 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			if _, ok := decodedData[string(Maps)]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, Maps)
			}

			if _, ok := decodedData[string(Queues)]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, Queues)
			}

		}

		t.Log("\twhen two map test loops have registered")
		{
			RegisterTestLoop(Maps, sourceMapPokedexRunner, func() map[string]interface{} {
				return dummyStatusMapPokedexTestLoop
			})
			RegisterTestLoop(Maps, sourceMapLoadRunner, func() map[string]interface{} {
				return dummyStatusMapLoadTestLoop
			})

			request := httptest.NewRequest(http.MethodGet, "localhost:8080/status", nil)
			recorder := httptest.NewRecorder()

			statusHandler(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\tstatus handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			data, _ := tryResponseRead(response.Body)

			msg = "\t\tresponse body must be valid json"
			var decodedData map[string]interface{}
			err := json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdecoded map must contain top-level keys for map and queue test loops"
			if len(decodedData) == 2 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			if _, ok := decodedData[string(Maps)]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, Maps)
			}

			if _, ok := decodedData[string(Queues)]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, Queues)
			}

			msg = "\t\tmap for map test loop status must contain keys for both registered test loops"
			statusPokedexRunnerTestLoop, okPokedex := decodedData[string(Maps)].(map[string]interface{})[sourceMapPokedexRunner]
			if okPokedex {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			statusLoadRunnerTestLoop, okLoad := decodedData[string(Maps)].(map[string]interface{})[sourceMapLoadRunner]
			if okLoad {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			msg = "\t\tnested maps must be equal to registered status"
			parseNumberValuesBackToInt(statusPokedexRunnerTestLoop.(map[string]interface{}))
			if ok, detail := mapsEqualInContent(dummyStatusMapPokedexTestLoop, statusPokedexRunnerTestLoop.(map[string]interface{})); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			parseNumberValuesBackToInt(statusLoadRunnerTestLoop.(map[string]interface{}))
			if ok, detail := mapsEqualInContent(dummyStatusMapLoadTestLoop, statusLoadRunnerTestLoop.(map[string]interface{})); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}
	}

}

func TestLivenessHandler(t *testing.T) {

	t.Log("given the need to test the liveness handler, serving the application's liveness check")
	{
		t.Log("when http get is sent")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodGet, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\tliveness handlet must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("when http method other than get is sent")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusMethodNotAllowed
			msg := fmt.Sprintf("\t\tliveness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func TestReadinessHandler(t *testing.T) {

	t.Log("given the need to test the readiness handler, serving the application's readiness check")
	{
		request := httptest.NewRequest(http.MethodGet, "localhost:8080/readiness", nil)

		t.Log("\twhen http method other than http get is sent")
		{
			recorder := httptest.NewRecorder()

			readinessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusMethodNotAllowed
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen initial state is given")
		{
			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusServiceUnavailable
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen client has raised not ready")
		{
			RaiseNotReady()

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusServiceUnavailable
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			data, err := tryResponseRead(response.Body)
			msg = "\t\tresponse body must be readable"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned payload must be empty"

			if len(data) == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen client has raised readiness")
		{
			RaiseReady()

			recorder := httptest.NewRecorder()
			recorder = httptest.NewRecorder()

			readinessHandler(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			data, err := tryResponseRead(response.Body)
			msg = "\t\tresponse body must be readable"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tbody of returned payload must be valid json"
			var decodedData map[string]interface{}
			err = json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			expectedKey := "Up"
			msg = fmt.Sprintf("\t\tjson must contain '%s' key", expectedKey)
			if _, ok := decodedData[expectedKey]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			ready := decodedData[expectedKey].(bool)
			msg = "\t\tjson must contain affirmative readiness flag"
			if ready {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

	}

}

func parseNumberValuesBackToInt(m map[string]interface{}) {

	m[statusKeyNumMaps] = int(m[statusKeyNumMaps].(float64))
	m[statusKeyNumRuns] = int(m[statusKeyNumRuns].(float64))
	m[statusKeyTotalRuns] = int(m[statusKeyTotalRuns].(float64))

}

func tryResponseRead(body io.ReadCloser) ([]byte, error) {

	if data, err := ioutil.ReadAll(body); err == nil {
		return data, nil
	} else {
		return nil, errors.New("unable to read response body")
	}

}
