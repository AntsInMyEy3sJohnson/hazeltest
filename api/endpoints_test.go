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
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)

		}

		t.Log("when http method other than get is sent")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusMethodNotAllowed
			msg := fmt.Sprintf("\t\tliveness handler must return http status %d", expectedStatusCode)
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)

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
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)
		}

		t.Log("\twhen initial state is given")
		{
			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()

			expectedStatusCode := http.StatusServiceUnavailable
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)

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
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)

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
			checkStatusCode(t, expectedStatusCode, response.StatusCode, msg)

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

func checkStatusCode(t *testing.T, expected, actual int, msg string) {

	if expected == actual {
		t.Log(msg, checkMark)
	} else {
		t.Fatal(msg, ballotX)
	}

}

func tryResponseRead(body io.ReadCloser) ([]byte, error) {

	if data, err := ioutil.ReadAll(body); err == nil {
		return data, nil
	} else {
		return nil, errors.New("unable to read response body")
	}

}
