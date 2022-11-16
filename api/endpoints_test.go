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

func TestReadinessCheck(t *testing.T) {

	t.Log("given the need to test the application's readiness check")
	{
		request := httptest.NewRequest(http.MethodGet, "localhost:8080/readiness", nil)
		recorder := httptest.NewRecorder()

		t.Log("\twhen initial state is given")
		{
			readinessHandler(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()

			checkStatusCode(t, 503, response.StatusCode)

		}

		t.Log("\twhen client has raised not ready")
		{
			RaiseNotReady()

			readinessHandler(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			checkStatusCode(t, 503, response.StatusCode)

			data, err := tryResponseRead(response.Body)
			msg := "\t\tresponse body must be readable"
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

			recorder = httptest.NewRecorder()

			readinessHandler(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			checkStatusCode(t, 200, response.StatusCode)

			data, err := tryResponseRead(response.Body)
			msg := "\t\tresponse body must be readable"
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

func checkStatusCode(t *testing.T, expected, actual int) {

	msg := fmt.Sprintf("\t\tendpoint must return http status %d", expected)

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
