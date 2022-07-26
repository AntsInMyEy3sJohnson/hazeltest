package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadinessCheck(t *testing.T) {

	request := httptest.NewRequest(http.MethodGet, "localhost:8080/readiness", nil)
	recorder := httptest.NewRecorder()

	readinessHandler(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()

	statusCode := response.StatusCode
	expectedStatusCode := 503

	if statusCode != expectedStatusCode {
		t.Errorf("expected status code %d, got %d", expectedStatusCode, statusCode)
	}

	RaiseNotReady()

	readinessHandler(recorder, request)

	response = recorder.Result()
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)

	if err != nil {
		t.Errorf("unexpected error occurred: %v", err)
	}

	if len(data) > 0 {
		t.Errorf("expected nil payload to be returned, got: %s", data)
	}

	statusCode = response.StatusCode
	expectedStatusCode = 503
	if statusCode != expectedStatusCode {
		t.Errorf("expected status code %d, got %d", expectedStatusCode, statusCode)
	}

	RaiseReady()

	recorder = httptest.NewRecorder()

	readinessHandler(recorder, request)

	response = recorder.Result()
	defer response.Body.Close()

	statusCode = response.StatusCode
	expectedStatusCode = 200
	if statusCode != expectedStatusCode {
		t.Errorf("expected status code %d, got %d", expectedStatusCode, statusCode)
	}

	data, err = ioutil.ReadAll(response.Body)

	if err != nil {
		t.Errorf("unexpected error occurred: %v", err)
	}

	var decodedData map[string]interface{}
	err = json.Unmarshal(data, &decodedData)

	if err != nil {
		t.Errorf("got malformed json response: %s", data)
	}

	expectedKey := "Up"
	if _, ok := decodedData[expectedKey]; !ok {
		t.Errorf("expected key '%s' not present in returned json response", expectedKey)
	}

	ready := decodedData[expectedKey].(bool)
	if !ready {
		t.Error("api did not signal readiness")
	}

}
