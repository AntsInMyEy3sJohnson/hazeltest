package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusHandler(t *testing.T) {

	t.Log("given a status handler to serve the application's status endpoint")
	{
		t.Log("\twhen http method other than get is passed")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/status", nil))
			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			var decodedData map[string]any
			err = json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdecoded map must contain top-level keys for each of the available actor groups"
			if len(decodedData) == len(availableActorGroups) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tfor each of the actor groups, top-level key must store empty map"
			for _, v := range availableActorGroups {
				if len(decodedData[string(v)].(map[string]any)) == 0 {
					t.Log(msg, checkMark, v)
				} else {
					t.Fatal(msg, ballotX, v)
				}
			}
		}

		t.Log("\twhen two map runners and one chaos monkey have registered")
		{
			tracker = newStatefulActorTracker()

			RegisterStatefulActor(MapRunners, sourceMapPokedexRunner, func() map[string]any {
				return testStatusMapPokedexTestLoop
			})

			RegisterStatefulActor(MapRunners, sourceMapLoadRunner, func() map[string]any {
				return testStatusMapLoadTestLoop
			})

			RegisterStatefulActor(ChaosMonkeys, sourceChaosMonkeyMemberKiller, func() map[string]any {
				return testStatusMemberKillerMonkey
			})

			request := httptest.NewRequest(http.MethodGet, "localhost:8080/status", nil)
			recorder := httptest.NewRecorder()

			statusHandler(recorder, request)
			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\tstatus handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, response.StatusCode)
			}

			data, _ := tryResponseRead(response.Body)

			msg = "\t\tresponse body must be valid json"
			var decodedData map[string]any
			err := json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdecoded map must contain top-level keys for each of the available actor groups"
			if len(decodedData) == len(availableActorGroups) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tmap runner status map must contain top-level keys for map and queue runners"
			decodedTestLoopsData := decodedData[string(MapRunners)].(map[string]any)
			if _, ok := decodedTestLoopsData[sourceMapPokedexRunner]; ok {
				t.Log(msg, checkMark, sourceMapPokedexRunner)
			} else {
				t.Fatal(msg, ballotX, sourceMapPokedexRunner)
			}

			if _, ok := decodedTestLoopsData[sourceMapLoadRunner]; ok {
				t.Log(msg, checkMark, sourceMapLoadRunner)
			} else {
				t.Fatal(msg, ballotX, sourceMapLoadRunner)
			}

			msg = "\t\tnested maps must contain expected values"
			parseRunnerNumberValuesBackToInt(decodedTestLoopsData[sourceMapPokedexRunner].(map[string]any))
			if ok, detail := mapsEqualInContent(testStatusMapPokedexTestLoop, decodedTestLoopsData[sourceMapPokedexRunner].(map[string]any)); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			parseRunnerNumberValuesBackToInt(decodedTestLoopsData[sourceMapLoadRunner].(map[string]any))
			if ok, detail := mapsEqualInContent(testStatusMapLoadTestLoop, decodedTestLoopsData[sourceMapLoadRunner].(map[string]any)); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

			msg = "\t\tchaos monkey map must contain exactly one element"
			chaosMonkeyStatus := decodedData[string(ChaosMonkeys)].(map[string]any)
			if len(chaosMonkeyStatus) == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tchaos monkey map must contain member killer status equal to given test status"
			memberKillerStatus := chaosMonkeyStatus[sourceChaosMonkeyMemberKiller].(map[string]any)
			parseChaosMonkeyNumberValuesBackToInt(memberKillerStatus)
			if ok, detail := mapsEqualInContent(testStatusMemberKillerMonkey, memberKillerStatus); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}
		}
		t.Log("\twhen only chaos monkey has registered")
		{
			tracker = newStatefulActorTracker()

			RegisterStatefulActor(ChaosMonkeys, sourceChaosMonkeyMemberKiller, func() map[string]any {
				return testStatusMemberKillerMonkey
			})

			request := httptest.NewRequest(http.MethodGet, "localhost:8080/status", nil)
			recorder := httptest.NewRecorder()

			statusHandler(recorder, request)
			response := recorder.Result()
			defer func(body io.ReadCloser) {
				_ = body.Close()
			}(response.Body)

			expectedStatusCode := http.StatusOK
			msg := fmt.Sprintf("\t\tstatus handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, response.StatusCode)
			}

			data, _ := tryResponseRead(response.Body)

			msg = "\t\tresponse body must be valid json"
			var decodedData map[string]any
			err := json.Unmarshal(data, &decodedData)
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus map must contain one top-level element for each available actor group"
			if len(decodedData) == len(availableActorGroups) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tstatus map must contain top-level key for chaos monkey status"
			if _, ok := decodedData[string(ChaosMonkeys)]; ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tchaos monkey map must contain exactly one element"
			chaosMonkeyStatus := decodedData[string(ChaosMonkeys)].(map[string]any)
			if len(chaosMonkeyStatus) == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tchaos monkey map must contain member killer status equal to given test status"
			memberKillerStatus := chaosMonkeyStatus[sourceChaosMonkeyMemberKiller].(map[string]any)
			parseChaosMonkeyNumberValuesBackToInt(memberKillerStatus)
			if ok, detail := mapsEqualInContent(testStatusMemberKillerMonkey, memberKillerStatus); ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, detail)
			}

		}
	}

}

func TestLivenessHandler(t *testing.T) {

	t.Log("given a liveness handler to serve the application's liveness check")
	{
		t.Log("when http get is sent")
		{
			recorder := httptest.NewRecorder()

			livenessHandler(recorder, httptest.NewRequest(http.MethodGet, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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

	t.Log("given a readiness handler to serve the application's readiness endpoint")
	{
		request := httptest.NewRequest(http.MethodGet, "localhost:8080/readiness", nil)

		t.Log("\twhen http method other than http get is sent")
		{
			recorder := httptest.NewRecorder()

			readinessHandler(recorder, httptest.NewRequest(http.MethodPost, "localhost:8080/liveness", nil))
			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			r = &readiness{false, false, 0}

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)
			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			r = &readiness{false, false, 0}

			RaiseNotReady()

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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

		t.Log("\twhen client has raised readiness after at least one client has registered")
		{

			r = &readiness{false, false, 0}

			// RaiseNotReady will register actor
			RaiseNotReady()
			RaiseReady()

			recorder := httptest.NewRecorder()

			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

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
			var decodedData map[string]any
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

		t.Log("\twhen client has raised readiness and no client has registered yet")
		{
			r = &readiness{false, false, 0}

			RaiseReady()

			recorder := httptest.NewRecorder()

			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(response.Body)

			expectedStatusCode := http.StatusServiceUnavailable
			msg := fmt.Sprintf("\t\treadiness handler must return http status %d", expectedStatusCode)
			if response.StatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen one client has raised readiness and another raises not ready afterwards")
		{
			r = &readiness{false, false, 0}

			RaiseReady()
			RaiseNotReady()

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(body io.ReadCloser) {
				_ = body.Close()
			}(response.Body)

			msg := "\t\treadiness handler must return 503"
			expectedStatusCode := http.StatusServiceUnavailable
			actualStatusCode := response.StatusCode
			if actualStatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%d', got '%d'\n", expectedStatusCode, actualStatusCode))
			}

		}

		t.Log("\twhen client signals readiness after others have signalled readiness and non-readiness")
		{
			r = &readiness{false, false, 0}

			RaiseNotReady()
			RaiseReady()
			RaiseNotReady()
			RaiseReady()

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(body io.ReadCloser) {
				_ = body.Close()
			}(response.Body)

			msg := "\t\treadiness handler must return 200"
			expectedStatusCode := http.StatusOK
			actualStatusCode := response.StatusCode
			if actualStatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%d', got '%d'\n", expectedStatusCode, actualStatusCode))
			}
		}

		t.Log("\twhen client invoke raise readiness without having invoked raise not ready first")
		{
			r = &readiness{false, false, 0}

			RaiseReady()
			RaiseNotReady()
			RaiseReady()

			recorder := httptest.NewRecorder()
			readinessHandler(recorder, request)

			response := recorder.Result()
			defer func(body io.ReadCloser) {
				_ = body.Close()
			}(response.Body)

			msg := "\t\treadiness handler must return 503"
			expectedStatusCode := http.StatusServiceUnavailable
			actualStatusCode := response.StatusCode
			if actualStatusCode == expectedStatusCode {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("expected '%d', got '%d'\n", expectedStatusCode, actualStatusCode))
			}
		}

	}

}

func parseChaosMonkeyNumberValuesBackToInt(m map[string]any) {

	m[statusKeyNumRuns] = int(m[statusKeyNumRuns].(float64))
	m[statusKeyNumMembersKilled] = int(m[statusKeyNumMembersKilled].(float64))

}

func parseRunnerNumberValuesBackToInt(m map[string]any) {

	m[statusKeyNumMaps] = int(m[statusKeyNumMaps].(float64))
	m[statusKeyNumRuns] = int(m[statusKeyNumRuns].(float64))
	m[statusKeyTotalRuns] = int(m[statusKeyTotalRuns].(float64))

}

func tryResponseRead(body io.ReadCloser) ([]byte, error) {

	if data, err := io.ReadAll(body); err == nil {
		return data, nil
	} else {
		return nil, errors.New("unable to read response body")
	}

}
