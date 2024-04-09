package state

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		dummyConfig map[string]any
	}
	testCleanerBuilder struct {
		behavior         *testCleanerBehavior
		buildInvocations int
	}
	testCleanerBehavior struct {
		throwErrorUponBuild, throwErrorUponClean bool
	}
	testCleaner struct {
		behavior *testCleanerBehavior
	}
	cleanerWatcher struct {
		m                sync.Mutex
		cleanInvocations int
	}
)

const (
	checkMark               = "\u2713"
	ballotX                 = "\u2717"
	mapStateCleanerBasePath = "stateCleaner.maps"
	hzCluster               = "awesome-hz-cluster"
)

var (
	hzMembers         = []string{"awesome-hz-member:5701", "another-awesome-hz-member:5701"}
	cw                = cleanerWatcher{}
	cleanerBuildError = errors.New("something went terribly wrong when attempting to build the cleaner")
	cleanerCleanError = errors.New("something went terribly wrong when attempting to clean state")
)

func (cw *cleanerWatcher) reset() {
	cw.m = sync.Mutex{}

	cw.cleanInvocations = 0
}

func (c *testCleaner) clean() error {

	cw.m.Lock()
	defer cw.m.Unlock()

	cw.cleanInvocations++

	if c.behavior.throwErrorUponClean {
		return cleanerCleanError
	}

	return nil

}

func (b *testCleanerBuilder) build() (cleaner, error) {

	b.buildInvocations++

	if b.behavior.throwErrorUponBuild {
		return nil, cleanerBuildError
	}

	return &testCleaner{behavior: b.behavior}, nil

}

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	} else {
		return fmt.Errorf("test error: unable to find value in dummy config for given key path '%s'", keyPath)
	}

	return nil

}

func TestRunCleaners(t *testing.T) {

	t.Log("given a function to invoke registered state cleaner builders")
	{
		t.Log("\twhen at least one state cleaner builder has registered")
		{
			t.Log("\t\twhen both build and clean invocations are successful")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}

					msg = "\t\t\tbuilder's build method must have been invoked once"
					if b.buildInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, b.buildInvocations)
					}

					msg = "\t\t\tclean method must have been invoked once"
					if cw.cleanInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, cw.cleanInvocations)
					}
				})
			}
			t.Log("\t\twhen build invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						throwErrorUponBuild: true,
					}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during build must be returned"
					if errors.Is(err, cleanerBuildError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}
			t.Log("\t\twhen clean invocation yields error")
			{
				runTestCaseAndResetState(func() {
					b := &testCleanerBuilder{behavior: &testCleanerBehavior{
						throwErrorUponClean: true,
					}}
					builders = []cleanerBuilder{b}

					err := RunCleaners(hzCluster, hzMembers)

					msg := "\t\t\terror during clean must be returned"
					if errors.Is(err, cleanerCleanError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX, err)
					}
				})
			}

		}
	}

}

func runTestCaseAndResetState(testFunc func()) {

	defer cw.reset()
	testFunc()

}

func TestMapCleanerBuilderBuild(t *testing.T) {

	t.Log("given a method to build a map cleaner builder")
	{
		t.Log("\twhen populate config is successful")
		{
			b := newMapCleanerBuilder()
			b.cfb.a = &testConfigPropertyAssigner{dummyConfig: assembleTestConfig()}

			c, err := b.build()

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, err)
			}

			msg = "\t\tmap cleaner built must carry map state cleaner key path"
			mc := c.(*mapCleaner)

			if mc.keyPath == mapStateCleanerBasePath {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, mc.keyPath)
			}

			msg = "\t\tmap cleaner built must carry state cleaner config"
			if mc.c != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
	}

}

func assembleTestConfig() map[string]any {

	return map[string]any{
		mapStateCleanerBasePath + ".enabled":        true,
		mapStateCleanerBasePath + ".prefix.enabled": true,
		mapStateCleanerBasePath + ".prefix.prefix":  "ht_",
	}

}
