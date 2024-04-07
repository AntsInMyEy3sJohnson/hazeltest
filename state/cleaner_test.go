package state

import (
	"fmt"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		dummyConfig map[string]any
	}
)

const (
	checkMark               = "\u2713"
	ballotX                 = "\u2717"
	mapStateCleanerBasePath = "stateCleaner.maps"
)

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
