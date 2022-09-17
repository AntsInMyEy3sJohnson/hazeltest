package maps

import "errors"

type (
	testConfigPropertyAssigner struct {
		returnError   bool
		runnerKeyPath string
		dummyConfig   map[string]interface{}
	}
)

const (
	checkMark     = "\u2713"
	ballotX       = "\u2717"
	runnerKeyPath = "testRunner"
	mapPrefix     = "t_"
	mapBaseName   = "test"
)

func (a testConfigPropertyAssigner) Assign(keyPath string, assignFunc func(any)) error {

	if a.returnError {
		return errors.New("deliberately thrown error")
	}

	assignFunc(a.dummyConfig[keyPath])
	return nil

}
