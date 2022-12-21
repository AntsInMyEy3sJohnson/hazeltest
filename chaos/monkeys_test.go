package chaos

import (
	"errors"
	"testing"
)

type (
	testConfigPropertyAssigner struct {
		returnError bool
		dummyConfig map[string]any
	}
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

var (
	monkeyKeyPath = "memberKiller"
	testConfig    = map[string]any{
		monkeyKeyPath + ".enabled":                                    true,
		monkeyKeyPath + ".stopWhenRunnersFinished":                    true,
		monkeyKeyPath + ".chaosProbability":                           0.6,
		monkeyKeyPath + ".memberAccess.mode":                          "k8sOutOfCluster",
		monkeyKeyPath + ".memberAccess.k8sOutOfCluster.kubeconfig":    "default",
		monkeyKeyPath + ".memberAccess.k8sOufOfCluster.namespace":     "hazelcastplatform",
		monkeyKeyPath + ".memberAccess.k8sOutOfCluster.labelSelector": "app.kubernetes.io/name=hazelcastimdg",
		monkeyKeyPath + ".memberAccess.k8sInCluster.labelSelector":    "app.kubernetes.io/name=hazelcastimdg",
		monkeyKeyPath + ".sleep.enabled":                              true,
		monkeyKeyPath + ".sleep.durationSeconds":                      600,
		monkeyKeyPath + ".sleep.enableRandomness":                     false,
		monkeyKeyPath + ".memberGrace.enabled":                        true,
		monkeyKeyPath + ".memberGrace.durationSeconds":                30,
		monkeyKeyPath + ".memberGrace.enableRandomness":               true,
	}
)

func (a testConfigPropertyAssigner) Assign(keyPath string, eval func(string, any) error, assign func(any)) error {

	if a.returnError {
		return errors.New("something somewhere went terribly wrong")
	}

	if value, ok := a.dummyConfig[keyPath]; ok {
		if err := eval(keyPath, value); err != nil {
			return err
		}
		assign(value)
	}

	return nil

}

func TestPopulateConfig(t *testing.T) {

	t.Log("given the need to test populating the chaos monkey config")
	{
		b := monkeyConfigBuilder{monkeyKeyPath: monkeyKeyPath}
		t.Log("\twhen property assignment does not yield an error")
		{
			propertyAssigner = testConfigPropertyAssigner{false, testConfig}
			mc, err := b.populateConfig()

			msg := "\t\tno errors should be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig should be returned"
			if mc != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig should contain correct values"
			if configValuesAsExpected(mc, testConfig) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func configValuesAsExpected(mc *monkeyConfig, expected map[string]any) bool {

	return mc.enabled == expected[monkeyKeyPath+".enabled"] &&
		mc.stopWhenRunnersFinished == expected[monkeyKeyPath+".stopWhenRunnersFinished"] &&
		mc.chaosProbability == expected[monkeyKeyPath+".chaosProbability"] &&
		string(mc.accessConfig.memberAccessMode) == expected[monkeyKeyPath+".memberAccess.mode"] &&
		mc.accessConfig.k8sOutOfClusterMemberAccess.kubeconfig == expected[monkeyKeyPath+".memberAccess.k8sOutOfCluster.kubeconfig"] &&
		mc.accessConfig.k8sOutOfClusterMemberAccess.namespace == expected[monkeyKeyPath+".memberAccess.k8sOufOfCluster.namespace"] &&
		mc.accessConfig.k8sOutOfClusterMemberAccess.labelSelector == expected[monkeyKeyPath+".memberAccess.k8sOutOfCluster.labelSelector"] &&
		mc.accessConfig.k8sInClusterMemberAccess.labelSelector == expected[monkeyKeyPath+".memberAccess.k8sInCluster.labelSelector"] &&
		mc.sleep.enabled == expected[monkeyKeyPath+".sleep.enabled"] &&
		mc.sleep.durationSeconds == expected[monkeyKeyPath+".sleep.durationSeconds"] &&
		mc.sleep.enableRandomness == expected[monkeyKeyPath+".sleep.enableRandomness"] &&
		mc.memberGrace.enabled == expected[monkeyKeyPath+".memberGrace.enabled"] &&
		mc.memberGrace.durationSeconds == expected[monkeyKeyPath+".memberGrace.durationSeconds"] &&
		mc.memberGrace.enableRandomness == expected[monkeyKeyPath+".memberGrace.enableRandomness"]

}
