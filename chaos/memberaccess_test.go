package chaos

import (
	"k8s.io/client-go/rest"
	"testing"
)

type (
	testK8sConfigProvider struct{}
)

func (t *testK8sConfigProvider) getOrInit(_ memberAccessConfig) (*rest.Config, error) {

	return &rest.Config{}, nil

}

func TestChooseMemberOnK8s(t *testing.T) {

	t.Log("given the need to test choosing hazelcast member on kubernetes")
	{
		t.Log("\twhen member to be killed can be identified")
		{
			memberChooser := k8sHzMemberChooser{configProvider: &testK8sConfigProvider{}}
			_, err := memberChooser.choose(assembleDummyAccessConfig())

			msg := "\t\tno error must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func assembleDummyAccessConfig() memberAccessConfig {

	return memberAccessConfig{
		memberAccessMode: k8sOutOfClusterAccessMode,
		targetOnlyActive: true,
		k8sOutOfClusterMemberAccess: k8sOutOfClusterMemberAccess{
			kubeconfig:    "default",
			namespace:     "hazelcastplatform",
			labelSelector: "app.kubernetes.io/name=hazelcastimdg",
		},
		k8sInClusterMemberAccess: k8sInClusterMemberAccess{},
	}

}
