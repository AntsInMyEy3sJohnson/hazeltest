package chaos

import (
	"context"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"testing"
)

type (
	testK8sConfigBuilder        struct{}
	testK8sClientsetInitializer struct {
		k8sConfigBuilder
	}
	testK8sPodLister struct{}
)

var (
	testBuilder              = &testK8sConfigBuilder{}
	testClientsetInitializer = &testK8sClientsetInitializer{testBuilder}
	testPodLister            = &testK8sPodLister{}
)

func (b *testK8sConfigBuilder) buildForOutOfClusterAccess(_, _ string) (*rest.Config, error) {

	return &rest.Config{}, nil

}

func (b *testK8sConfigBuilder) buildForInClusterAccess() (*rest.Config, error) {

	return &rest.Config{}, nil

}

func (t *testK8sClientsetInitializer) getOrInit(_ memberAccessConfig) (*kubernetes.Clientset, error) {

	return &kubernetes.Clientset{}, nil

}

func (l *testK8sPodLister) list(_ *kubernetes.Clientset, _ context.Context, _ string, _ metav1.ListOptions) (*v1.PodList, error) {

	return &v1.PodList{}, nil

}

func TestChooseMemberOnK8s(t *testing.T) {

	t.Log("given the need to test choosing hazelcast member on kubernetes")
	{
		t.Log("\twhen member to be killed can be identified")
		{
			memberChooser := k8sHzMemberChooser{testClientsetInitializer, testPodLister}
			_, err := memberChooser.choose(assembleDummyAccessConfig())

			msg := "\t\tno error must be returned"
			if err == nil {
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
