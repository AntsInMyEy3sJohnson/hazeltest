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
	testK8sPodLister struct {
		podsToReturn []v1.Pod
	}
)

var (
	testBuilder              = &testK8sConfigBuilder{}
	testClientsetInitializer = &testK8sClientsetInitializer{testBuilder}
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

	return &v1.PodList{Items: l.podsToReturn}, nil

}

func assembleReadyPod(name string) v1.Pod {

	return v1.Pod{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.PodSpec{},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

}

func TestChooseMemberOnK8s(t *testing.T) {

	t.Log("given the need to test choosing hazelcast member on kubernetes")
	{
		t.Log("\twhen member to be killed can be identified")
		{
			pods := []v1.Pod{assembleReadyPod("hazelcastimdg-0"), assembleReadyPod("hazelcastimdg-1"), assembleReadyPod("hazelcastimdg-2")}
			memberChooser := k8sHzMemberChooser{testClientsetInitializer, &testK8sPodLister{pods}}
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
