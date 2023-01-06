package chaos

import (
	"context"
	"errors"
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
		returnError bool
	}
	testK8sPodLister struct {
		podsToReturn   []v1.Pod
		returnError    bool
		numInvocations int
	}
)

var (
	clientsetInitError = errors.New("lo and behold, the error everyone told you was never going to happen")
	podListError       = errors.New("another impossible error")
)

var (
	testBuilder      = &testK8sConfigBuilder{}
	csInitializer    = &testK8sClientsetInitializer{testBuilder, false}
	errCsInitializer = &testK8sClientsetInitializer{testBuilder, true}
	emptyMember      = hzMember{}
)

func (b *testK8sConfigBuilder) buildForOutOfClusterAccess(_, _ string) (*rest.Config, error) {

	return &rest.Config{}, nil

}

func (b *testK8sConfigBuilder) buildForInClusterAccess() (*rest.Config, error) {

	return &rest.Config{}, nil

}

func (t *testK8sClientsetInitializer) getOrInit(_ memberAccessConfig) (*kubernetes.Clientset, error) {

	if t.returnError {
		return nil, clientsetInitError
	}

	return &kubernetes.Clientset{}, nil

}

func (l *testK8sPodLister) list(_ *kubernetes.Clientset, _ context.Context, _ string, _ metav1.ListOptions) (*v1.PodList, error) {

	l.numInvocations++

	if l.returnError {
		return nil, podListError
	}

	return &v1.PodList{Items: l.podsToReturn}, nil

}

func assemblePod(name string, ready bool) v1.Pod {

	var readyCondition v1.ConditionStatus
	if ready {
		readyCondition = v1.ConditionTrue
	} else {
		readyCondition = v1.ConditionFalse
	}

	return v1.Pod{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1.PodSpec{},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: readyCondition,
				},
			},
		},
	}

}

func TestChooseMemberOnK8s(t *testing.T) {

	t.Log("given the need to test choosing a hazelcast member on kubernetes")
	{
		t.Log("\twhen clientset cannot be initialized")
		{
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{errCsInitializer, podLister}
			member, err := memberChooser.choose(assembleDummyAccessConfig(true))

			msg := "\t\terror must be returned"
			if err != nil && err == clientsetInitError {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned member must be empty"
			if member == emptyMember {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpod lister must have no invocations"
			if podLister.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen label selector cannot be determined")
		{
			ac := assembleDummyAccessConfig(true)
			ac.memberAccessMode = "awesomeUnknownMemberAccessMode"
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csInitializer, podLister}
			member, err := memberChooser.choose(ac)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tempty member must be returned"
			if member == emptyMember {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpod lister must have no invocations"
			if podLister.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen pod lister returns error")
		{
			podLister := &testK8sPodLister{[]v1.Pod{}, true, 0}
			memberChooser := k8sHzMemberChooser{csInitializer, podLister}
			member, err := memberChooser.choose(assembleDummyAccessConfig(true))

			msg := "\t\terror must be returned"
			if err != nil && err == podListError {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tempty member must be returned"
			if member == emptyMember {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpod lister must have one invocation"
			if podLister.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen no pods are present")
		{
			memberChooser := k8sHzMemberChooser{csInitializer,
				&testK8sPodLister{[]v1.Pod{}, false, 0}}
			member, err := memberChooser.choose(assembleDummyAccessConfig(true))

			msg := "\t\terror must be returned"
			if err != nil && err == noMemberFoundError {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tempty member must be returned"
			if member == emptyMember {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen ready pod is present and target only active is activated")
		{
			pod := assemblePod("hazelcastimdg-0", true)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csInitializer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(assembleDummyAccessConfig(true))

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tname of selected member must be equal to name of ready pod"
			if member.identifier == pod.Name {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen only non-ready pods are active and target only active is activated")
		{
			pod := assemblePod("hazelcastimdg-0", false)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csInitializer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(assembleDummyAccessConfig(true))

			msg := "\t\terror must be returned"
			if err != nil && err == noMemberFoundError {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tempty member must be returned"
			if member == emptyMember {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen pods are present and target only active is not activated")
		{
			pod := assemblePod("hazelcastimdg-0", false)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csInitializer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(assembleDummyAccessConfig(false))

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tname of chosen member must correspond to name of given pod"
			if member.identifier == pod.Name {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func assembleDummyAccessConfig(targetOnlyActive bool) memberAccessConfig {

	return memberAccessConfig{
		memberAccessMode: k8sOutOfClusterAccessMode,
		targetOnlyActive: targetOnlyActive,
		k8sOutOfClusterMemberAccess: k8sOutOfClusterMemberAccess{
			kubeconfig:    "default",
			namespace:     "hazelcastplatform",
			labelSelector: "app.kubernetes.io/name=hazelcastimdg",
		},
		k8sInClusterMemberAccess: k8sInClusterMemberAccess{},
	}

}
