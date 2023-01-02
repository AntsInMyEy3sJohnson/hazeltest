package chaos

import (
	"context"
	"errors"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/homedir"
	"math/rand"
	"path/filepath"
)

type (
	k8sConfigBuilder interface {
		buildForOutOfClusterAccess(masterUrl, kubeconfigPath string) (*rest.Config, error)
		buildForInClusterAccess() (*rest.Config, error)
	}
	k8sClientSetInitializer interface {
		getOrInit(ac memberAccessConfig) (*kubernetes.Clientset, error)
	}
	k8sPodLister interface {
		list(cs *kubernetes.Clientset, ctx context.Context, namespace string, listOptions metav1.ListOptions) (*v1.PodList, error)
	}
	k8sPodDeleter interface {
		delete(cs *kubernetes.Clientset, ctx context.Context, namespace, name string, deleteOptions metav1.DeleteOptions) error
	}
	defaultK8sClientSetInitializer struct {
		k8sConfigBuilder
		cs *kubernetes.Clientset
	}
	defaultK8sPodLister  struct{}
	defaultK8sPodDeleter struct{}
	k8sHzMemberChooser   struct {
		k8sClientSetInitializer
		k8sPodLister
	}
	k8sHzMemberKiller struct {
		k8sClientSetInitializer
		k8sPodDeleter
	}
)

var (
	noMemberFoundError = errors.New("unable to identify hazelcast member to be terminated")
)

func determineK8sLabelSelector(ac memberAccessConfig) (string, error) {

	switch ac.memberAccessMode {
	case k8sOutOfClusterAccessMode:
		return ac.k8sOutOfClusterMemberAccess.labelSelector, nil
	case k8sInClusterAccessMode:
		return ac.k8sInClusterMemberAccess.labelSelector, nil
	default:
		return "", fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

}

func (lister *defaultK8sPodLister) list(cs *kubernetes.Clientset, ctx context.Context, namespace string, listOptions metav1.ListOptions) (*v1.PodList, error) {

	if podList, err := cs.CoreV1().Pods(namespace).List(ctx, listOptions); err != nil {
		return nil, err
	} else {
		return podList, nil
	}

}

func (deleter *defaultK8sPodDeleter) delete(cs *kubernetes.Clientset, ctx context.Context, namespace, name string, deleteOptions metav1.DeleteOptions) error {

	if err := cs.CoreV1().Pods(namespace).Delete(ctx, name, deleteOptions); err != nil {
		return err
	} else {
		return nil
	}

}

func (w *defaultK8sClientSetInitializer) getOrInit(ac memberAccessConfig) (*kubernetes.Clientset, error) {

	if w.cs != nil {
		return w.cs, nil
	}

	var config *rest.Config
	if ac.memberAccessMode == k8sOutOfClusterAccessMode {
		var kubeconfig string
		if ac.k8sOutOfClusterMemberAccess.kubeconfig == "default" {
			kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
		} else {
			kubeconfig = ac.k8sOutOfClusterMemberAccess.kubeconfig
		}
		if c, err := w.buildForOutOfClusterAccess("", kubeconfig); err != nil {
			return nil, err
		} else {
			config = c
		}
	} else if ac.memberAccessMode == k8sInClusterAccessMode {
		if c, err := w.buildForInClusterAccess(); err != nil {
			return nil, err
		} else {
			config = c
		}
	} else {
		// TODO Introduce dedicated error types?
		return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

	if cs, err := kubernetes.NewForConfig(config); err != nil {
		return nil, err
	} else {
		w.cs = cs
	}

	return w.cs, nil

}

func (chooser *k8sHzMemberChooser) choose(ac memberAccessConfig) (hzMember, error) {

	clientset, err := chooser.getOrInit(ac)
	if err != nil {
		return hzMember{}, err
	}

	namespace := ac.namespace
	var labelSelector string
	if s, err := determineK8sLabelSelector(ac); err != nil {
		return hzMember{}, err
	} else {
		labelSelector = s
	}

	ctx := context.TODO()
	podList, err := chooser.list(clientset, ctx, namespace, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return hzMember{}, err
	}

	pods := podList.Items

	if len(pods) == 0 {
		return hzMember{}, noMemberFoundError
	}

	var podToKill v1.Pod
	podFound := false

	if ac.targetOnlyActive {
		for i := 0; i < len(pods); i++ {
			candidate := selectRandomPodFromList(pods)
			if isPodReady(candidate) {
				podToKill = candidate
				podFound = true
				break
			}
		}
		if !podFound {
			return hzMember{}, noMemberFoundError
		}
	} else {
		podToKill = selectRandomPodFromList(pods)
	}

	return hzMember{podToKill.Name}, nil

}

func selectRandomPodFromList(pods []v1.Pod) v1.Pod {

	randomIndex := rand.Intn(len(pods))
	return pods[randomIndex]

}

func isPodReady(p v1.Pod) bool {

	podConditions := p.Status.Conditions

	for _, condition := range podConditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}

	return false

}

func (killer *k8sHzMemberKiller) kill(m hzMember, ac memberAccessConfig, memberGrace sleepConfig) error {

	clientset, err := killer.k8sClientSetInitializer.getOrInit(ac)

	namespace := ac.namespace
	ctx := context.TODO()

	var gracePeriod int
	if memberGrace.enabled {
		if memberGrace.enableRandomness {
			gracePeriod = rand.Intn(memberGrace.durationSeconds + 1)
		} else {
			gracePeriod = memberGrace.durationSeconds
		}
	} else {
		gracePeriod = 0
	}

	g := int64(gracePeriod)
	err = killer.delete(clientset, ctx, namespace, m.identifier, metav1.DeleteOptions{GracePeriodSeconds: &g})

	if err != nil {
		return err
	}

	return nil

}
