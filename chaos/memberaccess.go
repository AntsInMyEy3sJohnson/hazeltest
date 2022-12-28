package chaos

import (
	"context"
	"errors"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"math/rand"
	"path/filepath"
)

type (
	k8sConfigProvider interface {
		getOrInit(ac memberAccessConfig) (*rest.Config, error)
	}
	defaultK8sConfigProvider struct {
		config *rest.Config
	}
	k8sHzMemberChooser struct {
		configProvider k8sConfigProvider
	}
	k8sHzMemberKiller struct {
		configProvider k8sConfigProvider
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

func (i *defaultK8sConfigProvider) getOrInit(ac memberAccessConfig) (*rest.Config, error) {

	if i.config == nil {
		if ac.memberAccessMode == k8sOutOfClusterAccessMode {
			var kubeconfig string
			if ac.k8sOutOfClusterMemberAccess.kubeconfig == "default" {
				kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
			} else {
				kubeconfig = ac.k8sOutOfClusterMemberAccess.kubeconfig
			}
			config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
			if err != nil {
				// TODO Add logging
				return nil, err
			}
			i.config = config
		} else if ac.memberAccessMode == k8sInClusterAccessMode {
			config, err := rest.InClusterConfig()
			if err != nil {
				return nil, err
			}
			i.config = config
		} else {
			// TODO Introduce dedicated error types?
			return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
		}
	}

	return i.config, nil

}

func (chooser *k8sHzMemberChooser) choose(ac memberAccessConfig) (hzMember, error) {

	k8sConfig, err := chooser.configProvider.getOrInit(ac)
	if err != nil {
		return hzMember{}, err
	}

	clientset, err := kubernetes.NewForConfig(k8sConfig)
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
	podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
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

	k8sConfig, err := killer.configProvider.getOrInit(ac)
	if err != nil {
		return err
	}

	clientset, err := kubernetes.NewForConfig(k8sConfig)

	namespace := ac.namespace
	podInterface := clientset.CoreV1().Pods(namespace)

	ctx := context.TODO()
	podToKill, err := podInterface.Get(ctx, m.identifier, metav1.GetOptions{})
	if err != nil {
		return err
	}

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
	err = podInterface.Delete(ctx, podToKill.Name, metav1.DeleteOptions{GracePeriodSeconds: &g})

	if err != nil {
		return err
	}

	return nil

}
