package chaos

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"math/rand"
	"path/filepath"
)

type (
	k8sHzMemberChooser struct{}
	k8sHzMemberKiller  struct{}
)

var (
	k8sConfig *rest.Config
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

func initK8sConfig(ac memberAccessConfig) (*rest.Config, error) {

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
		return config, nil
	} else if ac.memberAccessMode == k8sInClusterAccessMode {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		return config, nil
	} else {
		// TODO Introduce dedicated error types?
		return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

}

func (chooser *k8sHzMemberChooser) choose(ac memberAccessConfig) (hzMember, error) {

	if k8sConfig == nil {
		if config, err := initK8sConfig(ac); err != nil {
			return hzMember{}, err
		} else {
			k8sConfig = config
		}
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

	randomIndex := rand.Intn(len(pods))
	podToKill := pods[randomIndex]

	return hzMember{podToKill.Name}, nil

}

func (killer *k8sHzMemberKiller) kill(m hzMember, ac memberAccessConfig, memberGrace sleepConfig) error {

	if k8sConfig == nil {
		if config, err := initK8sConfig(ac); err != nil {
			return err
		} else {
			k8sConfig = config
		}
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
