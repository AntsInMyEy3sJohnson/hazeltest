package chaos

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"math/rand"
	"path/filepath"
)

type (
	k8sHzMemberChooser struct{}
	k8sHzMemberKiller  struct{}
)

func (chooser *k8sHzMemberChooser) choose() (hzMember, error) {

	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return hzMember{}, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return hzMember{}, err
	}

	namespace := "hazelcastplatform"
	labelSelector := "app.kubernetes.io/name=hazelcastimdg"
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

func (killer *k8sHzMemberKiller) kill(m hzMember) error {

	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)

	namespace := "hazelcastplatform"
	podInterface := clientset.CoreV1().Pods(namespace)

	ctx := context.TODO()
	podToKill, err := podInterface.Get(ctx, m.identifier, metav1.GetOptions{})
	if err != nil {
		return err
	}

	grace := int64(0)
	err = podInterface.Delete(ctx, podToKill.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace})

	if err != nil {
		return err
	}

	return nil

}
