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

func RunMonkey() {

	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	namespace := "hazelcastplatform"
	labelSelector := "app.kubernetes.io/name=hazelcastimdg"
	ctx := context.TODO()
	podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		panic(err.Error())
	}

	pods := podList.Items

	randomIndex := rand.Intn(len(pods))
	podToKill := pods[randomIndex]

	podInterface := clientset.CoreV1().Pods(namespace)
	grace := int64(0)
	err = podInterface.Delete(ctx, podToKill.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace})

	if err != nil {
		panic(err.Error())
	}

}
