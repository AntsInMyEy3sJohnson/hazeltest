package chaos

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

type (
	k8sConfigBuilder interface {
		buildForOutOfClusterAccess(masterUrl, kubeconfigPath string) (*rest.Config, error)
		buildForInClusterAccess() (*rest.Config, error)
	}
	k8sClientsetInitializer interface {
		init(c *rest.Config) (*kubernetes.Clientset, error)
	}
	k8sClientsetProvider interface {
		getOrInit(ac memberAccessConfig) (*kubernetes.Clientset, error)
	}
	k8sNamespaceDiscoverer interface {
		discover() (string, error)
	}
	k8sPodLister interface {
		list(cs *kubernetes.Clientset, ctx context.Context, namespace string, listOptions metav1.ListOptions) (*v1.PodList, error)
	}
	k8sPodDeleter interface {
		delete(cs *kubernetes.Clientset, ctx context.Context, namespace, name string, deleteOptions metav1.DeleteOptions) error
	}
	defaultK8sConfigBuilder        struct{}
	defaultK8sClientsetInitializer struct{}
	defaultK8sClientsetProvider    struct {
		configBuilder        k8sConfigBuilder
		clientsetInitializer k8sClientsetInitializer
		cs                   *kubernetes.Clientset
	}
	defaultK8sNamespaceDiscoverer struct{}
	defaultK8sPodLister           struct{}
	defaultK8sPodDeleter          struct{}
	k8sHzMemberChooser            struct {
		clientsetProvider   k8sClientsetProvider
		namespaceDiscoverer k8sNamespaceDiscoverer
		podLister           k8sPodLister
	}
	k8sHzMemberKiller struct {
		clientsetProvider   k8sClientsetProvider
		namespaceDiscoverer k8sNamespaceDiscoverer
		podDeleter          k8sPodDeleter
	}
)

var (
	noMemberFoundError = errors.New("unable to identify hazelcast member to be terminated")
)

func determineK8sLabelSelector(ac memberAccessConfig) (string, error) {

	switch ac.memberAccessMode {
	case k8sOutOfClusterAccessMode:
		return ac.k8sOutOfCluster.labelSelector, nil
	case k8sInClusterAccessMode:
		return ac.k8sInCluster.labelSelector, nil
	default:
		return "", fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

}

func (b *defaultK8sConfigBuilder) buildForOutOfClusterAccess(masterUrl, kubeconfigPath string) (*rest.Config, error) {

	return clientcmd.BuildConfigFromFlags(masterUrl, kubeconfigPath)

}

func (b *defaultK8sConfigBuilder) buildForInClusterAccess() (*rest.Config, error) {

	return rest.InClusterConfig()

}

func (i *defaultK8sClientsetInitializer) init(c *rest.Config) (*kubernetes.Clientset, error) {

	return kubernetes.NewForConfig(c)

}

func (d *defaultK8sNamespaceDiscoverer) discover() (string, error) {

	if ns, ok := os.LookupEnv("POD_NAMESPACE"); ok {
		return ns, nil
	}

	if data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err != nil {
		return "", err
	} else {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns, nil
		}
	}

	return "", errors.New("kubernetes namespace discovery failed: namespace neither present in environment variable 'POD_NAMESPACE' nor in serviceaccount file")

}

func (l *defaultK8sPodLister) list(cs *kubernetes.Clientset, ctx context.Context, namespace string, listOptions metav1.ListOptions) (*v1.PodList, error) {

	if podList, err := cs.CoreV1().Pods(namespace).List(ctx, listOptions); err != nil {
		return nil, err
	} else {
		return podList, nil
	}

}

func (d *defaultK8sPodDeleter) delete(cs *kubernetes.Clientset, ctx context.Context, namespace, name string, deleteOptions metav1.DeleteOptions) error {

	if err := cs.CoreV1().Pods(namespace).Delete(ctx, name, deleteOptions); err != nil {
		return err
	} else {
		return nil
	}

}

func (p *defaultK8sClientsetProvider) getOrInit(ac memberAccessConfig) (*kubernetes.Clientset, error) {

	if p.cs != nil {
		return p.cs, nil
	}

	var config *rest.Config
	if ac.memberAccessMode == k8sOutOfClusterAccessMode {
		var kubeconfig string
		if ac.k8sOutOfCluster.kubeconfig == "default" {
			kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
		} else {
			kubeconfig = ac.k8sOutOfCluster.kubeconfig
		}
		if c, err := p.configBuilder.buildForOutOfClusterAccess("", kubeconfig); err != nil {
			return nil, err
		} else {
			config = c
		}
	} else if ac.memberAccessMode == k8sInClusterAccessMode {
		if c, err := p.configBuilder.buildForInClusterAccess(); err != nil {
			return nil, err
		} else {
			config = c
		}
	} else {
		// TODO Introduce dedicated error types?
		return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

	if cs, err := p.clientsetInitializer.init(config); err != nil {
		return nil, err
	} else {
		p.cs = cs
	}

	return p.cs, nil

}

func (chooser *k8sHzMemberChooser) choose(ac memberAccessConfig) (hzMember, error) {

	clientset, err := chooser.clientsetProvider.getOrInit(ac)
	if err != nil {
		return hzMember{}, err
	}

	var namespace string
	if ac.memberAccessMode == k8sOutOfClusterAccessMode {
		namespace = ac.k8sOutOfCluster.namespace
	} else {
		if ns, err := chooser.namespaceDiscoverer.discover(); err != nil {
			return hzMember{}, err
		} else {
			namespace = ns
		}
	}

	var labelSelector string
	if s, err := determineK8sLabelSelector(ac); err != nil {
		return hzMember{}, err
	} else {
		labelSelector = s
	}

	ctx := context.TODO()
	podList, err := chooser.podLister.list(clientset, ctx, namespace, metav1.ListOptions{LabelSelector: labelSelector})
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

	clientset, err := killer.clientsetProvider.getOrInit(ac)
	if err != nil {
		return err
	}

	var namespace string
	if ac.memberAccessMode == k8sOutOfClusterAccessMode {
		namespace = ac.k8sOutOfCluster.namespace
	} else {
		if ns, err := killer.namespaceDiscoverer.discover(); err != nil {
			return err
		} else {
			namespace = ns
		}
	}

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
	err = killer.podDeleter.delete(clientset, ctx, namespace, m.identifier, metav1.DeleteOptions{GracePeriodSeconds: &g})

	if err != nil {
		return err
	}

	return nil

}
