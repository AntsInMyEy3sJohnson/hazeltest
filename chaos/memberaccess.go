package chaos

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
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
		getOrDiscover(ac memberAccessConfig) (string, error)
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
	defaultK8sNamespaceDiscoverer struct {
		discoveredNamespace string
	}
	defaultK8sPodLister  struct{}
	defaultK8sPodDeleter struct{}
	k8sHzMemberChooser   struct {
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

const (
	k8sNamespaceEnvVariable = "POD_NAMESPACE"
)

var (
	noMemberFoundError = errors.New("unable to identify hazelcast member to be terminated")
)

func labelSelectorFromConfig(ac memberAccessConfig) (string, error) {

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

func (d *defaultK8sNamespaceDiscoverer) getOrDiscover(ac memberAccessConfig) (string, error) {

	if d.discoveredNamespace != "" {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("namespace has already been populated -- returning '%s'", d.discoveredNamespace), log.TraceLevel)
		return d.discoveredNamespace, nil
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("performing kubernetes namespace discovery for access mode '%s'", ac.memberAccessMode), log.TraceLevel)

	var namespace string

	switch ac.memberAccessMode {
	case k8sOutOfClusterAccessMode:
		namespace = ac.k8sOutOfCluster.namespace
	case k8sInClusterAccessMode:
		lp.LogChaosMonkeyEvent(fmt.Sprintf("attempting to look up kubernetes namespace using env variable, '%s'", k8sNamespaceEnvVariable), log.TraceLevel)
		if ns, ok := os.LookupEnv(k8sNamespaceEnvVariable); ok {
			namespace = ns
		}
		lp.LogChaosMonkeyEvent("attempting to look up kubernetes namespace using pod-mounted file", log.TraceLevel)
		if data, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err != nil {
			return "", err
		} else {
			if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
				namespace = ns
			}
		}
		msg := fmt.Sprintf("kubernetes namespace discovery failed: namespace neither present in environment variable '%s' nor in serviceaccount file", k8sNamespaceEnvVariable)
		lp.LogChaosMonkeyEvent(msg, log.ErrorLevel)
		return "", errors.New(msg)
	default:
		msg := fmt.Sprintf("cannot perform kubernetes namespace discovery for member access mode '%s' -- access mode either unknown or unrelated to kubernetes", ac.memberAccessMode)
		lp.LogChaosMonkeyEvent(msg, log.ErrorLevel)
		return "", errors.New(msg)
	}

	d.discoveredNamespace = namespace
	return namespace, nil

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
		lp.LogChaosMonkeyEvent("kubernetes clientset already present -- returning previously initialized state", log.TraceLevel)
		return p.cs, nil
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("initializing kubernetes clientset for access mode '%s'", ac.memberAccessMode), log.InfoLevel)

	var config *rest.Config
	if ac.memberAccessMode == k8sOutOfClusterAccessMode {
		var kubeconfig string
		if ac.k8sOutOfCluster.kubeconfig == "default" {
			kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
		} else {
			kubeconfig = ac.k8sOutOfCluster.kubeconfig
		}
		lp.LogChaosMonkeyEvent(fmt.Sprintf("using kubeconfig path '%s' to initialize kubernetes rest.config", kubeconfig), log.TraceLevel)
		if c, err := p.configBuilder.buildForOutOfClusterAccess("", kubeconfig); err != nil {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to initialize rest.config for accessing kubernetes in mode '%s': %s", ac.memberAccessMode, err.Error()), log.ErrorLevel)
			return nil, err
		} else {
			config = c
		}
	} else if ac.memberAccessMode == k8sInClusterAccessMode {
		if c, err := p.configBuilder.buildForInClusterAccess(); err != nil {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to initialize rest.config for accessing kubernetes in mode '%s': %s", ac.memberAccessMode, err.Error()), log.ErrorLevel)
			return nil, err
		} else {
			config = c
		}
	} else {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("cannot initialize kubernetes clientset for unknown or unsupported access mode '%s'", ac.memberAccessMode), log.ErrorLevel)
		return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.memberAccessMode)
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("successfully initialized rest.config for accessing kubernetes in mode '%s'", ac.memberAccessMode), log.TraceLevel)

	if cs, err := p.clientsetInitializer.init(config); err != nil {
		return nil, err
	} else {
		lp.LogChaosMonkeyEvent("initializing clientset using rest.config", log.TraceLevel)
		p.cs = cs
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("successfully initialized kubernetes clientset for access mode '%s'", ac.memberAccessMode), log.InfoLevel)

	return p.cs, nil

}

func (chooser *k8sHzMemberChooser) choose(ac memberAccessConfig) (hzMember, error) {

	lp.LogChaosMonkeyEvent("choosing hazelcast member", log.InfoLevel)

	clientset, err := chooser.clientsetProvider.getOrInit(ac)
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to choose hazelcast member: clientset initialization failed: %s", err.Error()), log.ErrorLevel)
		return hzMember{}, err
	}

	namespace, err := chooser.namespaceDiscoverer.getOrDiscover(ac)
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to choose hazelcast member: namespace to operate in could not be determined: %s", err.Error()), log.ErrorLevel)
		return hzMember{}, err
	}

	var labelSelector string
	if s, err := labelSelectorFromConfig(ac); err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to choose hazelcast member: could not determine label selector: %s", err.Error()), log.ErrorLevel)
		return hzMember{}, err
	} else {
		labelSelector = s
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("using label selector '%s' in namespace '%s' to choose hazelcast member", labelSelector, namespace), log.InfoLevel)

	ctx := context.TODO()
	podList, err := chooser.podLister.list(clientset, ctx, namespace, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to choose hazelcast member: could not list pods: %s", err.Error()), log.ErrorLevel)
		return hzMember{}, err
	}

	pods := podList.Items
	lp.LogChaosMonkeyEvent(fmt.Sprintf("found %d candidate pod/-s", len(pods)), log.TraceLevel)

	if len(pods) == 0 {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("no hazelcast members found for label selector '%s' in namespace '%s'", labelSelector, namespace), log.ErrorLevel)
		return hzMember{}, noMemberFoundError
	}

	var podToKill v1.Pod
	podFound := false

	if ac.targetOnlyActive {
		lp.LogChaosMonkeyEvent("targetOnlyActive enabled -- trying to find active (ready) hazelcast member pod", log.TraceLevel)
		for i := 0; i < len(pods); i++ {
			candidate := selectRandomPodFromList(pods)
			if isPodReady(candidate) {
				podToKill = candidate
				podFound = true
				break
			}
		}
		if !podFound {
			lp.LogChaosMonkeyEvent(fmt.Sprintf("out of %d candidate pods, none was ready (can only target ready pods because targetOnlyActive was enabled)", len(pods)), log.ErrorLevel)
			return hzMember{}, noMemberFoundError
		}
	} else {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("targetOnlyActive disabled -- randomly choosing one out of %d pods", len(pods)), log.TraceLevel)
		podToKill = selectRandomPodFromList(pods)
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("successfully chose hazelcast member: %s", podToKill.Name), log.InfoLevel)
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
			lp.LogChaosMonkeyEvent(fmt.Sprintf("found ready pod: %s", p.Name), log.TraceLevel)
			return true
		}
		lp.LogChaosMonkeyEvent(fmt.Sprintf("skipping condition type '%s' for pod '%s'", condition.Type, p.Name), log.TraceLevel)
	}

	return false

}

func (killer *k8sHzMemberKiller) kill(m hzMember, ac memberAccessConfig, memberGrace sleepConfig) error {

	lp.LogChaosMonkeyEvent(fmt.Sprintf("killing hazelcast member '%s'", m.identifier), log.InfoLevel)

	clientset, err := killer.clientsetProvider.getOrInit(ac)
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to kill hazelcast member: clientset initialization failed: %s", err.Error()), log.ErrorLevel)
		return err
	}

	namespace, err := killer.namespaceDiscoverer.getOrDiscover(ac)
	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("unable to kill hazelcast member: namespace to operate in could not be determined: %s", err.Error()), log.ErrorLevel)
		return err
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

	lp.LogChaosMonkeyEvent(fmt.Sprintf("using grace period seconds '%d' to kill hazelcast member '%s'", gracePeriod, m.identifier), log.TraceLevel)

	g := int64(gracePeriod)
	err = killer.podDeleter.delete(clientset, ctx, namespace, m.identifier, metav1.DeleteOptions{GracePeriodSeconds: &g})

	if err != nil {
		lp.LogChaosMonkeyEvent(fmt.Sprintf("killing hazelcast member '%s' unsuccessful: %s", m.identifier, err.Error()), log.ErrorLevel)
		return err
	}

	lp.LogChaosMonkeyEvent(fmt.Sprintf("successfully killed hazelcast member '%s' granting %d seconds of grace period", m.identifier, gracePeriod), log.InfoLevel)
	return nil

}
