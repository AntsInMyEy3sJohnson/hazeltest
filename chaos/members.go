package chaos

import (
	"context"
	"errors"
	"fmt"
	"hazeltest/client"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	log "go.uber.org/zap/zapcore"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	k8sNamespaceEnvVariable = "POD_NAMESPACE"
)

const (
	atOnce  hzMemberTerminationMode = "atOnce"
	delayed hzMemberTerminationMode = "delayed"
)

var (
	noMembersFoundError      = errors.New("no members found to be terminated")
	noReadyMembersFoundError = errors.New("no active (ready) members found to be terminated")

	noMembersProvidedForKillingError   = errors.New("cannot kill hazelcast members because given list of members was nil or empty")
	noMembersProvidedToChooseFromError = errors.New("cannot choose hazelcast members to kill because list of members to choose from was either nil or empty")
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
		getOrInit(ac *memberAccessConfig) (*kubernetes.Clientset, error)
	}
	k8sNamespaceDiscoverer interface {
		getOrDiscover(ac *memberAccessConfig) (string, error)
	}
	k8sPodLister interface {
		list(cs *kubernetes.Clientset, ctx context.Context, namespace string, listOptions metav1.ListOptions) (*v1.PodList, error)
	}
	k8sPodDeleter interface {
		delete(cs *kubernetes.Clientset, ctx context.Context, namespace, name string, deleteOptions metav1.DeleteOptions) error
	}
	k8sOutOfClusterMemberAccess struct {
		kubeconfig, namespace, labelSelector string
	}
	k8sInClusterMemberAccess struct {
		labelSelector string
	}
	memberSelectionConfig struct {
		selectionMode                     string
		targetOnlyActive                  bool
		absoluteNumMembersToKill          uint8
		relativePercentageOfMembersToKill float32
	}
	memberAccessConfig struct {
		accessMode      hzOnK8sMemberAccessMode
		k8sOutOfCluster k8sOutOfClusterMemberAccess
		k8sInCluster    k8sInClusterMemberAccess
	}
	memberTerminationConfig struct {
		mode             hzMemberTerminationMode
		delaySeconds     uint8
		enableRandomness bool
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
	hzMemberTerminationMode string
)

func labelSelectorFromConfig(ac *memberAccessConfig) (string, error) {

	switch ac.accessMode {
	case k8sOutOfCluster:
		return ac.k8sOutOfCluster.labelSelector, nil
	case k8sInCluster:
		return ac.k8sInCluster.labelSelector, nil
	default:
		return "", fmt.Errorf("encountered unknown k8s access mode: %s", ac.accessMode)
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

func (d *defaultK8sNamespaceDiscoverer) getOrDiscover(ac *memberAccessConfig) (string, error) {

	if d.discoveredNamespace != "" {
		lp.Log(fmt.Sprintf("namespace has already been populated -- returning '%s'", d.discoveredNamespace), client.ChaosMonkeyEvent, log.DebugLevel)
		return d.discoveredNamespace, nil
	}

	lp.Log(fmt.Sprintf("performing kubernetes namespace discovery for access mode '%s'", ac.accessMode), client.ChaosMonkeyEvent, log.DebugLevel)

	var namespace string

	switch ac.accessMode {
	case k8sOutOfCluster:
		namespace = ac.k8sOutOfCluster.namespace
	case k8sInCluster:
		lp.Log(fmt.Sprintf("attempting to look up kubernetes namespace using env variable, '%s'", k8sNamespaceEnvVariable), client.ChaosMonkeyEvent, log.DebugLevel)
		if ns, ok := os.LookupEnv(k8sNamespaceEnvVariable); ok {
			namespace = ns
		}
		lp.Log("attempting to look up kubernetes namespace using pod-mounted file", client.ChaosMonkeyEvent, log.DebugLevel)
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err != nil {
			return "", err
		} else {
			if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
				namespace = ns
			}
		}
		if namespace == "" {
			msg := fmt.Sprintf("kubernetes namespace discovery failed: namespace neither present in environment variable '%s' nor in serviceaccount file", k8sNamespaceEnvVariable)
			lp.Log(msg, client.ChaosMonkeyEvent, log.ErrorLevel)
			return "", errors.New(msg)
		}
	default:
		msg := fmt.Sprintf("cannot perform kubernetes namespace discovery for member access mode '%s' -- access mode either unknown or unrelated to kubernetes", ac.accessMode)
		lp.Log(msg, client.ChaosMonkeyEvent, log.ErrorLevel)
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
	}

	return nil

}

func (p *defaultK8sClientsetProvider) getOrInit(ac *memberAccessConfig) (*kubernetes.Clientset, error) {

	if p.cs != nil {
		lp.Log("kubernetes clientset already present -- returning previously initialized state", client.ChaosMonkeyEvent, log.DebugLevel)
		return p.cs, nil
	}

	lp.Log(fmt.Sprintf("initializing kubernetes clientset for access mode '%s'", ac.accessMode), client.ChaosMonkeyEvent, log.InfoLevel)

	var config *rest.Config
	if ac.accessMode == k8sOutOfCluster {
		var kubeconfig string
		if ac.k8sOutOfCluster.kubeconfig == "default" {
			kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
		} else {
			kubeconfig = ac.k8sOutOfCluster.kubeconfig
		}
		lp.Log(fmt.Sprintf("using kubeconfig path '%s' to initialize kubernetes rest.config", kubeconfig), client.ChaosMonkeyEvent, log.DebugLevel)
		if c, err := p.configBuilder.buildForOutOfClusterAccess("", kubeconfig); err != nil {
			lp.Log(fmt.Sprintf("unable to initialize rest.config for accessing kubernetes in mode '%s': %s", ac.accessMode, err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
			return nil, err
		} else {
			config = c
		}
	} else if ac.accessMode == k8sInCluster {
		if c, err := p.configBuilder.buildForInClusterAccess(); err != nil {
			lp.Log(fmt.Sprintf("unable to initialize rest.config for accessing kubernetes in mode '%s': %s", ac.accessMode, err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
			return nil, err
		} else {
			config = c
		}
	} else {
		lp.Log(fmt.Sprintf("cannot initialize kubernetes clientset for unknown or unsupported access mode '%s'", ac.accessMode), client.ChaosMonkeyEvent, log.ErrorLevel)
		return nil, fmt.Errorf("encountered unknown k8s access mode: %s", ac.accessMode)
	}

	lp.Log(fmt.Sprintf("successfully initialized rest.config for accessing kubernetes in mode '%s'", ac.accessMode), client.ChaosMonkeyEvent, log.DebugLevel)

	if cs, err := p.clientsetInitializer.init(config); err != nil {
		return nil, err
	} else {
		lp.Log("initializing clientset using rest.config", client.ChaosMonkeyEvent, log.DebugLevel)
		p.cs = cs
	}

	lp.Log(fmt.Sprintf("successfully initialized kubernetes clientset for access mode '%s'", ac.accessMode), client.ChaosMonkeyEvent, log.InfoLevel)

	return p.cs, nil

}

func (chooser *k8sHzMemberChooser) choose(ac *memberAccessConfig, sc *memberSelectionConfig) ([]hzMember, error) {

	lp.Log("choosing hazelcast members", client.ChaosMonkeyEvent, log.InfoLevel)

	clientset, err := chooser.clientsetProvider.getOrInit(ac)
	if err != nil {
		lp.Log(fmt.Sprintf("unable to choose hazelcast members: clientset initialization failed: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return nil, err
	}

	namespace, err := chooser.namespaceDiscoverer.getOrDiscover(ac)
	if err != nil {
		lp.Log(fmt.Sprintf("unable to choose hazelcast members: namespace to operate in could not be determined: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return nil, err
	}

	var labelSelector string
	if s, err := labelSelectorFromConfig(ac); err != nil {
		lp.Log(fmt.Sprintf("unable to choose hazelcast members: could not determine label selector: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return nil, err
	} else {
		labelSelector = s
	}

	lp.Log(fmt.Sprintf("using label selector '%s' in namespace '%s' to choose hazelcast members", labelSelector, namespace), client.ChaosMonkeyEvent, log.DebugLevel)

	ctx := context.TODO()
	podList, err := chooser.podLister.list(clientset, ctx, namespace, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		lp.Log(fmt.Sprintf("unable to choose hazelcast members: could not list pods: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return nil, err
	}

	pods := podList.Items
	if len(pods) == 0 {
		lp.Log(fmt.Sprintf("no hazelcast members found for label selector '%s' in namespace '%s'", labelSelector, namespace), client.ChaosMonkeyEvent, log.WarnLevel)
		return nil, noMembersFoundError
	}

	lp.Log(fmt.Sprintf("found %d candidate pod/-s", len(pods)), client.ChaosMonkeyEvent, log.DebugLevel)

	if hzMembers, err := chooseTargetMembersFromPods(pods, sc, false); err == nil {
		lp.Log(fmt.Sprintf("successfully chose %d hazelcast member/-s to kill from given list of %d pod/-s", len(hzMembers), len(pods)), client.ChaosMonkeyEvent, log.InfoLevel)
		return hzMembers, nil
	} else {
		lp.Log(fmt.Sprintf("encountered error upon attempt to choose target members from given list of %d pod/-s: %s", len(pods), err.Error()), client.ChaosMonkeyEvent, log.WarnLevel)
		return nil, err
	}

}

func chooseTargetMembersFromPods(pods []v1.Pod, sc *memberSelectionConfig, listWasCheckedForReadyPods bool) ([]hzMember, error) {

	if pods == nil || len(pods) == 0 {
		lp.Log("cannot select target members from given list of pods because list was either nil or empty", client.ChaosMonkeyEvent, log.WarnLevel)
		return nil, noMembersProvidedToChooseFromError
	}

	lp.Log(fmt.Sprintf("selecting target members from given list of %d pod/-s", len(pods)), client.ChaosMonkeyEvent, log.DebugLevel)

	if sc.targetOnlyActive && !listWasCheckedForReadyPods {
		lp.Log("target-only-active setting was enabled, but list hasn't been checked for ready pods yet -- performing check", client.ChaosMonkeyEvent, log.DebugLevel)
		var onlyReadyPods []v1.Pod
		for _, p := range pods {
			if isPodReady(p) {
				onlyReadyPods = append(onlyReadyPods, p)
			}
		}
		if len(onlyReadyPods) == 0 {
			lp.Log(fmt.Sprintf("target-only-active setting was enabled, but ouf of %d given pod/-s, none were active (ready)", len(pods)), client.ChaosMonkeyEvent, log.WarnLevel)
			return nil, noReadyMembersFoundError
		}
		lp.Log(fmt.Sprintf("out of %d given pod/-s, %d are currently posting readiness -- entering next iteration of pod selection", len(pods), len(onlyReadyPods)), client.ChaosMonkeyEvent, log.DebugLevel)
		return chooseTargetMembersFromPods(onlyReadyPods, sc, true)
	}

	numPodsToSelect, err := evaluateNumPodsToSelect(pods, sc)
	lp.Log(fmt.Sprintf("evaluated number of pods to select from given list of %d pod/-s to be %d", len(pods), numPodsToSelect), client.ChaosMonkeyEvent, log.DebugLevel)

	if err != nil {
		return nil, err
	}

	selectedPods := make(map[string]v1.Pod)
	for i := uint8(0); i < numPodsToSelect; {
		candidate := selectRandomPodFromList(pods)
		if _, ok := selectedPods[candidate.Name]; !ok {
			selectedPods[candidate.Name] = candidate
			i++
		}
	}

	var hzMembers []hzMember
	for _, v := range selectedPods {
		hzMembers = append(hzMembers, hzMember{v.Name})
	}

	lp.Log(fmt.Sprintf("selected the following pods for termination: %v", hzMembers), client.ChaosMonkeyEvent, log.InfoLevel)

	return hzMembers, nil

}

func evaluateNumPodsToSelect(selectionPool []v1.Pod, sc *memberSelectionConfig) (uint8, error) {

	if len(selectionPool) == 0 {
		return 0, errors.New("cannot select pods from empty pod list")
	}

	if sc.selectionMode == absoluteMemberSelectionMode {
		if len(selectionPool) < int(sc.absoluteNumMembersToKill) {
			return 0, fmt.Errorf("was instructed to select %d pod/-s from pod list, but given list contained only %d pod/-s",
				sc.absoluteNumMembersToKill, len(selectionPool))
		}
		return sc.absoluteNumMembersToKill, nil
	}

	if sc.selectionMode == relativeMemberSelectionMode {
		return uint8(math.Ceil(float64(float32(len(selectionPool)) * sc.relativePercentageOfMembersToKill))), nil
	}

	return 0, errors.New("unknown member selection mode: " + sc.selectionMode)

}

func selectRandomPodFromList(pods []v1.Pod) v1.Pod {

	randomIndex := rand.Intn(len(pods))
	return pods[randomIndex]

}

func isPodReady(p v1.Pod) bool {

	podConditions := p.Status.Conditions

	for _, condition := range podConditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			lp.Log(fmt.Sprintf("found ready pod: %s", p.Name), client.ChaosMonkeyEvent, log.DebugLevel)
			return true
		}
		lp.Log(fmt.Sprintf("skipping condition type '%s' for pod '%s'", condition.Type, p.Name), client.ChaosMonkeyEvent, log.DebugLevel)
	}

	return false

}

func (killer *k8sHzMemberKiller) kill(members []hzMember, s sleeper, ac *memberAccessConfig, memberGrace *sleepConfig, cc *chaosProbabilityConfig, tc *memberTerminationConfig) (int, chan bool, error) {

	if members == nil || len(members) == 0 {
		return 0, nil, noMembersProvidedForKillingError
	}

	if cc.evaluationMode == perMemberActivityEvaluation && cc.percentage == 0.0 {
		lp.Log(fmt.Sprintf("member killer was given set of %d member/-s, but per-member activity evaluation "+
			"mode was enabled with a chaos percentage of zero, so cannot kill members", len(members)), client.ChaosMonkeyEvent, log.InfoLevel)
		return 0, nil, nil
	}

	clientset, err := killer.clientsetProvider.getOrInit(ac)
	if err != nil {
		lp.Log(fmt.Sprintf("unable to kill hazelcast members: clientset initialization failed: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return 0, nil, err
	}

	namespace, err := killer.namespaceDiscoverer.getOrDiscover(ac)
	if err != nil {
		lp.Log(fmt.Sprintf("unable to kill hazelcast members: namespace to operate in could not be determined: %s", err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		return 0, nil, err
	}

	memberKillEvents := make(chan bool, len(members))
	numMembersToKill := 0
	for _, m := range members {

		if cc.evaluationMode == perMemberActivityEvaluation {
			f := rand.Float64()
			if cc.percentage < f {
				lp.Log(fmt.Sprintf("not killing hazelcast member '%s' as per-member evaluation "+
					"mode was enabled and evaluated random number did not fall within range of chaos probability",
					m.identifier), client.ChaosMonkeyEvent, log.InfoLevel)
				continue
			}
		}

		numMembersToKill++

		lp.Log(fmt.Sprintf("invoking pod deletion for hazelcast member '%s'", m.identifier), client.ChaosMonkeyEvent, log.DebugLevel)

		gracePeriod := evaluatePodTerminationGracePeriod(memberGrace)
		go invokePodDeletion(killer.podDeleter, s, clientset, m, tc, namespace, gracePeriod, memberKillEvents)

	}

	return numMembersToKill, memberKillEvents, nil

}

func invokePodDeletion(
	d k8sPodDeleter,
	s sleeper,
	clientset *kubernetes.Clientset,
	m hzMember,
	tc *memberTerminationConfig,
	namespace string,
	gracePeriod int64,
	membersKilled chan bool,
) {

	if tc.mode == delayed {
		lp.Log(fmt.Sprintf("delaying termination of hazelcast member '%s'", m.identifier), client.ChaosMonkeyEvent, log.DebugLevel)
		s.sleep(&sleepConfig{
			enabled:          true,
			durationSeconds:  int(tc.delaySeconds),
			enableRandomness: tc.enableRandomness},
			sleepTimeFunc)
	} else {
		lp.Log(fmt.Sprintf("killing hazelcast member '%s' without delay", m.identifier), client.ChaosMonkeyEvent, log.DebugLevel)
	}

	lp.Log(fmt.Sprintf("using grace period seconds '%d' to kill hazelcast member '%s'", gracePeriod, m.identifier), client.ChaosMonkeyEvent, log.DebugLevel)

	ctx := context.TODO()
	err := d.delete(clientset, ctx, namespace, m.identifier, metav1.DeleteOptions{GracePeriodSeconds: &gracePeriod})

	if err != nil {
		lp.Log(fmt.Sprintf("killing hazelcast member '%s' unsuccessful: %s", m.identifier, err.Error()), client.ChaosMonkeyEvent, log.ErrorLevel)
		membersKilled <- false
	} else {
		lp.Log(fmt.Sprintf("successfully killed hazelcast member '%s' granting %d seconds of grace period", m.identifier, gracePeriod), client.ChaosMonkeyEvent, log.InfoLevel)
		membersKilled <- true
	}

}

func evaluatePodTerminationGracePeriod(memberGrace *sleepConfig) int64 {

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

	return int64(gracePeriod)

}
