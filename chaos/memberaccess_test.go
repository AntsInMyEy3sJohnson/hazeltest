package chaos

import (
	"context"
	"errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"math"
	"strings"
	"testing"
)

type (
	testK8sConfigBuilder struct {
		returnError                 bool
		numBuildsOutOfClusterAccess int
		numBuildsInClusterAccess    int
		masterUrl                   string
		kubeconfig                  string
	}
	testK8sClientsetInitializer struct {
		returnError bool
		numInits    int
	}
	testK8sClientsetProvider struct {
		k8sConfigBuilder
		k8sClientsetInitializer
		returnError bool
	}
	testK8sNamespaceDiscoverer struct {
		returnError bool
	}
	testK8sPodLister struct {
		podsToReturn   []v1.Pod
		returnError    bool
		numInvocations int
	}
	testK8sPodDeleter struct {
		returnError        bool
		numInvocations     int
		gracePeriodSeconds int64
	}
)

var (
	clientsetInitError            = errors.New("lo and behold, the error everyone told you was never going to happen")
	configBuildError              = errors.New("another impossible error")
	podListError                  = errors.New("another one")
	podDeleteError                = errors.New("and yet another one")
	namespaceNotDiscoverableError = errors.New("and here goes your sanity")
)

var (
	testBuilder                = &testK8sConfigBuilder{}
	testClientsetInitializer   = &testK8sClientsetInitializer{}
	testNamespaceDiscoverer    = &testK8sNamespaceDiscoverer{}
	errTestNamespaceDiscoverer = &testK8sNamespaceDiscoverer{true}
	csProvider                 = &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false}
	errCsProvider              = &testK8sClientsetProvider{testBuilder, testClientsetInitializer, true}
	emptyMember                = hzMember{}
	emptyClientset             = &kubernetes.Clientset{}
	defaultKubeconfig          = "default"
	nonDefaultKubeconfig       = "/some/path/to/a/custom/kubeconfig"
	hazelcastNamespace         = "hazelcastplatform"
	testAccessConfig           = assembleTestAccessConfig(k8sInClusterAccessMode, "", true)
)

func (b *testK8sConfigBuilder) buildForOutOfClusterAccess(masterUrl, kubeconfig string) (*rest.Config, error) {

	b.numBuildsOutOfClusterAccess++

	b.masterUrl = masterUrl
	b.kubeconfig = kubeconfig

	if b.returnError {
		return nil, configBuildError
	}

	return &rest.Config{}, nil

}

func (b *testK8sConfigBuilder) buildForInClusterAccess() (*rest.Config, error) {

	b.numBuildsInClusterAccess++

	if b.returnError {
		return nil, configBuildError
	}

	return &rest.Config{}, nil

}

func (i *testK8sClientsetInitializer) init(_ *rest.Config) (*kubernetes.Clientset, error) {

	i.numInits++

	if i.returnError {
		return nil, clientsetInitError
	}

	return emptyClientset, nil

}

func (p *testK8sClientsetProvider) getOrInit(_ memberAccessConfig) (*kubernetes.Clientset, error) {

	if p.returnError {
		return nil, clientsetInitError
	}

	return &kubernetes.Clientset{}, nil

}

func (d *testK8sNamespaceDiscoverer) getOrDiscover(ac memberAccessConfig) (string, error) {

	if d.returnError {
		return "", namespaceNotDiscoverableError
	}

	if ac.accessMode == k8sOutOfClusterAccessMode {
		return ac.k8sOutOfCluster.namespace, nil
	}

	return hazelcastNamespace, nil

}

func (l *testK8sPodLister) list(_ *kubernetes.Clientset, _ context.Context, _ string, _ metav1.ListOptions) (*v1.PodList, error) {

	l.numInvocations++

	if l.returnError {
		return nil, podListError
	}

	return &v1.PodList{Items: l.podsToReturn}, nil

}

func (d *testK8sPodDeleter) delete(_ *kubernetes.Clientset, _ context.Context, _, _ string, deleteOptions metav1.DeleteOptions) error {

	d.numInvocations++
	d.gracePeriodSeconds = *deleteOptions.GracePeriodSeconds

	if d.returnError {
		return podDeleteError
	}

	return nil

}

func TestSelectRandomPodFromList(t *testing.T) {

	t.Log("given random pod selection")
	{
		t.Log("\twhen multiple pods are provided")
		{
			podSelected := map[string]bool{
				"hazelcastplatform-0": false,
				"hazelcastplatform-1": false,
				"hazelcastplatform-2": false,
			}
			var pods []v1.Pod
			for k := range podSelected {
				pods = append(pods, assemblePod(k, false))
			}
			msg := "\t\tpod from list must be returned"
			for i := 0; i < 10; i++ {
				selected := selectRandomPodFromList(pods)
				if _, ok := podSelected[selected.Name]; ok {
					podSelected[selected.Name] = true
				} else {
					t.Fatal(msg, ballotX, selected.Name)
				}
			}
			msg = "\t\teach pod must have been selected at least once"
			for k, v := range podSelected {
				if v {
					t.Log(msg, checkMark, k)
				} else {
					t.Fatal(msg, ballotX, k)
				}
			}
		}
	}
}

func TestDefaultNamespaceDiscovererGetOrDiscover(t *testing.T) {

	t.Log("given the default namespace discoverer")
	{
		t.Log("\twhen namespace discovery is successful")
		{
			discoverer := &defaultK8sNamespaceDiscoverer{}
			namespace, err := discoverer.getOrDiscover(assembleTestAccessConfig(k8sOutOfClusterAccessMode, "default", false))

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\texpected value for namespace must be returned"
			if namespace == hazelcastNamespace {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace state must be set in discoverer"
			if discoverer.discoveredNamespace == hazelcastNamespace {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\twhen queried again, discoverer must return previously discovered namespace"
			ac := assembleTestAccessConfig(k8sOutOfClusterAccessMode, "default", false)
			ac.k8sOutOfCluster.namespace = "another-namespace"

			namespace, err = discoverer.getOrDiscover(ac)

			if namespace == hazelcastNamespace {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen namespace discovery is not successful")
		{
			discoverer := &defaultK8sNamespaceDiscoverer{}
			namespace, err := discoverer.getOrDiscover(assembleTestAccessConfig("some-non-existing-access-mode", "default", false))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace must be empty"
			if namespace == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdiscoverer must have empty namespace state"
			if discoverer.discoveredNamespace == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestDefaultClientsetProviderGetOrInit(t *testing.T) {

	t.Log("given the need to initialize or get the kubernetes client set")
	{
		t.Log("\twhen k8s out-of-cluster access mode is given")
		{
			t.Log("\t\twhen config builder does not yield an error and default kubeconfig is given")
			{
				builder := &testK8sConfigBuilder{}
				initializer := &testK8sClientsetInitializer{}
				provider := &defaultK8sClientsetProvider{
					configBuilder:        builder,
					clientsetInitializer: initializer,
				}

				cs, err := provider.getOrInit(assembleTestAccessConfig(k8sOutOfClusterAccessMode, defaultKubeconfig, true))

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset must be returned"
				if cs != nil && cs == emptyClientset {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset state must be set"
				if provider.cs != nil && provider.cs == cs {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tmaster url and kubeconfig must have been passed correctly"
				if builder.masterUrl == "" && strings.Contains(builder.kubeconfig, ".kube/config") {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig build for out-of-cluster access must have one invocation"
				if builder.numBuildsOutOfClusterAccess == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig build for in-cluster access must have no invocations"
				if builder.numBuildsInClusterAccess == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tinitializer must have one invocation"
				if initializer.numInits == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen config builder does not yield an error and non-default kubeconfig is given")
			{
				builder := &testK8sConfigBuilder{}
				initializer := &testK8sClientsetInitializer{}
				provider := &defaultK8sClientsetProvider{
					configBuilder:        builder,
					clientsetInitializer: initializer,
				}

				_, _ = provider.getOrInit(assembleTestAccessConfig(k8sOutOfClusterAccessMode, nonDefaultKubeconfig, true))

				msg := "\t\t\tmaster url and kubeconfig must have been passed correctly"
				if builder.masterUrl == "" && builder.kubeconfig == nonDefaultKubeconfig {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen config builder yields an error")
			{
				builder := &testK8sConfigBuilder{returnError: true}
				initializer := &testK8sClientsetInitializer{}
				provider := &defaultK8sClientsetProvider{
					configBuilder:        builder,
					clientsetInitializer: initializer,
				}

				cs, err := provider.getOrInit(assembleTestAccessConfig(k8sOutOfClusterAccessMode, defaultKubeconfig, true))

				msg := "\t\t\terror must be returned"
				if err != nil && errors.Is(err, configBuildError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset must be nil"
				if cs == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset state must be nil"
				if provider.cs == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
		t.Log("\twhen k8s in-cluster mode is given")
		{
			t.Log("\t\twhen config builder does not yield an error")
			{
				builder := &testK8sConfigBuilder{returnError: false}
				initializer := &testK8sClientsetInitializer{}
				provider := &defaultK8sClientsetProvider{configBuilder: builder, clientsetInitializer: initializer}

				cs, err := provider.getOrInit(testAccessConfig)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset must be returned"
				if cs != nil && cs == emptyClientset {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset state must be set"
				if provider.cs != nil && provider.cs == cs {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig build for in-cluster access must have one invocation"
				if builder.numBuildsInClusterAccess == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tconfig build for out-of-cluster access must have no invocations"
				if builder.numBuildsOutOfClusterAccess == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen config builder yields an error")
			{
				builder := &testK8sConfigBuilder{returnError: true}
				initializer := &testK8sClientsetInitializer{}
				provider := &defaultK8sClientsetProvider{configBuilder: builder, clientsetInitializer: initializer}

				cs, err := provider.getOrInit(testAccessConfig)

				msg := "\t\t\terror must be returned"
				if err != nil && errors.Is(err, configBuildError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset must be nil"
				if cs == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclientset state must be nil"
				if provider.cs == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
		t.Log("\twhen unknown k8s access mode is given")
		{
			provider := &defaultK8sClientsetProvider{}

			unknownAccessMode := "someUnknownMemberAccessMode"
			cs, err := provider.getOrInit(assembleTestAccessConfig(unknownAccessMode, "default", true))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must contain access mode in question"
			if strings.Contains(err.Error(), unknownAccessMode) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclientset must be nil"
			if cs == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclientset state must be nil"
			if provider.cs == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen clientset has already been set")
		{
			builder := &testK8sConfigBuilder{}
			initializer := &testK8sClientsetInitializer{}
			provider := &defaultK8sClientsetProvider{configBuilder: builder, clientsetInitializer: initializer}
			provider.cs = emptyClientset

			cs, err := provider.getOrInit(assembleTestAccessConfig("something", "default", true))

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclientset state must be returned"
			if cs != nil && cs == emptyClientset {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tconfig builder must have no invocations"
			if builder.numBuildsOutOfClusterAccess == 0 && builder.numBuildsInClusterAccess == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tinitializer must have no invocation"
			if initializer.numInits == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}
		t.Log("\twhen clientset initialization yields an error")
		{
			initializer := &testK8sClientsetInitializer{returnError: true}
			provider := &defaultK8sClientsetProvider{configBuilder: &testK8sConfigBuilder{}, clientsetInitializer: initializer}

			cs, err := provider.getOrInit(testAccessConfig)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, clientsetInitError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclientset must be nil"
			if cs == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclientset state must be nil"
			if provider.cs == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestChooseMemberOnK8s(t *testing.T) {

	t.Log("given the member chooser's method to choose a target hazelcast member")
	{
		t.Log("\twhen clientset cannot be initialized")
		{
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{errCsProvider, testNamespaceDiscoverer, podLister}
			member, err := memberChooser.choose(assembleTestAccessConfig(k8sOutOfClusterAccessMode, defaultKubeconfig, true))

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
		t.Log("\twhen namespace discovery is not successful")
		{
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csProvider, errTestNamespaceDiscoverer, nil}
			member, err := memberChooser.choose(testAccessConfig)

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
		t.Log("\twhen label selector cannot be determined")
		{
			ac := testAccessConfig
			ac.accessMode = "awesomeUnknownMemberAccessMode"
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer, podLister}
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
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer, podLister}
			member, err := memberChooser.choose(testAccessConfig)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, podListError) {
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
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer,
				&testK8sPodLister{[]v1.Pod{}, false, 0}}
			member, err := memberChooser.choose(assembleTestAccessConfig(k8sOutOfClusterAccessMode, defaultKubeconfig, true))

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, noMemberFoundError) {
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
			pod := assemblePod("hazelcastplatform-0", true)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(testAccessConfig)

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
			pod := assemblePod("hazelcastplatform-0", false)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(testAccessConfig)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, noMemberFoundError) {
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
			pod := assemblePod("hazelcastplatform-0", false)
			pods := []v1.Pod{pod}
			memberChooser := k8sHzMemberChooser{csProvider, testNamespaceDiscoverer,
				&testK8sPodLister{pods, false, 0}}
			member, err := memberChooser.choose(assembleTestAccessConfig(k8sInClusterAccessMode, defaultKubeconfig, false))

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

func TestKillMemberOnK8s(t *testing.T) {

	t.Log("given the member killer monkey's method to kill a hazelcast member on kubernetes")
	{
		t.Log("\twhen clientset initialization yields an error")
		{
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{
				clientsetProvider:   errCsProvider,
				namespaceDiscoverer: testNamespaceDiscoverer,
				podDeleter:          deleter,
			}

			err := killer.kill(
				hzMember{"hazelcastplatform-0"},
				assembleTestAccessConfig(k8sInClusterAccessMode, "default", true),
				assembleMemberGraceSleepConfig(true, true, 42),
			)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, clientsetInitError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdeleter must have no invocations"
			if deleter.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen namespace discovery is not successful")
		{
			podDeleter := &testK8sPodDeleter{false, 0, 42}
			killer := k8sHzMemberKiller{csProvider, errTestNamespaceDiscoverer, podDeleter}

			err := killer.kill(hzMember{}, testAccessConfig, assembleMemberGraceSleepConfig(false, false, 0))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpod deleter must have no invocations"
			if podDeleter.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen member grace is enabled with randomness")
		{
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{
				clientsetProvider:   csProvider,
				namespaceDiscoverer: testNamespaceDiscoverer,
				podDeleter:          deleter,
			}

			memberGraceSeconds := math.MaxInt - 1
			err := killer.kill(
				hzMember{"hazelcastplatform-0"},
				assembleTestAccessConfig(k8sInClusterAccessMode, "default", true),
				assembleMemberGraceSleepConfig(true, true, memberGraceSeconds),
			)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdeleter must have one invocation"
			if deleter.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdeletion must be invoked with non-zero random member grace seconds"
			if deleter.gracePeriodSeconds > 0 && deleter.gracePeriodSeconds != int64(memberGraceSeconds) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen member grace is enabled without randomness")
		{
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{
				clientsetProvider:   csProvider,
				namespaceDiscoverer: testNamespaceDiscoverer,
				podDeleter:          deleter,
			}

			memberGraceSeconds := 42
			err := killer.kill(
				hzMember{"hazelcastplatform-0"},
				assembleTestAccessConfig(k8sInClusterAccessMode, "default", true),
				assembleMemberGraceSleepConfig(true, false, memberGraceSeconds),
			)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdeletion must be invoked with number equal to pre-configured number"
			if deleter.gracePeriodSeconds == int64(memberGraceSeconds) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen member grace is disabled")
		{
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{
				clientsetProvider:   csProvider,
				namespaceDiscoverer: testNamespaceDiscoverer,
				podDeleter:          deleter,
			}

			err := killer.kill(
				hzMember{"hazelcastplatform-0"},
				assembleTestAccessConfig(k8sInClusterAccessMode, "default", true),
				assembleMemberGraceSleepConfig(false, false, 42),
			)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tdeletion must be invoked with member grace zero"
			if deleter.gracePeriodSeconds == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen pod deletion yields an error")
		{
			deleter := &testK8sPodDeleter{returnError: true}
			killer := &k8sHzMemberKiller{
				clientsetProvider:   csProvider,
				namespaceDiscoverer: testNamespaceDiscoverer,
				podDeleter:          deleter,
			}

			err := killer.kill(
				hzMember{"hazelcastplatform-0"},
				assembleTestAccessConfig(k8sInClusterAccessMode, "default", true),
				assembleMemberGraceSleepConfig(false, false, 42),
			)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, podDeleteError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

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

func assembleMemberGraceSleepConfig(enabled, enableRandomness bool, durationSeconds int) sleepConfig {

	return sleepConfig{
		enabled:          enabled,
		durationSeconds:  durationSeconds,
		enableRandomness: enableRandomness,
	}

}

func assembleTestAccessConfig(memberAccessMode, kubeconfig string, targetOnlyActive bool) memberAccessConfig {

	return memberAccessConfig{
		accessMode:       memberAccessMode,
		targetOnlyActive: targetOnlyActive,
		k8sOutOfCluster: k8sOutOfClusterMemberAccess{
			kubeconfig:    kubeconfig,
			namespace:     "hazelcastplatform",
			labelSelector: "app.kubernetes.io/name=hazelcastplatform",
		},
		k8sInCluster: k8sInClusterMemberAccess{},
	}

}
