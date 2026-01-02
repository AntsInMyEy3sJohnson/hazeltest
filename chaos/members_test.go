package chaos

import (
	"context"
	"errors"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"math"
	"strings"
	"testing"
)

var (
	clientsetInitError            = errors.New("lo and behold, the error everyone told you was never going to happen")
	configBuildError              = errors.New("another impossible error")
	podListError                  = errors.New("another one")
	podDeleteError                = errors.New("and yet another one")
	namespaceNotDiscoverableError = errors.New("and here goes your sanity")
)

var (
	testBuilder                         = &testK8sConfigBuilder{}
	testClientsetInitializer            = &testK8sClientsetInitializer{}
	emptyClientset                      = &kubernetes.Clientset{}
	chaosConfigHavingPerRunActivityMode = &chaosProbabilityConfig{1.0, perRunActivityEvaluation}
	defaultKubeconfig                   = "default"
	nonDefaultKubeconfig                = "/some/path/to/a/custom/kubeconfig"
	hazelcastNamespace                  = "hazelcastplatform"
	testAccessConfig                    = assembleTestMemberAccessConfig(k8sInCluster, "")
	testSelectionConfig                 = assembleTestMemberSelectionConfig(relativeMemberSelectionMode, true, 0.0, 0.0)
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
		returnError    bool
		numInvocations int
	}
	testK8sNamespaceDiscoverer struct {
		returnError    bool
		numInvocations int
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

func (p *testK8sClientsetProvider) getOrInit(_ *memberAccessConfig) (*kubernetes.Clientset, error) {

	p.numInvocations++

	if p.returnError {
		return nil, clientsetInitError
	}

	return &kubernetes.Clientset{}, nil

}

func (d *testK8sNamespaceDiscoverer) getOrDiscover(ac *memberAccessConfig) (string, error) {

	d.numInvocations++

	if d.returnError {
		return "", namespaceNotDiscoverableError
	}

	if ac.accessMode == k8sOutOfCluster {
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

func TestChooseTargetMembersFromPods(t *testing.T) {

	t.Log("given a list of pods, a member selection config, and information whether the list contains only ready pods")
	{
		t.Log("\twhen list of pods is nil")
		{
			hzMembers, err := chooseTargetMembersFromPods(nil, nil, false)

			msg := "\t\tnil list of hazelcast members must be returned"
			if hzMembers == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must be returned"
			if err != nil && errors.Is(err, noMembersProvidedToChooseFromError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
		t.Log("\twhen list of pods is empty")
		{
			hzMembers, err := chooseTargetMembersFromPods([]v1.Pod{}, nil, false)

			msg := "\t\tnil list of hazelcast members must be returned"
			if hzMembers == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must be returned"
			if err != nil && errors.Is(err, noMembersProvidedToChooseFromError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen only active members should be targeted")
		{
			t.Log("\t\twhen list contains only pods in non-ready state")
			{
				pods := assemblePodList(12, 0)

				hzMembers, err := chooseTargetMembersFromPods(pods,
					assembleTestMemberSelectionConfig(relativeMemberSelectionMode, true, 0, 0.0),
					false)

				msg := "\t\t\treturned list of hazelcast members must be nil"
				if hzMembers == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\terror must be returned"
				if err != nil && errors.Is(err, noReadyMembersFoundError) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen list also contains pods in ready state")
			{
				t.Log("\t\t\twhen absolute member selection mode is configured")
				{
					t.Log("\t\t\t\twhen selection config requires more pods to be selected than are ready")
					{
						numReadyPods := 9
						pods := assemblePodList(12, numReadyPods)

						numPodsToSelect := uint8(10)
						hzMembers, err := chooseTargetMembersFromPods(pods,
							assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, true, numPodsToSelect, 0.0),
							false)

						msg := "\t\t\t\t\treturned list of hazelcast members must be nil"
						if hzMembers == nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\terror must be returned"
						if err != nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}

					t.Log("\t\t\t\twhen selection config requires number of pods to be selected less than number of pods having ready state")
					{
						numReadyPods := 9
						pods := assemblePodList(12, numReadyPods)

						numPodsToSelect := uint8(6)
						hzMembers, err := chooseTargetMembersFromPods(pods,
							assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, true, numPodsToSelect, 0.0),
							false)

						msg := "\t\t\t\t\treturned list of hazelcast members must contain expected number of elements"
						if uint8(len(hzMembers)) == numPodsToSelect {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treturned list of hazelcast members must contain only unique identifiers"
						if assertOnlyUniqueIdentifiers(hzMembers) {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tno error must be returned"
						if err == nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}

				}

				t.Log("\t\t\twhen relative member selection mode is configured")
				{
					// While the absolute member selection mode leaves no room for interpretation concerning the number of
					// members to be selected, the relative member selection mode needs a set of pods to "relate" to, hence
					// from which to derive the number of pods to be selected. This could be two sets: Either the one
					// containing the entirety of pods, or the one containing only those having ready state.
					// --> Need to make sure that the base set for evaluating the number of pods to select is the
					// set containing only pods having ready state when only active members have been configured to be
					// subject to the selection.

					numPods := 9
					numReadyPods := 4
					pods := assemblePodList(numPods, numReadyPods)

					percentageOfMembersToSelect := 0.5
					hzMembers, err := chooseTargetMembersFromPods(pods,
						assembleTestMemberSelectionConfig(relativeMemberSelectionMode, true, 0.0, float32(percentageOfMembersToSelect)),
						false)

					msg := "\t\t\t\treturned list of hazelcast members must contain expected number of elements"
					if len(hzMembers) == int(float64(numReadyPods)*percentageOfMembersToSelect) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treturned list of hazelcast members must contain only unique identifiers"
					if assertOnlyUniqueIdentifiers(hzMembers) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}

			t.Log("\t\twhen list only contains pods in ready state and selection config requires all pods to be selected as members")
			{
				numPods := 21
				pods := assemblePodList(numPods, numPods)

				hzMembers, err := chooseTargetMembersFromPods(pods,
					assembleTestMemberSelectionConfig(relativeMemberSelectionMode, true, 0, 1.0),
					false)
				msg := "\t\t\tlist of returned hazelcast members must contain members corresponding to all pods"
				if len(hzMembers) == numPods {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\t\treturned list of hazelcast members must contain only unique identifiers"
				if assertOnlyUniqueIdentifiers(hzMembers) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

			}
		}

		t.Log("\twhen non-active members can be subject to selection, too")
		{
			t.Log("\t\twhen absolute member selection mode is configured")
			{
				numPods := 15
				numReadyPods := 3
				pods := assemblePodList(numPods, numReadyPods)

				numPodsToSelect := uint8(5)
				hzMembers, err := chooseTargetMembersFromPods(pods,
					assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, false, numPodsToSelect, 0.0), false)

				msg := "\t\t\tlist of returned hazelcast members must contain expected number of elements"
				if uint8(len(hzMembers)) == numPodsToSelect {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned list of hazelcast members must contain only unique identifiers"
				if assertOnlyUniqueIdentifiers(hzMembers) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen relative member selection mode is configured")
			{
				// This time, make sure the base set of pods to "relate" to in relative member selection mode
				// is the entirety of pods, rather than only those currently having ready state.

				numPods := 11
				numReadyPods := 3
				pods := assemblePodList(numPods, numReadyPods)

				hzMembers, err := chooseTargetMembersFromPods(pods,
					assembleTestMemberSelectionConfig(relativeMemberSelectionMode, false, 0.0, 1.0), false)

				msg := "\t\t\tlist of returned hazelcast members must contain expected number of elements"
				if len(hzMembers) == numPods {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned list of hazelcast members must contain only unique identifiers"
				if assertOnlyUniqueIdentifiers(hzMembers) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestEvaluateNumPodsToSelect(t *testing.T) {

	t.Log("given a selection pool of pods and a member selection config")
	{
		t.Log("\twhen selection pool is empty")
		{
			numPodsToSelect, err := evaluateNumPodsToSelect([]v1.Pod{}, nil)

			msg := "\t\tevaluated number of pods to select must be zero"
			if numPodsToSelect == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen absolute member selection mode is configured")
		{
			t.Log("\t\twhen number of pods selection pool is less than configured number of members to select")
			{
				podList := assemblePodList(1, 0)
				numPodsToSelect, err := evaluateNumPodsToSelect(podList,
					assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, false, 2, 0))

				msg := "\t\t\tevaluated number of pods to select must be zero"
				if numPodsToSelect == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen number of pods in selection pool is equal to configured number of members to select")
			{
				podList := assemblePodList(5, 0)

				configuredNumPodsToSelect := uint8(len(podList))
				numPodsToSelect, err := evaluateNumPodsToSelect(podList,
					assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, false, configuredNumPodsToSelect, 0))

				msg := "\t\t\tevaluated number of pods to select must correspond to configured number of pods to select"
				if numPodsToSelect == configuredNumPodsToSelect {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}

		t.Log("\twhen relative member selection mode is configured")
		{
			t.Log("\t\twhen configured percentage of members to kill is zero")
			{
				podList := assemblePodList(1, 0)
				numPodsToSelect, err := evaluateNumPodsToSelect(podList,
					assembleTestMemberSelectionConfig(relativeMemberSelectionMode, false, 0, 0.0))

				msg := "\t\t\tevaluated number of pods must be zero, too"
				if numPodsToSelect == 0 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen configured percentage of members to kill is 100")
			{
				podList := assemblePodList(9, 0)
				numPodsToSelect, err := evaluateNumPodsToSelect(podList,
					assembleTestMemberSelectionConfig(relativeMemberSelectionMode, false, 0, 1.0))

				msg := "\t\t\tevaluated number of pods to select must be equal to number of pods in selection pool"
				if numPodsToSelect == uint8(len(podList)) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen configured percentage of members to kill would result in non-integer number")

			podList := assemblePodList(9, 0)

			sc := assembleTestMemberSelectionConfig(relativeMemberSelectionMode, false, 0, 0.5)
			numPodsToSelect, err := evaluateNumPodsToSelect(podList, sc)

			msg := "\t\t\tnumber of pods to select must correspond to next-highest integer"
			if numPodsToSelect == uint8(math.Ceil(float64(float32(len(podList))*sc.relativePercentageOfMembersToKill))) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

		t.Log("\twhen unknown member selection mode is configured")
		{
			numPodsToSelected, err := evaluateNumPodsToSelect(assemblePodList(1, 0),
				assembleTestMemberSelectionConfig("awesomeNonExistingSelectionMode", false, 0, 0.0))

			msg := "\t\tnumber of pods to select must be zero"
			if numPodsToSelected == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

		}

	}

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
			namespace, err := discoverer.getOrDiscover(assembleTestMemberAccessConfig(k8sOutOfCluster, "default"))

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
			ac := assembleTestMemberAccessConfig(k8sOutOfCluster, "default")
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
			namespace, err := discoverer.getOrDiscover(assembleTestMemberAccessConfig("some-non-existing-access-mode", "default"))

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

				cs, err := provider.getOrInit(assembleTestMemberAccessConfig(k8sOutOfCluster, defaultKubeconfig))

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

				_, _ = provider.getOrInit(assembleTestMemberAccessConfig(k8sOutOfCluster, nonDefaultKubeconfig))

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

				cs, err := provider.getOrInit(assembleTestMemberAccessConfig(k8sOutOfCluster, defaultKubeconfig))

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

			unknownAccessMode := hzOnK8sMemberAccessMode("someUnknownMemberAccessMode")
			cs, err := provider.getOrInit(assembleTestMemberAccessConfig(unknownAccessMode, "default"))

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\terror must contain access mode in question"
			if strings.Contains(err.Error(), string(unknownAccessMode)) {
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

			cs, err := provider.getOrInit(assembleTestMemberAccessConfig("something", "default"))

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
			errCsProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, true, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{errCsProvider, nsDiscoverer, podLister}
			members, err := memberChooser.choose(assembleTestMemberAccessConfig(k8sOutOfCluster, defaultKubeconfig),
				assembleTestMemberSelectionConfig(relativeMemberSelectionMode, true, 0, 0.0))

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, clientsetInitError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned member list must be nil"
			if members == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have one invocation"
			if errCsProvider.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have no invocations"
			if nsDiscoverer.numInvocations == 0 {
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
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			errNsDiscoverer := &testK8sNamespaceDiscoverer{true, 0}
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csProvider, errNsDiscoverer, nil}
			members, err := memberChooser.choose(testAccessConfig, testSelectionConfig)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned list of members must be nil"
			if members == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have one invocation"
			if csProvider.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have one invocation"
			if errNsDiscoverer.numInvocations == 1 {
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
			ac := *testAccessConfig
			ac.accessMode = "awesomeUnknownMemberAccessMode"
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
			members, err := memberChooser.choose(&ac, testSelectionConfig)

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned list of members must be nil"
			if members == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have one invocation"
			if csProvider.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have one invocation"
			if nsDiscoverer.numInvocations == 1 {
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
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			podLister := &testK8sPodLister{[]v1.Pod{}, true, 0}
			memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
			members, err := memberChooser.choose(testAccessConfig, testSelectionConfig)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, podListError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned list of members must be nil"
			if members == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have one invocation"
			if csProvider.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have one invocation"
			if nsDiscoverer.numInvocations == 1 {
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
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			podLister := &testK8sPodLister{[]v1.Pod{}, false, 0}
			memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
			members, err := memberChooser.choose(assembleTestMemberAccessConfig(k8sOutOfCluster, defaultKubeconfig), testSelectionConfig)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, noMembersFoundError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tlist of returned members must be nil"
			if members == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have one invocation"
			if csProvider.numInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have one invocation"
			if nsDiscoverer.numInvocations == 1 {
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
		t.Log("\twhen target only active (ready) was enabled")
		{
			t.Log("\t\twhen absolute member selection mode was configured")
			{
				t.Log("\t\t\twhen active (ready) pod is present")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					pod := assemblePod("hazelcastplatform-0", true)
					pods := []v1.Pod{pod}
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
					sc := assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, true, 1, 0.0)
					members, err := memberChooser.choose(testAccessConfig, sc)

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treturned list of members must contain one element"
					if len(members) == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					member := members[0]
					msg = "\t\t\t\tname of selected member must be equal to name of ready pod"
					if member.identifier == pod.Name {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen only non-active (-ready) pods are present")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					pod := assemblePod("hazelcastplatform-0", false)
					pods := []v1.Pod{pod}
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
					members, err := memberChooser.choose(testAccessConfig, testSelectionConfig)

					msg := "\t\t\t\terror must be returned"
					if err != nil && errors.Is(err, noReadyMembersFoundError) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treturned list of members must be nil"
					if members == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen active (ready) pods are present, but desired number of pods is higher than number of available pods")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}

					numAvailablePods := 15
					pods := assemblePodList(numAvailablePods, 12)
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := &k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}

					members, err := memberChooser.choose(testAccessConfig, assembleTestMemberSelectionConfig(
						absoluteMemberSelectionMode, true, uint8(numAvailablePods), 0.0))

					msg := "\t\t\t\terror must be returned"
					if err != nil && strings.Contains(err.Error(), "was instructed to select") {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treturn list of members must be nil"
					if members == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

				}
				t.Log("\t\t\twhen active (ready) pods are present and desired number of pods is equal to number of available pods")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					numAvailablePods := 15
					pods := assemblePodList(numAvailablePods, numAvailablePods)
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := &k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}

					members, err := memberChooser.choose(testAccessConfig, assembleTestMemberSelectionConfig(
						absoluteMemberSelectionMode, true, uint8(numAvailablePods), 0.0))

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnumber of hazelcast members in returned list must be equal to number of available pods"
					if len(members) == numAvailablePods {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tentries in returned list of hazelcast members must be unique"
					if assertOnlyUniqueIdentifiers(members) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
			t.Log("\t\twhen relative member selection mode was configured")
			{
				t.Log("\t\t\twhen active (ready) pods are present and relative member selection mode was configured with 100 % of members to be killed")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					numAvailablePods := 15
					numReadyPods := 12
					pods := assemblePodList(numAvailablePods, numReadyPods)
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := &k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}

					members, err := memberChooser.choose(testAccessConfig, assembleTestMemberSelectionConfig(
						relativeMemberSelectionMode, true, 0.0, 1.0))

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnumber of hazelcast members in returned list must contain 100 % of pods having active (ready) state"
					if len(members) == numReadyPods {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tentries in returned list of hazelcast members must be unique"
					if assertOnlyUniqueIdentifiers(members) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}

		}
		t.Log("\twhen target only active (ready) is disabled")
		{
			t.Log("\t\twhen one pod is present")
			{
				csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
				nsDiscoverer := &testK8sNamespaceDiscoverer{}
				pod := assemblePod("hazelcastplatform-0", false)
				pods := []v1.Pod{pod}
				podLister := &testK8sPodLister{pods, false, 0}
				memberChooser := k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}
				ac := assembleTestMemberAccessConfig(k8sInCluster, defaultKubeconfig)
				sc := assembleTestMemberSelectionConfig(absoluteMemberSelectionMode, false, 1, 0.0)
				members, err := memberChooser.choose(ac, sc)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned list of members must contain one element"
				if len(members) == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				member := members[0]
				msg = "\t\t\tname of chosen member must correspond to name of given pod"
				if member.identifier == pod.Name {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tclient set provider must have one invocation"
				if csProvider.numInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tnamespace discoverer must have one invocation"
				if nsDiscoverer.numInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpod lister must have one invocation"
				if podLister.numInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
			t.Log("\t\twhen multiple pods are present, but only a subset of them are in active (ready) state")
			{
				t.Log("\t\t\twhen absolute member selection mode was configured and number of desired pods is equal to total number of available pods")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					numAvailablePods := 15
					pods := assemblePodList(numAvailablePods, 12)
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := &k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}

					members, err := memberChooser.choose(testAccessConfig, assembleTestMemberSelectionConfig(
						absoluteMemberSelectionMode, false, uint8(numAvailablePods), 0.0))

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnumber of hazelcast members in returned list must be equal to number of available pods"
					if len(members) == numAvailablePods {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tentries in returned list of hazelcast members must be unique"
					if assertOnlyUniqueIdentifiers(members) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen relative member selection mode was configured with 100 % of members to be killed")
				{
					csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					numAvailablePods := 15
					numReadyPods := 9
					pods := assemblePodList(numAvailablePods, numReadyPods)
					podLister := &testK8sPodLister{pods, false, 0}
					memberChooser := &k8sHzMemberChooser{csProvider, nsDiscoverer, podLister}

					members, err := memberChooser.choose(testAccessConfig, assembleTestMemberSelectionConfig(
						relativeMemberSelectionMode, false, 0.0, 1.0))

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnumber of hazelcast members in returned list must contain 100 % of pods having any state"
					if len(members) == numAvailablePods {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tentries in returned list of hazelcast members must be unique"
					if assertOnlyUniqueIdentifiers(members) {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have one invocation"
					if csProvider.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have one invocation"
					if nsDiscoverer.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod lister must have one invocation"
					if podLister.numInvocations == 1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
			}
		}
	}

}

func TestKillMemberOnK8s(t *testing.T) {

	t.Log("given the member killer monkey's method to kill a hazelcast member on kubernetes")
	{
		t.Log("\twhen given list of members is nil")
		{
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{csProvider, nsDiscoverer, deleter}

			numMembersKilled, err := killer.kill(nil, nil, nil, nil)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, noMembersProvidedForKillingError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of killed members must be zero"
			if numMembersKilled == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have no invocations"
			if csProvider.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have no invocations"
			if nsDiscoverer.numInvocations == 0 {
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
		t.Log("\twhen given list of members is empty")
		{
			csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
			nsDiscoverer := &testK8sNamespaceDiscoverer{}
			deleter := &testK8sPodDeleter{}
			killer := &k8sHzMemberKiller{csProvider, nsDiscoverer, deleter}

			numMembersKilled, err := killer.kill([]hzMember{}, nil, nil, nil)

			msg := "\t\terror must be returned"
			if err != nil && errors.Is(err, noMembersProvidedForKillingError) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treported number of killed members must be zero"
			if numMembersKilled == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tclient set provider must have no invocations"
			if csProvider.numInvocations == 0 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnamespace discoverer must have no invocations"
			if nsDiscoverer.numInvocations == 0 {
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
		t.Log("\twhen given list of members contains at least one member")
		{
			t.Log("\t\twhen per-member evaluation mode is enabled")
			{
				t.Log("\t\t\twhen chaos probability percentage is zero")
				{
					csProvider := &testK8sClientsetProvider{}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					deleter := &testK8sPodDeleter{}
					killer := &k8sHzMemberKiller{
						csProvider,
						nsDiscoverer,
						deleter,
					}
					numMembersKilled, err := killer.kill(
						assembleMemberList(42),
						assembleTestMemberAccessConfig(k8sInCluster, "default"),
						nil,
						assembleChaosProbabilityConfig(0.0, perMemberActivityEvaluation),
					)

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treported number of killed members must be zero"
					if numMembersKilled == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tclient set provider must have zero invocations"
					if csProvider.numInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tnamespace discoverer must have zero invocations"
					if nsDiscoverer.numInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod deleter must have zero invocations"
					if deleter.numInvocations == 0 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen chaos probability percentage is 50")
				{
					csProvider := &testK8sClientsetProvider{}
					nsDiscoverer := &testK8sNamespaceDiscoverer{}
					podDeleter := &testK8sPodDeleter{}
					killer := &k8sHzMemberKiller{
						clientsetProvider:   csProvider,
						namespaceDiscoverer: nsDiscoverer,
						podDeleter:          podDeleter,
					}

					numMembers := 500
					chaosPercentage := 0.5
					numMembersKilled, err := killer.kill(
						assembleMemberList(numMembers),
						assembleTestMemberAccessConfig(k8sInCluster, "default"),
						assembleMemberGraceSleepConfig(false, false, 0),
						assembleChaosProbabilityConfig(chaosPercentage, perMemberActivityEvaluation),
					)

					msg := "\t\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\treported number of killed members must be (roughly) half the number of members"
					if math.Abs(float64(numMembersKilled)-float64(numMembers)*chaosPercentage) < float64(numMembers)*0.1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}

					msg = "\t\t\t\tpod deleter's invocations must be equal to (roughly) half the number of members"
					if math.Abs(float64(podDeleter.numInvocations)-(float64(numMembers)*chaosPercentage)) < float64(numMembers)*0.1 {
						t.Log(msg, checkMark)
					} else {
						t.Fatal(msg, ballotX)
					}
				}
				t.Log("\t\t\twhen chaos probability percentage is 100")
				{
					t.Log("\t\t\t\twhen clientset initialization yields an error")
					{
						errCsProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, true, 0}
						nsDiscoverer := &testK8sNamespaceDiscoverer{}
						deleter := &testK8sPodDeleter{}
						killer := &k8sHzMemberKiller{
							clientsetProvider:   errCsProvider,
							namespaceDiscoverer: nsDiscoverer,
							podDeleter:          deleter,
						}

						numMembersKilled, err := killer.kill(
							assembleMemberList(3),
							assembleTestMemberAccessConfig(k8sInCluster, "default"),
							assembleMemberGraceSleepConfig(true, true, 42),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\terror must be returned"
						if err != nil && errors.Is(err, clientsetInitError) {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be zero"
						if numMembersKilled == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tclient set provider must have one invocation"
						if errCsProvider.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tnamespace discoverer must have no invocations"
						if nsDiscoverer.numInvocations == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeleter must have no invocations"
						if deleter.numInvocations == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
					t.Log("\t\t\t\twhen namespace discovery is not successful")
					{
						csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
						errNsDiscoverer := &testK8sNamespaceDiscoverer{true, 0}
						podDeleter := &testK8sPodDeleter{false, 0, 42}
						killer := k8sHzMemberKiller{csProvider, errNsDiscoverer, podDeleter}

						numMembersKilled, err := killer.kill(
							assembleMemberList(3),
							testAccessConfig,
							assembleMemberGraceSleepConfig(false, false, 0),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\terror must be returned"
						if err != nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be zero"
						if numMembersKilled == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tclient set provider must have one invocation"
						if csProvider.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tnamespace discoverer must have one invocation"
						if errNsDiscoverer.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tpod deleter must have no invocations"
						if podDeleter.numInvocations == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
					t.Log("\t\t\t\twhen member grace is enabled with randomness")
					{
						csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
						nsDiscoverer := &testK8sNamespaceDiscoverer{}
						deleter := &testK8sPodDeleter{}
						killer := &k8sHzMemberKiller{
							clientsetProvider:   csProvider,
							namespaceDiscoverer: nsDiscoverer,
							podDeleter:          deleter,
						}

						memberGraceSeconds := math.MaxInt - 1
						numMembersKilled, err := killer.kill(
							assembleMemberList(1),
							assembleTestMemberAccessConfig(k8sInCluster, "default"),
							assembleMemberGraceSleepConfig(true, true, memberGraceSeconds),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\tno error must be returned"
						if err == nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be one"
						if numMembersKilled == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tclient set provider must have one invocation"
						if csProvider.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tnamespace discoverer must have one invocation"
						if nsDiscoverer.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeleter must have one invocation"
						if deleter.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeletion must be invoked with non-zero random member grace seconds"
						if deleter.gracePeriodSeconds > 0 && deleter.gracePeriodSeconds != int64(memberGraceSeconds) {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
					t.Log("\t\t\t\twhen member grace is enabled without randomness")
					{
						csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
						nsDiscoverer := &testK8sNamespaceDiscoverer{}
						deleter := &testK8sPodDeleter{}
						killer := &k8sHzMemberKiller{
							clientsetProvider:   csProvider,
							namespaceDiscoverer: nsDiscoverer,
							podDeleter:          deleter,
						}

						memberGraceSeconds := 42
						numMembersKilled, err := killer.kill(
							[]hzMember{
								{"hazelcastplatform-0"},
							},
							assembleTestMemberAccessConfig(k8sInCluster, "default"),
							assembleMemberGraceSleepConfig(true, false, memberGraceSeconds),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\tno error must be returned"
						if err == nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be one"
						if numMembersKilled == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeletion must be invoked with number equal to pre-configured number"
						if deleter.gracePeriodSeconds == int64(memberGraceSeconds) {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
					t.Log("\t\t\t\twhen member grace is disabled")
					{
						csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
						nsDiscoverer := &testK8sNamespaceDiscoverer{}
						deleter := &testK8sPodDeleter{}
						killer := &k8sHzMemberKiller{
							clientsetProvider:   csProvider,
							namespaceDiscoverer: nsDiscoverer,
							podDeleter:          deleter,
						}

						numMembersKilled, err := killer.kill(
							[]hzMember{
								{"hazelcastplatform-0"},
							},
							assembleTestMemberAccessConfig(k8sInCluster, "default"),
							assembleMemberGraceSleepConfig(false, false, 42),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\tno error must be returned"
						if err == nil {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be one"
						if numMembersKilled == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tclient set provider must have one invocation"
						if csProvider.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tnamespace discoverer must have one invocation"
						if nsDiscoverer.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeleter must have one invocation"
						if deleter.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tdeletion must be invoked with member grace zero"
						if deleter.gracePeriodSeconds == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
					t.Log("\t\t\t\twhen pod deletion yields an error")
					{
						csProvider := &testK8sClientsetProvider{testBuilder, testClientsetInitializer, false, 0}
						nsDiscoverer := &testK8sNamespaceDiscoverer{}
						errDeleter := &testK8sPodDeleter{returnError: true}
						killer := &k8sHzMemberKiller{
							clientsetProvider:   csProvider,
							namespaceDiscoverer: nsDiscoverer,
							podDeleter:          errDeleter,
						}

						numMembers := 42
						numMembersKilled, err := killer.kill(
							assembleMemberList(numMembers),
							assembleTestMemberAccessConfig(k8sInCluster, "default"),
							assembleMemberGraceSleepConfig(false, false, 42),
							chaosConfigHavingPerRunActivityMode,
						)

						msg := "\t\t\t\t\terror must be returned"
						if err != nil && errors.Is(err, podDeleteError) {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\treported number of killed members must be zero"
						if numMembersKilled == 0 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tclient set provider must have one invocation"
						if csProvider.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}

						msg = "\t\t\t\t\tnamespace discoverer must have one invocation"
						if nsDiscoverer.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
						// Loop aborts as soon as it encounters an error
						msg = "\t\t\t\t\tdeleter must have one invocation"
						if errDeleter.numInvocations == 1 {
							t.Log(msg, checkMark)
						} else {
							t.Fatal(msg, ballotX)
						}
					}
				}
			}
		}
	}

}

func assertOnlyUniqueIdentifiers(ms []hzMember) bool {

	identifiers := make(map[string]struct{})
	for _, member := range ms {
		if _, ok := identifiers[member.identifier]; ok {
			return false
		}
		identifiers[member.identifier] = struct{}{}
	}

	return true

}

func assemblePodList(numPods, numReady int) []v1.Pod {

	podList := make([]v1.Pod, numPods)

	for i := 0; i < numPods; i++ {
		var ready bool
		if i < numReady {
			ready = true
		} else {
			ready = false
		}
		podList[i] = assemblePod(fmt.Sprintf("awesome-hazelcast-pod-%d", i), ready)
	}

	return podList

}

func assembleMemberList(numMembers int) []hzMember {

	members := make([]hzMember, numMembers)

	for i := 0; i < numMembers; i++ {
		members[i] = hzMember{identifier: fmt.Sprintf("hazelcastimdg-%d", i)}
	}

	return members

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

func assembleMemberGraceSleepConfig(enabled, enableRandomness bool, durationSeconds int) *sleepConfig {

	return &sleepConfig{
		enabled:          enabled,
		durationSeconds:  durationSeconds,
		enableRandomness: enableRandomness,
	}

}

func assembleChaosProbabilityConfig(percentage float64, evaluationMode activityEvaluationMode) *chaosProbabilityConfig {

	return &chaosProbabilityConfig{
		percentage:     percentage,
		evaluationMode: evaluationMode,
	}

}

func assembleTestMemberSelectionConfig(selectionMode string, targetOnlyActive bool, absoluteNumMembersToKill uint8, relativePercentageOfMembersToKill float32) *memberSelectionConfig {

	return &memberSelectionConfig{
		selectionMode:                     selectionMode,
		targetOnlyActive:                  targetOnlyActive,
		absoluteNumMembersToKill:          absoluteNumMembersToKill,
		relativePercentageOfMembersToKill: relativePercentageOfMembersToKill,
	}

}

func assembleTestMemberAccessConfig(memberAccessMode hzOnK8sMemberAccessMode, kubeconfig string) *memberAccessConfig {

	return &memberAccessConfig{
		accessMode: memberAccessMode,
		k8sOutOfCluster: k8sOutOfClusterMemberAccess{
			kubeconfig:    kubeconfig,
			namespace:     "hazelcastplatform",
			labelSelector: "app.kubernetes.io/name=hazelcastplatform",
		},
		k8sInCluster: k8sInClusterMemberAccess{},
	}

}
