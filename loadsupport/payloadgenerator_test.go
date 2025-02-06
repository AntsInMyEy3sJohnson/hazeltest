package loadsupport

import (
	"fmt"
	"sync"
	"testing"
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

func TestDefaultPayloadProvider_RegisterPayloadGenerationRequirement(t *testing.T) {

	t.Log("given an actor's base and a payload generation requirement")
	{
		t.Log("\twhen actor invokes registration")
		{
			actorBaseName := "mapLoadRunner"
			r := PayloadGenerationRequirement{}

			dp := DefaultPayloadProvider{}
			dp.RegisterPayloadGenerationRequirement(actorBaseName, r)

			registeredRequirement, ok := dp.actorRequirements.Load(actorBaseName)
			msg := "\t\tactor must have been registered"
			if ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpayload generation requirement must have been inserted"
			if registeredRequirement == r {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestDefaultPayloadProvider_RetrievePayload(t *testing.T) {

	t.Log("given an actor name")
	{
		t.Log("\twhen no payload generation requirement corresponding to actor name is present")
		{
			dp := DefaultPayloadProvider{}

			// Invoke retrieve method without previous registration of payload generation requirement for this
			// actor's base name
			p, err := dp.RetrievePayload("super-awesome-actor-name")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned payload must be nil pointer"
			if p == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen payload generation requirement corresponding to actor name was previously registered")
		{
			t.Log("\t\twhen registered payload generation requirement enables both fixed-size and variable-size payloads")
			{
				dp := DefaultPayloadProvider{}

				actorBaseName := "mapsLoadRunner"
				dp.RegisterPayloadGenerationRequirement(actorBaseName, PayloadGenerationRequirement{
					UseFixedSize:    true,
					UseVariableSize: true,
				})

				actorExtendedName := fmt.Sprintf("%s-ht_load-0", actorBaseName)
				p, err := dp.RetrievePayload(actorExtendedName)

				msg := "\t\t\terror must be returned"
				if err != nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned payload must be nil pointer"
				if p == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen registered payload generation requirement enables only variable-size payload")
			{
				dp := DefaultPayloadProvider{}

				actorBaseName := "mapLoadRunner"
				r := PayloadGenerationRequirement{
					UseVariableSize: true,
					VariableSize: VariableSizePayloadDefinition{
						LowerBoundaryBytes: 0,
						UpperBoundaryBytes: 21,
						SameSizeStepsLimit: 3,
					},
				}
				dp.RegisterPayloadGenerationRequirement(actorBaseName, r)

				p, err := dp.RetrievePayload(fmt.Sprintf("%s-ht_load-42", actorBaseName))

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned payload must correspond to boundaries specified by means of previously registered payload generation requirement"
				if len(*p) >= r.VariableSize.LowerBoundaryBytes && len(*p) <= r.VariableSize.UpperBoundaryBytes {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen registered payload generation requirement enables only fixed-size payload")
			{
				dp := DefaultPayloadProvider{}

				actorBaseName := "mapsPokedexRunner"
				r := PayloadGenerationRequirement{
					UseFixedSize: true,
					FixedSize: FixedSizePayloadDefinition{
						SizeBytes: 42,
					},
				}

				dp.RegisterPayloadGenerationRequirement(actorBaseName, r)

				p, err := dp.RetrievePayload(fmt.Sprintf("%s-ht_load-42", actorBaseName))

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\treturned payload must correspond to fixed size specified by means of previously registered payload generation requirement"
				if len(*p) == r.FixedSize.SizeBytes {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}
		}
	}

}

func TestDefaultPayloadProvider_findMatchingPayloadGenerationRequirement(t *testing.T) {

	t.Log("given an actor name")
	{
		t.Log("\twhen no actor with corresponding base name has previously registered")
		{
			dp := DefaultPayloadProvider{}
			// Register a couple of dummy actors
			for _, a := range []string{"aragorn", "gimli", "legolas"} {
				dp.RegisterPayloadGenerationRequirement(a, PayloadGenerationRequirement{
					UseVariableSize: true,
					VariableSize: VariableSizePayloadDefinition{
						LowerBoundaryBytes: len(a),
					},
				})
			}

			r, err := dp.findMatchingPayloadGenerationRequirement("super-awesome-actor-name")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned requirements value must represent empty requirement"
			emptyRequirement := PayloadGenerationRequirement{}
			if r == emptyRequirement {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen actor with corresponding base name has previously registered")
		{
			dp := DefaultPayloadProvider{}

			registeredRequirement := PayloadGenerationRequirement{
				UseVariableSize: true,
				VariableSize: VariableSizePayloadDefinition{
					LowerBoundaryBytes: 500,
					UpperBoundaryBytes: 2000,
					SameSizeStepsLimit: 250,
				},
			}

			dp.RegisterPayloadGenerationRequirement("mapLoadRunner", registeredRequirement)
			r, err := dp.findMatchingPayloadGenerationRequirement("mapLoadRunner-ht_load-0")

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\trequirement corresponding to actor name must be returned"
			if r == registeredRequirement {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, r)
			}

		}
	}

}

func TestInitializeAndReturnFixedSizePayload(t *testing.T) {

	t.Log("given an actor name and a payload generation requirement")
	{
		t.Log("\twhen payload with given size wasn't previously initialized")
		{
			r := PayloadGenerationRequirement{
				FixedSize: FixedSizePayloadDefinition{
					SizeBytes: 42,
				},
			}
			p, err := initializeAndReturnFixedSizePayload("awesome-actor-name", r)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned payload must match expected size"
			if len(*p) == r.FixedSize.SizeBytes {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tfixed-size payload must have been associated with size in store"
			v, ok := fixedSizePayloads.Load(r.FixedSize.SizeBytes)
			stored := v.(*string)

			if ok && stored == p {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX, fmt.Sprintf("%v != %v", p, stored))
			}
		}

		t.Log("\twhen payload with given size was already initialized")
		{
			// Manually insert payload into store
			previouslyInitializedPayload := "super-awesome-payload"
			sizeBytes := len(previouslyInitializedPayload)
			pointer := &previouslyInitializedPayload
			fixedSizePayloads.Store(sizeBytes, pointer)

			p, err := initializeAndReturnFixedSizePayload("some-actor-name", PayloadGenerationRequirement{
				FixedSize: FixedSizePayloadDefinition{
					SizeBytes: sizeBytes,
				},
			})

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\treturned pointer must point to payload previously initialized for given size"
			if p == pointer {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}

func TestGenerateRandomStringPayloadWithinBoundary(t *testing.T) {

	t.Log("given an actor name and a payload generation requirement")
	{
		t.Log("\twhen given actor has not previously invoked payload generation yet")
		{
			payloadConsumingActors = sync.Map{}

			actorExtendedName := "mapLoadRunner-ht_load-0"

			r := PayloadGenerationRequirement{
				UseVariableSize: true,
				VariableSize: VariableSizePayloadDefinition{
					LowerBoundaryBytes: 0,
					UpperBoundaryBytes: 10,
					SameSizeStepsLimit: 5,
				},
			}

			p, err := generateRandomStringPayloadWithinBoundary(actorExtendedName, r)

			msg := "\t\tno error must be returned"
			if err == nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tsize of generated payload must correspond to boundaries provided in given payload generation requirement"
			if len(*p) >= r.VariableSize.LowerBoundaryBytes && len(*p) <= r.VariableSize.UpperBoundaryBytes {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnew payload generation info value must have been inserted"
			v, ok := payloadConsumingActors.Load(actorExtendedName)
			if ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tnumber of invocations must have been updated in payload generation info for this actor"
			insertedInfo := v.(VariablePayloadGenerationInfo)
			if insertedInfo.numGeneratePayloadInvocations == 1 {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpayload generation info must contain size of generated payload"
			if insertedInfo.payloadSize == len(*p) {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen given actor has previously invoked payload generation")
		{
			payloadConsumingActors = sync.Map{}

			actorExtendedName := "mapLoadRunner-ht_load-0"

			r := PayloadGenerationRequirement{
				UseVariableSize: true,
				VariableSize: VariableSizePayloadDefinition{
					LowerBoundaryBytes: 0,
					UpperBoundaryBytes: 5001,
					SameSizeStepsLimit: 6,
				},
			}

			previouslyGeneratedPayload := ""
			for i := 0; i < r.VariableSize.SameSizeStepsLimit+1; i++ {
				p, err := generateRandomStringPayloadWithinBoundary(actorExtendedName, r)

				msg := "\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark, i)
				} else {
					t.Fatal(msg, ballotX, i)
				}

				msg = "\t\tnumber of invocations must have been updated in payload generation info for this actor"
				v, _ := payloadConsumingActors.Load(actorExtendedName)

				payloadGenerationInfo := v.(VariablePayloadGenerationInfo)

				var expectedTrackedNumberOfInvocations int
				if i < r.VariableSize.SameSizeStepsLimit {
					expectedTrackedNumberOfInvocations = i + 1
				} else {
					expectedTrackedNumberOfInvocations = 1
				}

				if payloadGenerationInfo.numGeneratePayloadInvocations == expectedTrackedNumberOfInvocations {
					t.Log(msg, checkMark, i)
				} else {
					t.Fatal(msg, ballotX, i, payloadGenerationInfo.numGeneratePayloadInvocations)
				}

				if i == 0 {
					msg = "\t\t\tsize of generated payload must correspond to previously registered payload generation requirement"
					if len(*p) > r.VariableSize.LowerBoundaryBytes && len(*p) <= r.VariableSize.UpperBoundaryBytes {
						t.Log(msg, checkMark, i)
					} else {
						t.Fatal(msg, ballotX, i)
					}
				} else if i < r.VariableSize.SameSizeStepsLimit {
					t.Log("\t\t\twhen number of invocations is within same size step boundary")
					{
						msg = "\t\t\t\tpayload's size must be equal to previous generated payload's size"
						if len(*p) == len(previouslyGeneratedPayload) {
							t.Log(msg, checkMark, i)
						} else {
							t.Fatal(msg, ballotX, i)
						}
					}
				} else {
					t.Log("\t\t\twhen number of invocations exceeds same size step boundary")
					{
						msg = "\t\t\t\tpayload's size must differ from previously generated payload's size"
						if len(*p) != len(previouslyGeneratedPayload) {
							t.Log(msg, checkMark, i)
						} else {
							t.Fatal(msg, ballotX, i)
						}
					}
				}

				previouslyGeneratedPayload = *p
			}

		}
	}

}
