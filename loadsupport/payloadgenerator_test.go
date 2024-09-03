package loadsupport

import (
	"sync"
	"testing"
)

const (
	checkMark = "\u2713"
	ballotX   = "\u2717"
)

func TestGenerateTrackedRandomStringPayloadWithinBoundary(t *testing.T) {

	t.Log("given an actor name")
	{
		t.Log("\twhen no actor with matching name has previously registered a payload generation requirement")
		{
			p, err := GenerateTrackedRandomStringPayloadWithinBoundary("awesome-actor")

			msg := "\t\terror must be returned"
			if err != nil {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tempty string must be returned"
			if p == "" {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}

		t.Log("\twhen actor has previously registered")
		{
			t.Log("\t\twhen given actor has not previously invoked payload generation yet")
			{
				payloadConsumingActors = sync.Map{}

				actorBaseName := "mapLoadRunner"
				actorExtendedName := "mapLoadRunner-ht_load-0"

				tr = payloadConsumingActorTracker{}

				registeredRequirement := PayloadGenerationRequirement{
					LowerBoundaryBytes: 0,
					UpperBoundaryBytes: 10,
					SameSizeStepsLimit: 5,
				}
				RegisterPayloadGenerationRequirement(actorBaseName, registeredRequirement)

				p, err := GenerateTrackedRandomStringPayloadWithinBoundary(actorExtendedName)

				msg := "\t\t\tno error must be returned"
				if err == nil {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tsize of generated payload must correspond to previously registered payload generation requirement"
				if len(p) >= registeredRequirement.LowerBoundaryBytes && len(p) <= registeredRequirement.UpperBoundaryBytes {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tnew payload generation info value must have been inserted"
				v, ok := payloadConsumingActors.Load(actorExtendedName)
				if ok {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tnumber of invocations must have been updated in payload generation info for this actor"
				insertedInfo := v.(PayloadGenerationInfo)
				if insertedInfo.numGeneratePayloadInvocations == 1 {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}

				msg = "\t\t\tpayload generation info must contain size of generated payload"
				if insertedInfo.payloadSize == len(p) {
					t.Log(msg, checkMark)
				} else {
					t.Fatal(msg, ballotX)
				}
			}

			t.Log("\t\twhen given actor has previously invoked payload generation")
			{
				payloadConsumingActors = sync.Map{}

				actorBaseName := "mapLoadRunner"
				actorExtendedName := "mapLoadRunner-ht_load-0"

				tr = payloadConsumingActorTracker{}

				registeredRequirement := PayloadGenerationRequirement{
					LowerBoundaryBytes: 0,
					UpperBoundaryBytes: 5001,
					SameSizeStepsLimit: 6,
				}
				RegisterPayloadGenerationRequirement(actorBaseName, registeredRequirement)

				previouslyGeneratedPayload := ""
				for i := 0; i < registeredRequirement.SameSizeStepsLimit+1; i++ {
					p, err := GenerateTrackedRandomStringPayloadWithinBoundary(actorExtendedName)

					msg := "\t\t\tno error must be returned"
					if err == nil {
						t.Log(msg, checkMark, i)
					} else {
						t.Fatal(msg, ballotX, i)
					}

					msg = "\t\t\tnumber of invocations must have been updated in payload generation info for this actor"
					v, _ := payloadConsumingActors.Load(actorExtendedName)

					payloadGenerationInfo := v.(PayloadGenerationInfo)

					var expectedTrackedNumberOfInvocations int
					if i < registeredRequirement.SameSizeStepsLimit {
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
						if len(p) > registeredRequirement.LowerBoundaryBytes && len(p) <= registeredRequirement.UpperBoundaryBytes {
							t.Log(msg, checkMark, i)
						} else {
							t.Fatal(msg, ballotX, i)
						}
					} else if i < registeredRequirement.SameSizeStepsLimit {
						t.Log("\t\t\twhen number of invocations is within same size step boundary")
						{
							msg = "\t\t\t\tpayload's size must be equal to previous generated payload's size"
							if len(p) == len(previouslyGeneratedPayload) {
								t.Log(msg, checkMark, i)
							} else {
								t.Fatal(msg, ballotX, i)
							}
						}
					} else {
						t.Log("\t\t\twhen number of invocations exceeds same size step boundary")
						{
							msg = "\t\t\t\tpayload's size must differ from previously generated payload's size"
							if len(p) != len(previouslyGeneratedPayload) {
								t.Log(msg, checkMark, i)
							} else {
								t.Fatal(msg, ballotX, i)
							}
						}
					}

					previouslyGeneratedPayload = p
				}

			}
		}

	}

}

func TestPayloadConsumingActorTracker_findMatchingRequirement(t *testing.T) {

	t.Log("given an actor name")
	{
		t.Log("\twhen no actor with corresponding base name has previously registered")
		{
			tr = payloadConsumingActorTracker{}
			// Register a couple of dummy actors
			for _, a := range []string{"aragorn", "gimli", "legolas"} {
				RegisterPayloadGenerationRequirement(a, PayloadGenerationRequirement{LowerBoundaryBytes: len(a)})
			}

			r, err := tr.findMatchingRequirement("super-awesome-actor-name")

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
			tr = payloadConsumingActorTracker{}

			actorBaseName := "mapLoadRunner"
			registeredRequirement := PayloadGenerationRequirement{
				LowerBoundaryBytes: 500,
				UpperBoundaryBytes: 2000,
				SameSizeStepsLimit: 250,
			}
			RegisterPayloadGenerationRequirement(actorBaseName, registeredRequirement)

			RegisterPayloadGenerationRequirement("mapPokedexRunner", PayloadGenerationRequirement{})

			r, err := tr.findMatchingRequirement("mapLoadRunner-ht_load-0")

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

func TestRegisterPayloadGenerationRequirement(t *testing.T) {

	t.Log("given an actor's base and a payload generation requirement")
	{
		t.Log("\twhen actor invokes registration")
		{
			actorBaseName := "mapLoadRunner"
			r := PayloadGenerationRequirement{}

			RegisterPayloadGenerationRequirement(actorBaseName, r)

			registered, ok := tr.actors.Load(actorBaseName)
			msg := "\t\tactor must have been registered"
			if ok {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}

			msg = "\t\tpayload generation requirement must have been inserted"
			if registered == r {
				t.Log(msg, checkMark)
			} else {
				t.Fatal(msg, ballotX)
			}
		}
	}

}
