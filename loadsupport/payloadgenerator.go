package loadsupport

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type (
	PayloadProvider interface {
		RegisterPayloadGenerationRequirement(actorBaseName string)
		RetrievePayload(fullActorName string) (*string, error)
	}
	DefaultPayloadProvider struct {
		actorRequirements sync.Map
	}
	PayloadGenerationRequirement struct {
		useFixedSize, useVariableSize bool
		fixedSize                     FixedSizePayloadDefinition
		variableSize                  VariableSizePayloadDefinition
	}
	FixedSizePayloadDefinition struct {
		SizeBytes int
	}
	VariableSizePayloadDefinition struct {
		LowerBoundaryBytes, UpperBoundaryBytes, SameSizeStepsLimit int
	}
	VariablePayloadGenerationInfo struct {
		numGeneratePayloadInvocations int
		payloadSize                   int
	}
)

type (
	FixedPayloadGenerationRequirement struct {
		SizeBytes int
	}
)

// From https://stackoverflow.com/a/31832326
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var (
	lp                     = logging.GetLogProviderInstance(client.ID())
	DefaultProvider        = DefaultPayloadProvider{}
	payloadConsumingActors sync.Map
	fixedSizePayloads      sync.Map
)

func (p *DefaultPayloadProvider) RegisterPayloadGenerationRequirement(actorBaseName string, r PayloadGenerationRequirement) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("registering variable payload generation requirement for actor '%s': %v", actorBaseName, r), log.TraceLevel)
	p.actorRequirements.Store(actorBaseName, r)

}

func (p *DefaultPayloadProvider) RetrievePayload(actorName string) (*string, error) {

	r, err := p.findMatchingPayloadGenerationRequirement(actorName)

	if err != nil {
		msg := fmt.Sprintf("encountered error upon attempt to find maching payload generation requirement for actor '%s': %v", actorName, err)
		lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
		return nil, errors.New(msg)
	}

	if r.useVariableSize && r.useFixedSize {
		msg := fmt.Sprintf("instructions unclear: both variable-size and fixed-size payload enabled for actor '%s'", actorName)
		lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
		return nil, errors.New(msg)
	}

	if r.useVariableSize {
		return generateTrackedRandomStringPayloadWithinBoundary(actorName, r)
	} else {
		return initializeAndReturnFixedSizePayload(actorName, r)
	}

}

func (p *DefaultPayloadProvider) findMatchingPayloadGenerationRequirement(actorName string) (PayloadGenerationRequirement, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("attempting to find previously registered payload generation requirement for actor '%s'", actorName), log.TraceLevel)

	var r PayloadGenerationRequirement
	foundMatch := false
	p.actorRequirements.Range(func(key, value any) bool {
		if strings.HasPrefix(actorName, key.(string)) {
			foundMatch = true
			r = value.(PayloadGenerationRequirement)
			return false
		}
		return true
	})

	if foundMatch {
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("identified previously registered payload generation requirement for actor '%s': %v", actorName, r), log.TraceLevel)
		return r, nil
	}

	msg := fmt.Sprintf("unable to find matching requirement for actor '%s'", actorName)
	lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
	return r, errors.New(msg)

}

// GenerateRandomStringPayload generates a random string payload having a size of n bytes.
// Copied from: https://stackoverflow.com/a/31832326
// May I just add that StackOverflow is such a highly fascinating place.
func GenerateRandomStringPayload(n int) *string {

	src := rand.NewSource(time.Now().UnixNano())

	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	s := string(b)
	return &s

}

func initializeAndReturnFixedSizePayload(actorName string, r PayloadGenerationRequirement) (*string, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("retrieving fixed-size payload for actor '%s'", actorName), log.TraceLevel)

	sizeBytes := r.fixedSize.SizeBytes
	if _, ok := fixedSizePayloads.Load(r.fixedSize.SizeBytes); !ok {
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("performing first-time initialization of fixed-size payload of %d bytes", sizeBytes), log.InfoLevel)
		payload := GenerateRandomStringPayload(sizeBytes)
		fixedSizePayloads.Store(sizeBytes, payload)
	}

	v, _ := fixedSizePayloads.Load(sizeBytes)
	payload := v.(*string)

	return payload, nil

}

func generateTrackedRandomStringPayloadWithinBoundary(actorName string, r PayloadGenerationRequirement) (*string, error) {

	freshlyInserted := false
	if _, ok := payloadConsumingActors.Load(actorName); !ok {
		freshlyInserted = true
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("creating new payload generation info for actor '%s'", actorName), log.InfoLevel)
		payloadConsumingActors.Store(actorName, VariablePayloadGenerationInfo{})
	}

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("loading payload generation info for actor '%s'", actorName), log.TraceLevel)
	v, _ := payloadConsumingActors.Load(actorName)

	info := v.(VariablePayloadGenerationInfo)

	steps, lower, upper := r.variableSize.SameSizeStepsLimit, r.variableSize.LowerBoundaryBytes, r.variableSize.UpperBoundaryBytes
	if info.numGeneratePayloadInvocations >= steps || freshlyInserted {
		payloadSize := lower + rand.Intn(upper-lower+1)
		if !freshlyInserted {
			lp.LogPayloadGeneratorEvent(fmt.Sprintf("limit of %d invocation/-s for generating payload of same size reached for actor '%s' -- reset counter and determined new payload size of %d bytes", steps, actorName, payloadSize), log.TraceLevel)
		}
		info.numGeneratePayloadInvocations = 0
		info.payloadSize = payloadSize
	}

	info.numGeneratePayloadInvocations++
	payloadConsumingActors.Store(actorName, info)

	return GenerateRandomStringPayload(info.payloadSize), nil

}
