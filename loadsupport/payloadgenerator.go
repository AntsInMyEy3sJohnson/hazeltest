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
		RegisterPayloadGenerationRequirement(actorBaseName string, r PayloadGenerationRequirement)
		RetrievePayload(fullActorName string) (*PayloadWrapper, error)
	}
	DefaultPayloadProvider struct {
		actorRequirements sync.Map
	}
	PayloadGenerationRequirement struct {
		UseFixedSize, UseVariableSize bool
		FixedSize                     FixedSizePayloadDefinition
		VariableSize                  VariableSizePayloadDefinition
	}
	FixedSizePayloadDefinition struct {
		SizeBytes int
	}
	VariableSizePayloadDefinition struct {
		LowerBoundaryBytes, UpperBoundaryBytes, SameSizeStepsLimit int
	}
	PayloadWrapper struct {
		Payload []byte
	}
	variablePayloadGenerationInfo struct {
		numGeneratePayloadInvocations int
		payloadSize                   int
	}
	fixedSizePayloadsWrapper struct {
		p map[int]*PayloadWrapper
		m sync.Mutex
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
	payloadConsumingActors sync.Map
	fixedSizePayloads      = fixedSizePayloadsWrapper{
		p: make(map[int]*PayloadWrapper),
		m: sync.Mutex{},
	}
)

func (p *DefaultPayloadProvider) RegisterPayloadGenerationRequirement(actorBaseName string, r PayloadGenerationRequirement) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("registering variable payload generation requirement for actor with base name '%s': %v", actorBaseName, r), log.TraceLevel)
	p.actorRequirements.Store(actorBaseName, r)

}

func (p *DefaultPayloadProvider) RetrievePayload(actorName string) (*PayloadWrapper, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("retrieving payload for actor '%s'", actorName), log.TraceLevel)

	r, err := p.findMatchingPayloadGenerationRequirement(actorName)

	if err != nil {
		msg := fmt.Sprintf("encountered error upon attempt to find maching payload generation requirement for actor '%s': %v", actorName, err)
		lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
		return nil, errors.New(msg)
	}

	if r.UseVariableSize && r.UseFixedSize {
		msg := fmt.Sprintf("instructions unclear: both variable-size and fixed-size payload enabled for actor '%s'", actorName)
		lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
		return nil, errors.New(msg)
	}

	if r.UseVariableSize {
		return generateRandomStringPayloadWithinBoundary(actorName, r)
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
func GenerateRandomStringPayload(n int) *PayloadWrapper {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("generating random string payload having size of %d byte/-s", n), log.TraceLevel)

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

	return &PayloadWrapper{Payload: b}

}

func initializeAndReturnFixedSizePayload(actorName string, r PayloadGenerationRequirement) (*PayloadWrapper, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("initializing fixed-size payload for actor '%s'", actorName), log.TraceLevel)

	sizeBytes := r.FixedSize.SizeBytes
	fixedSizePayloads.m.Lock()
	defer fixedSizePayloads.m.Unlock()

	if _, ok := fixedSizePayloads.p[r.FixedSize.SizeBytes]; !ok {
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("performing first-time initialization of fixed-size payload of %d bytes", sizeBytes), log.InfoLevel)
		payload := GenerateRandomStringPayload(sizeBytes)
		fixedSizePayloads.p[sizeBytes] = payload
	}

	return fixedSizePayloads.p[sizeBytes], nil

}

func generateRandomStringPayloadWithinBoundary(actorName string, r PayloadGenerationRequirement) (*PayloadWrapper, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("generating random string payload for actor '%s' according to payload generation requirement: %v", actorName, r), log.TraceLevel)

	freshlyInserted := false
	if _, ok := payloadConsumingActors.Load(actorName); !ok {
		freshlyInserted = true
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("creating new payload generation info for actor '%s'", actorName), log.InfoLevel)
		payloadConsumingActors.Store(actorName, variablePayloadGenerationInfo{})
	}

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("loading payload generation info for actor '%s'", actorName), log.TraceLevel)
	v, _ := payloadConsumingActors.Load(actorName)

	info := v.(variablePayloadGenerationInfo)

	steps, lower, upper := r.VariableSize.SameSizeStepsLimit, r.VariableSize.LowerBoundaryBytes, r.VariableSize.UpperBoundaryBytes
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
