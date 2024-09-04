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
	PayloadConsumingActorTracker struct {
		actors sync.Map
	}
	PayloadGenerationRequirement struct {
		LowerBoundaryBytes, UpperBoundaryBytes int
		SameSizeStepsLimit                     int
	}
	PayloadGenerationInfo struct {
		numGeneratePayloadInvocations int
		payloadSize                   int
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
	ActorTracker           = PayloadConsumingActorTracker{}
	payloadConsumingActors sync.Map
)

func RegisterPayloadGenerationRequirement(actorBaseName string, r PayloadGenerationRequirement) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("registering payload generation requirement for actor '%s': %v", actorBaseName, r), log.TraceLevel)
	ActorTracker.actors.Store(actorBaseName, r)

}

func GenerateTrackedRandomStringPayloadWithinBoundary(actorName string) (string, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("generating payload for actor '%s'", actorName), log.TraceLevel)
	r, err := ActorTracker.FindMatchingPayloadGenerationRequirement(actorName)

	if err != nil {
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("cannot generate payload for actor '%s' because attempt to identify payload generation requirement resulted in error: %v", actorName, err), log.ErrorLevel)
		return "", err
	}

	freshlyInserted := false
	if _, ok := payloadConsumingActors.Load(actorName); !ok {
		freshlyInserted = true
		lp.LogPayloadGeneratorEvent(fmt.Sprintf("creating new payload generation info for actor '%s'", actorName), log.InfoLevel)
		payloadConsumingActors.Store(actorName, PayloadGenerationInfo{})
	}

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("loading payload generation info for actor '%s'", actorName), log.TraceLevel)
	v, _ := payloadConsumingActors.Load(actorName)

	info := v.(PayloadGenerationInfo)

	steps, lower, upper := r.SameSizeStepsLimit, r.LowerBoundaryBytes, r.UpperBoundaryBytes
	if info.numGeneratePayloadInvocations >= steps || freshlyInserted {
		payloadSize := lower + rand.Intn(upper-lower+1)
		if !freshlyInserted {
			lp.LogPayloadGeneratorEvent(fmt.Sprintf("limit of %d invocation/-s for generating payload of same size reached for actor '%s' -- reset counter and determined new payload size of %d bytes", steps, actorName, payloadSize), log.InfoLevel)
		}
		info.numGeneratePayloadInvocations = 0
		info.payloadSize = payloadSize
	}

	info.numGeneratePayloadInvocations++
	payloadConsumingActors.Store(actorName, info)

	return GenerateRandomStringPayload(info.payloadSize), nil

}

// GenerateRandomStringPayload generates a random string payload having a size of n bytes.
// Copied from: https://stackoverflow.com/a/31832326
// May I just add that StackOverflow is such a highly fascinating place.
func GenerateRandomStringPayload(n int) string {

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

	return string(b)

}

func (tr *PayloadConsumingActorTracker) FindMatchingPayloadGenerationRequirement(actorName string) (PayloadGenerationRequirement, error) {

	lp.LogPayloadGeneratorEvent(fmt.Sprintf("attempting to find previously registered payload generation requirement for actor '%s'", actorName), log.TraceLevel)

	var r PayloadGenerationRequirement
	foundMatch := false
	tr.actors.Range(func(key, value any) bool {
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

	msg := fmt.Sprintf("unable to find matching requirement for actor with name '%s'", actorName)
	lp.LogPayloadGeneratorEvent(msg, log.ErrorLevel)
	return r, errors.New(msg)

}
