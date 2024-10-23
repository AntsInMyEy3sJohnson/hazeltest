package state

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/api"
	"hazeltest/client"
	"hazeltest/hazelcastwrapper"
	"hazeltest/logging"
	"hazeltest/status"
	"math"
	"strings"
	"sync"
	"time"
)

type (
	BatchCleaner interface {
		Clean() (int, error)
	}
	BatchCleanerBuilder interface {
		Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error)
	}
	DefaultBatchMapCleanerBuilder struct {
		cfb batchCleanerConfigBuilder
	}
	DefaultBatchMapCleaner struct {
		ctx       context.Context
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		cfg       *batchCleanerConfig
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hazelcastwrapper.HzClientHandler
		cih       LastCleanedInfoHandler
		t         CleanedTracker
	}
	DefaultBatchQueueCleanerBuilder struct {
		cfb batchCleanerConfigBuilder
	}
	DefaultBatchQueueCleaner struct {
		ctx       context.Context
		name      string
		hzCluster string
		hzMembers []string
		keyPath   string
		cfg       *batchCleanerConfig
		qs        hazelcastwrapper.QueueStore
		ms        hazelcastwrapper.MapStore
		ois       hazelcastwrapper.ObjectInfoStore
		ch        hazelcastwrapper.HzClientHandler
		cih       LastCleanedInfoHandler
		t         CleanedTracker
	}
	BatchCleanResult struct {
		numCleanedDataStructures int
		err                      error
	}
	batchCleanerConfig struct {
		enabled                               bool
		usePrefix                             bool
		prefix                                string
		parallelCleanNumDataStructuresDivisor uint16
		useCleanAgainThreshold                bool
		cleanAgainThresholdMs                 uint64
		cleanMode                             DataStructureCleanMode
		errorBehavior                         ErrorDuringCleanBehavior
	}
	batchCleanerConfigBuilder struct {
		keyPath string
		a       client.ConfigPropertyAssigner
	}
)

type (
	SingleCleaner interface {
		Clean(name string) SingleCleanResult
	}
	SingleMapCleanerBuildValues struct {
		ctx       context.Context
		ms        hazelcastwrapper.MapStore
		t         CleanedTracker
		cih       LastCleanedInfoHandler
		cleanMode DataStructureCleanMode
	}
	// SingleMapCleanerBuilder is an interface for encapsulating the capability of assembling map cleaners
	// implementing the SingleCleaner interface. An interesting detail is perhaps that the Build methods for
	// single-data-structure cleaners ask for very few values encapsulating knowledge about the target Hazelcast cluster
	// as well as capability to access it when compared to the batch cleaner's Build methods.
	// That is because a batch cleaner is much more standalone in terms of its functionality -- basically, a batch
	// cleaner registers and initializes itself (including a Hazelcast client), and then investigates the
	// target Hazelcast cluster for data structures to be cleaned all on its own. This requires the batch cleaner
	// assembled by the builder to have more knowledge about the target Hazelcast cluster (in order to successfully
	// initialize a working Hazelcast client) as well as substantially more capabilities to assert the current state
	// of the cluster in terms of the data structures it holds. Cleaners for single-data-structure cleaning, on the
	// other hand, are told which data structure to clean. Hence, all the work in connecting to the target Hazelcast
	// cluster and figuring out which data structures there are has already been performed by the caller. Therefore, the
	// caller must have the knowledge and capability for doing so, and might as well pass both into the single-cleaner
	// build method. (Aligning the signature of the single-cleaner's Build methods more to those of the batch cleaners
	// would be possible, but would lead to inefficiency. For example, why would a single cleaner initialize a
	// new Hazelcast client if the caller already has one?)
	SingleMapCleanerBuilder interface {
		Build(bv *SingleMapCleanerBuildValues) (SingleCleaner, string)
	}
	DefaultSingleMapCleanerBuilder struct{}
	DefaultSingleMapCleaner        struct {
		cfg *singleCleanerConfig
		ctx context.Context
		ms  hazelcastwrapper.MapStore
		cih LastCleanedInfoHandler
		t   CleanedTracker
	}
	SingleQueueCleanerBuildValues struct {
		ctx       context.Context
		qs        hazelcastwrapper.QueueStore
		ms        hazelcastwrapper.MapStore
		t         CleanedTracker
		cih       LastCleanedInfoHandler
		cleanMode DataStructureCleanMode
	}
	// SingleQueueCleanerBuilder is an interface for encapsulating the capability of assembling queue cleaners
	// implementing the SingleCleaner interface. Concerning why the Build method asks for so little knowledge about
	// the target Hazelcast cluster and capabilities for accessing it, the same thoughts as on the
	// SingleMapCleanerBuilder interface apply.
	SingleQueueCleanerBuilder interface {
		Build(bv *SingleQueueCleanerBuildValues) SingleCleaner
	}
	DefaultSingleQueueCleanerBuilder struct{}
	DefaultSingleQueueCleaner        struct {
		cfg *singleCleanerConfig
		ctx context.Context
		ms  hazelcastwrapper.MapStore
		qs  hazelcastwrapper.QueueStore
		cih LastCleanedInfoHandler
		t   CleanedTracker
	}
	SingleCleanResult struct {
		NumCleanedItems int
		Err             error
	}
	singleCleanerConfig struct {
		cleanMode   DataStructureCleanMode
		syncMapName string
		hzService   string
	}
)

type (
	LastCleanedInfoHandler interface {
		// check asserts whether the given payload data structure in the target Hazelcast cluster is susceptible
		// to cleaning. In doing so, it acquires a lock on the given sync map (a map that, for each kind of Cleaner
		// implementation, tracks when payload data structures were last cleaned). The lock is not released
		// by this method in order to make sure a caller can invoke check and then update without another caller
		// overwriting the last cleaned info in the sync map in the meantime.
		// To signal to the caller that a lock was acquired and must potentially be released, the method returns
		// a mapLockInfo value -- if non-nil, then there is a lock to be released. See also mapLockInfo.
		// (One might reason that acquiring a lock in method invoked by a caller and then having the caller release the
		// lock is a nice source for bugs -- and it probably is. The use case of the Cleaner, however, dictates this:
		// First, the Cleaner must check whether a given payload data structure is susceptible to cleaning, then
		// it must clean the data structure, and make sure the last cleaned info is updated. To make sure no other
		// actor updates or reads the last cleaned info record during this process, the lock must be held for the
		// process' entire duration. Because check itself cannot know (and should not know) what the caller does
		// with the should clean result, it also cannot know whether it is already time to safely release the lock.
		// Therefore, this task must be handed to the caller -- and it can also not be in the responsibility of the
		// update method, because if a payload data structure was not susceptible to cleaning, then there is nothing to
		// update, hence the caller will not invoke update, but the lock must be released nonetheless. Hence, acquiring
		// and releasing the lock is split between invoker and invoked method.)
		check(syncMapName, payloadDataStructureName, hzService string) (mapLockInfo, bool, error)
		update(lockInfo mapLockInfo) error
	}
	CleanedTracker interface {
		add(name string, cleaned int)
	}
	ErrorDuringCleanBehavior     string
	DataStructureCleanMode       string
	LastCleanedInfoHandlerConfig struct {
		UseCleanAgainThreshold bool
		CleanAgainThresholdMs  uint64
	}
	DefaultLastCleanedInfoHandler struct {
		Ctx context.Context
		Ms  hazelcastwrapper.MapStore
		Cfg *LastCleanedInfoHandlerConfig
	}
	CleanedDataStructureTracker struct {
		G status.Gatherer
	}
	// mapLockInfo exists to signal to the caller of the check method on an implementation of
	// LastCleanedInfoHandler that the method acquired a lock on the map represented by the map
	// proxy object contained in the return value (mapLockInfo.m) for a specific key (mapLockInfo.keyName),
	// so the caller knows it should try to release the lock on the map for the key.
	mapLockInfo struct {
		m            hazelcastwrapper.Map
		mapName, key string
	}
)

const (
	Destroy DataStructureCleanMode = "destroy"
	Evict   DataStructureCleanMode = "evict"
)

const (
	Ignore ErrorDuringCleanBehavior = "ignore"
	Fail   ErrorDuringCleanBehavior = "fail"
)

const (
	HzMapService   = "hz:impl:mapService"
	HzQueueService = "hz:impl:queueService"
)

const (
	mapCleanerBasePath            = "stateCleaners.maps"
	queueCleanerBasePath          = "stateCleaners.queues"
	hzInternalDataStructurePrefix = "__"
	mapCleanersSyncMapName        = hzInternalDataStructurePrefix + "ht.mapCleaners"
	queueCleanersSyncMapName      = hzInternalDataStructurePrefix + "ht.queueCleaners"
)

var (
	builders         []BatchCleanerBuilder
	lp               *logging.LogProvider
	emptyMapLockInfo = mapLockInfo{}
)

func init() {
	register(newMapCleanerBuilder())
	register(newQueueCleanerBuilder())
	lp = logging.GetLogProviderInstance(client.ID())
}

func newMapCleanerBuilder() *DefaultBatchMapCleanerBuilder {

	return &DefaultBatchMapCleanerBuilder{
		cfb: batchCleanerConfigBuilder{
			keyPath: mapCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func newQueueCleanerBuilder() *DefaultBatchQueueCleanerBuilder {

	return &DefaultBatchQueueCleanerBuilder{
		cfb: batchCleanerConfigBuilder{
			keyPath: queueCleanerBasePath,
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func register(cb BatchCleanerBuilder) {
	builders = append(builders, cb)
}

func (t *CleanedDataStructureTracker) add(name string, cleaned int) {

	t.G.Gather(status.Update{Key: name, Value: cleaned})

}

func (b *DefaultBatchMapCleanerBuilder) Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), HzMapService, log.ErrorLevel)
		return nil, HzMapService, err
	}

	clientName := "mapCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.GetClient()}
	cih := &DefaultLastCleanedInfoHandler{
		Ctx: ctx,
		Ms:  ms,
		Cfg: &LastCleanedInfoHandlerConfig{
			UseCleanAgainThreshold: config.useCleanAgainThreshold,
			CleanAgainThresholdMs:  config.cleanAgainThresholdMs,
		},
	}

	t := &CleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.G.AssembleStatusCopy)

	return &DefaultBatchMapCleaner{
		ctx:       ctx,
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		cfg:       config,
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.GetClient()},
		ch:        ch,
		cih:       cih,
		t:         t,
	}, HzMapService, nil

}

func identifyCandidateDataStructures(ois hazelcastwrapper.ObjectInfoStore, ctx context.Context, hzService string) ([]hazelcastwrapper.ObjectInfo, error) {

	infos, err := ois.GetDistributedObjectsInfo(ctx)

	var result []hazelcastwrapper.ObjectInfo
	if err != nil {
		return result, err
	}

	for _, v := range infos {
		if !strings.HasPrefix(v.GetName(), hzInternalDataStructurePrefix) && v.GetServiceName() == hzService {
			result = append(result, v)
		}
	}

	return result, nil

}

func (cih *DefaultLastCleanedInfoHandler) check(syncMapName, payloadDataStructureName, hzService string) (mapLockInfo, bool, error) {

	syncMap, err := cih.Ms.GetMap(cih.Ctx, syncMapName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to retrieve sync map '%s': %v", syncMapName, err), hzService, log.ErrorLevel)
		return emptyMapLockInfo, false, err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved sync map '%s'", syncMapName), hzService, log.TraceLevel)
	lockSucceeded, err := syncMap.TryLock(cih.Ctx, payloadDataStructureName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to acquire lock on sync map '%s' for payload data structure '%s': %v", syncMapName, payloadDataStructureName, err), hzService, log.ErrorLevel)
		return emptyMapLockInfo, false, err
	}

	if !lockSucceeded {
		return emptyMapLockInfo, false, fmt.Errorf("unable to acquire lock on sync map '%s' for payload data structure key '%s'", syncMapName, payloadDataStructureName)
	}

	lockInfo := mapLockInfo{
		m:       syncMap,
		mapName: syncMapName,
		key:     payloadDataStructureName,
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully acquired lock on sync map '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.TraceLevel)
	if !cih.Cfg.UseCleanAgainThreshold {
		// No need to check for the last cleaned timestamp if the cleaner was advised not to apply a clean again threshold
		// (Caution: One might be tempted to check whether to apply a threshold right at the beginning of this method,
		// and, if not, right away return a value indicating the payload data structure must be cleaned, without acquiring a lock
		// for the key in question, and without updating the timestamp. This would imply the assumption, however,
		// that all cleaners of all Hazeltest instances currently working on that Hazelcast cluster were advised not
		// to apply their threshold. This assumption might be incorrect, so even if the current cleaner won't apply
		// a threshold and clean the payload data structure regardless of when it was last cleaned, it's important to
		// update the corresponding time stamp in the sync map nonetheless, which requires locking,
		// updating, and unlocking the entry in question.)
		return lockInfo, true, nil
	}

	v, err := syncMap.Get(cih.Ctx, payloadDataStructureName)
	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon retrieving last updated info from sync map for '%s' for payload data structure '%s'", syncMapName, payloadDataStructureName), hzService, log.ErrorLevel)
		return lockInfo, false, err
	}

	// Value will be nil if key (name of payload map) was not present in sync map
	if v == nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that payload data structure '%s' was never cleaned before", payloadDataStructureName), hzService, log.TraceLevel)
		return lockInfo, true, nil
	}

	var lastCleanedAt int64
	if lc, ok := v.(int64); !ok {
		msg := fmt.Sprintf("unable to treat retrieved value '%v' for payload data structure '%s' as int64 timestamp", v, payloadDataStructureName)
		lp.LogStateCleanerEvent(msg, hzService, log.ErrorLevel)
		return lockInfo, false, errors.New(msg)
	} else {
		lastCleanedAt = lc
	}

	cleanAgainThresholdMs := cih.Cfg.CleanAgainThresholdMs
	lp.LogStateCleanerEvent(fmt.Sprintf("successfully retrieved last updated info from sync map '%s' for payload data structure '%s'; last updated at %d", syncMapName, payloadDataStructureName, lastCleanedAt), hzService, log.TraceLevel)
	if time.Since(time.Unix(lastCleanedAt, 0)) < time.Millisecond*time.Duration(cleanAgainThresholdMs) {
		lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is less than configured threshold of '%d' milliseconds for payload data structure '%s'-- negative cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.TraceLevel)
		return lockInfo, false, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("determined that difference between last cleaned timestamp and current time is greater than or equal to configured threshold of '%d' milliseconds for payload data structure '%s'-- positive cleaning suggestion", cleanAgainThresholdMs, payloadDataStructureName), hzService, log.TraceLevel)
	return lockInfo, true, nil

}

func (cih *DefaultLastCleanedInfoHandler) update(lockInfo mapLockInfo) error {

	mapHoldingLock := lockInfo.m
	payloadDataStructureName := lockInfo.key

	cleanedAt := time.Now().UnixNano()
	ttlAndMaxIdle := time.Millisecond * time.Duration(cih.Cfg.CleanAgainThresholdMs)
	return mapHoldingLock.SetWithTTLAndMaxIdle(cih.Ctx, payloadDataStructureName, cleanedAt, ttlAndMaxIdle, ttlAndMaxIdle)

}

func (c *DefaultBatchMapCleaner) Clean() (int, error) {

	defer func() {
		_ = c.ch.Shutdown(c.ctx)
	}()

	if !c.cfg.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("map cleaner '%s' not enabled; won't run", c.name), HzMapService, log.InfoLevel)
		return 0, nil
	}

	b := DefaultSingleMapCleanerBuilder{}
	bv := &SingleMapCleanerBuildValues{
		ctx:       c.ctx,
		ms:        c.ms,
		t:         c.t,
		cih:       c.cih,
		cleanMode: c.cfg.cleanMode,
	}
	sc, _ := b.Build(bv)

	start := time.Now()
	numCleanedMaps, err := runGenericBatchClean(
		c.ctx,
		c.ois,
		HzMapService,
		c.cfg,
		sc,
	)
	elapsed := time.Since(start).Milliseconds()
	lp.LogTimingEvent("batch map clean", "N/A", int(elapsed), log.InfoLevel)

	return numCleanedMaps, err

}

func (b *DefaultBatchQueueCleanerBuilder) Build(ch hazelcastwrapper.HzClientHandler, ctx context.Context, g status.Gatherer, hzCluster string, hzMembers []string) (BatchCleaner, string, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), HzQueueService, log.ErrorLevel)
		return nil, HzQueueService, err
	}

	clientName := "queueCleaner"
	ch.InitHazelcastClient(ctx, clientName, hzCluster, hzMembers)

	ms := &hazelcastwrapper.DefaultMapStore{Client: ch.GetClient()}
	cih := &DefaultLastCleanedInfoHandler{
		Ms:  ms,
		Ctx: ctx,
		Cfg: &LastCleanedInfoHandlerConfig{
			UseCleanAgainThreshold: config.useCleanAgainThreshold,
			CleanAgainThresholdMs:  config.cleanAgainThresholdMs,
		},
	}

	t := &CleanedDataStructureTracker{g}
	api.RegisterStatefulActor(api.StateCleaners, clientName, t.G.AssembleStatusCopy)

	return &DefaultBatchQueueCleaner{
		ctx:       ctx,
		name:      clientName,
		hzCluster: hzCluster,
		hzMembers: hzMembers,
		keyPath:   b.cfb.keyPath,
		cfg:       config,
		qs:        &hazelcastwrapper.DefaultQueueStore{Client: ch.GetClient()},
		ms:        ms,
		ois:       &hazelcastwrapper.DefaultObjectInfoStore{Client: ch.GetClient()},
		ch:        ch,
		cih:       cih,
		t:         t,
	}, HzQueueService, nil

}

func releaseLock(ctx context.Context, lockInfo mapLockInfo, hzService string) error {

	if lockInfo == emptyMapLockInfo {
		return nil
	}

	if err := lockInfo.m.Unlock(ctx, lockInfo.key); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to release lock on sync map '%s' for key '%s' due to error: %v", lockInfo.mapName, lockInfo.key, err), hzService, log.ErrorLevel)
		return err
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("successfully released lock on sync map '%s' for key '%s'", lockInfo.mapName, lockInfo.key), hzService, log.TraceLevel)
	return nil

}

func (b *DefaultSingleMapCleanerBuilder) Build(bv *SingleMapCleanerBuildValues) (SingleCleaner, string) {

	cfg := &singleCleanerConfig{
		cleanMode:   bv.cleanMode,
		syncMapName: mapCleanersSyncMapName,
		hzService:   HzMapService,
	}
	return &DefaultSingleMapCleaner{
		cfg: cfg,
		ctx: bv.ctx,
		ms:  bv.ms,
		cih: bv.cih,
		t:   bv.t,
	}, HzMapService

}

func runGenericSingleClean(
	ctx context.Context,
	cih LastCleanedInfoHandler,
	t CleanedTracker,
	payloadDataStructureName string,
	cfg *singleCleanerConfig,
	retrieveAndCleanFunc func(payloadDataStructureName string) (int, error),
) SingleCleanResult {

	lockInfo, shouldClean, err := cih.check(cfg.syncMapName, payloadDataStructureName, cfg.hzService)

	// This defer ensures that the lock on the sync map for the given payload map is always released
	// no matter the susceptibility of the payload map for cleaning.
	defer func() {
		if err := releaseLock(ctx, lockInfo, cfg.hzService); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to release lock on '%s' for key '%s' due to error: %v", cfg.syncMapName, payloadDataStructureName, err), cfg.hzService, log.ErrorLevel)
		}
	}()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to determine whether '%s' should be cleaned due to error: %v", payloadDataStructureName, err), cfg.hzService, log.ErrorLevel)
		return SingleCleanResult{0, err}
	}

	if !shouldClean {
		lp.LogStateCleanerEvent(fmt.Sprintf("clean not required for '%s'", payloadDataStructureName), cfg.hzService, log.TraceLevel)
		return SingleCleanResult{0, nil}
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("determined that '%s' should be cleaned of state, commencing...", payloadDataStructureName), cfg.hzService, log.TraceLevel)
	numItemsCleaned, err := retrieveAndCleanFunc(payloadDataStructureName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadDataStructureName, err), cfg.hzService, log.ErrorLevel)
		return SingleCleanResult{0, err}
	}

	if numItemsCleaned > 0 {
		t.add(payloadDataStructureName, numItemsCleaned)
		lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned '%s', which held %d item/-s", payloadDataStructureName, numItemsCleaned), cfg.hzService, log.TraceLevel)
	}

	if err := cih.update(lockInfo); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to update last cleaned info for '%s': %v", payloadDataStructureName, err), cfg.hzService, log.ErrorLevel)
		return SingleCleanResult{numItemsCleaned, err}
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("last cleaned info successfully updated for '%s'", payloadDataStructureName), cfg.hzService, log.TraceLevel)
	return SingleCleanResult{numItemsCleaned, err}

}

func (c *DefaultSingleMapCleaner) retrieveAndClean(payloadMapName string) (int, error) {

	mapToClean, err := c.ms.GetMap(c.ctx, payloadMapName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean '%s' due to error upon retrieval of proxy object from Hazelcast cluster: %v", payloadMapName, err), HzMapService, log.ErrorLevel)
		return 0, err
	}

	if mapToClean == nil {
		msg := fmt.Sprintf("cannot clean '%s' because map retrieved from target Hazelcast cluster was nil", payloadMapName)
		lp.LogStateCleanerEvent(msg, HzMapService, log.ErrorLevel)
		return 0, errors.New(msg)
	}

	size, err := mapToClean.Size(c.ctx)
	if err != nil {
		msg := fmt.Sprintf("unable to clean '%s' because retrieval of current size returned with error: %v", payloadMapName, err)
		lp.LogStateCleanerEvent(msg, HzMapService, log.ErrorLevel)
		return 0, err
	}

	if size == 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("payload map '%s' does not currently hold any items -- skipping", payloadMapName), HzMapService, log.TraceLevel)
		return 0, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("payload map '%s' currently holds %d elements -- proceeding to clean", payloadMapName, size), HzMapService, log.TraceLevel)

	if c.cfg.cleanMode == Destroy {
		if err = mapToClean.Destroy(c.ctx); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon destroying '%s': %v", payloadMapName, err), HzMapService, log.ErrorLevel)
			return 0, err
		}
	} else {
		if err = mapToClean.EvictAll(c.ctx); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadMapName, err), HzMapService, log.ErrorLevel)
			return 0, err
		}
	}
	return size, nil

}

func (c *DefaultSingleMapCleaner) Clean(name string) SingleCleanResult {

	return runGenericSingleClean(
		c.ctx,
		c.cih,
		c.t,
		name,
		c.cfg,
		c.retrieveAndClean,
	)

}

func runGenericBatchClean(
	ctx context.Context,
	ois hazelcastwrapper.ObjectInfoStore,
	hzService string,
	cfg *batchCleanerConfig,
	sc SingleCleaner,
) (int, error) {

	candidateDataStructures, err := identifyCandidateDataStructures(ois, ctx, hzService)

	if err != nil {
		return 0, err
	}

	if len(candidateDataStructures) > 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("identified %d data structure candidate/-s to be considered for state cleaning", len(candidateDataStructures)), hzService, log.TraceLevel)
	} else {
		lp.LogStateCleanerEvent("no data structure candidates for state cleaning identified in target hazelcast cluster", hzService, log.TraceLevel)
		return 0, nil
	}

	var filteredDataStructures []hazelcastwrapper.ObjectInfo
	if cfg.usePrefix {
		lp.LogStateCleanerEvent(fmt.Sprintf("applying prefix '%s' to %d data structure candidate/-s identified for cleaning", cfg.prefix, len(candidateDataStructures)), hzService, log.TraceLevel)
		for _, v := range candidateDataStructures {
			if strings.HasPrefix(v.GetName(), cfg.prefix) {
				filteredDataStructures = append(filteredDataStructures, v)
			}
		}
	} else {
		filteredDataStructures = candidateDataStructures
	}

	numCleanedDataStructures := 0

	cleanResults := performParallelSingleCleans(filteredDataStructures, cfg, sc.Clean, hzService)
	for result := range cleanResults {
		numItemsCleaned, err := result.NumCleanedItems, result.Err
		if numItemsCleaned > 0 {
			numCleanedDataStructures++
		}
		if Fail == cfg.errorBehavior && err != nil {
			return numCleanedDataStructures, err
		}
	}

	return numCleanedDataStructures, nil

}

func calculateNumParallelSingleCleanWorkers(numFilteredDataStructures int, parallelCleanNumDataStructuresDivisor uint16) (uint16, error) {

	if numFilteredDataStructures == 0 {
		return 0, errors.New("cannot calculate number of workers for zero filtered data structures")
	}

	if parallelCleanNumDataStructuresDivisor == 0 {
		return 0, fmt.Errorf("cannot use zero as divisor in operation to calculate number of workers to perform parallel single clean on %d data structure/-s", numFilteredDataStructures)
	}

	return uint16(math.Max(1.0, math.Ceil(float64(numFilteredDataStructures/int(parallelCleanNumDataStructuresDivisor))))), nil

}

func performParallelSingleCleans(
	filteredDataStructures []hazelcastwrapper.ObjectInfo,
	cfg *batchCleanerConfig,
	singleCleanFunc func(name string) SingleCleanResult,
	hzService string,
) <-chan SingleCleanResult {

	if len(filteredDataStructures) == 0 {
		emptyChan := make(chan SingleCleanResult)
		close(emptyChan)
		return emptyChan
	}

	results := make(chan SingleCleanResult, len(filteredDataStructures))

	numWorkers, err := calculateNumParallelSingleCleanWorkers(len(filteredDataStructures), cfg.parallelCleanNumDataStructuresDivisor)
	if err != nil {
		return results
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("using %d worker/-s to perform parallel single clean on %d data structure/-s", numWorkers, len(filteredDataStructures)), hzService, log.InfoLevel)
	cleanTasks := make(chan string, len(filteredDataStructures))
	errorDuringProcessing := make(chan struct{})

	var wg sync.WaitGroup
	var once sync.Once

	for i := uint16(0); i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range cleanTasks {
				select {
				case <-errorDuringProcessing:
					if Fail == cfg.errorBehavior {
						return
					}
				default:
				}
				result := singleCleanFunc(task)
				results <- result
				if result.Err != nil {
					if Fail == cfg.errorBehavior {
						lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning data structure with name '%s' and error behavior was set to '%s', hence aborting after error: %v", task, Fail, result.Err), hzService, log.ErrorLevel)
						once.Do(func() {
							close(errorDuringProcessing)
						})
					} else {
						lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning data structure with name '%s', but error behavior was set to '%s', hence commencing after error: %v", task, Ignore, result.Err), hzService, log.TraceLevel)
					}
				} else {
					lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned %d element/-s from data structure with name '%s'", result.NumCleanedItems, task), hzService, log.TraceLevel)
				}
			}
		}()
	}

	go func() {
		for _, obj := range filteredDataStructures {
			cleanTasks <- obj.GetName()
		}
		close(cleanTasks)
	}()

	go func() {
		wg.Wait()
		close(results)
		once.Do(func() {
			close(errorDuringProcessing)
		})
	}()

	return results

}

func (b *DefaultSingleQueueCleanerBuilder) Build(bv *SingleQueueCleanerBuildValues) (SingleCleaner, string) {

	cfg := &singleCleanerConfig{
		cleanMode:   bv.cleanMode,
		syncMapName: queueCleanersSyncMapName,
		hzService:   HzQueueService,
	}
	return &DefaultSingleQueueCleaner{
		cfg: cfg,
		ctx: bv.ctx,
		qs:  bv.qs,
		ms:  bv.ms,
		cih: bv.cih,
		t:   bv.t,
	}, HzQueueService

}

func (c *DefaultSingleQueueCleaner) retrieveAndClean(payloadQueueName string) (int, error) {

	queueToClean, err := c.qs.GetQueue(c.ctx, payloadQueueName)

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("cannot clean '%s' due to error upon retrieval of proxy object from Hazelcast cluster: %v", payloadQueueName, err), HzQueueService, log.ErrorLevel)
		return 0, err
	}

	if queueToClean == nil {
		msg := fmt.Sprintf("cannot clean '%s' because queue retrieved from target Hazelcast cluster was nil", payloadQueueName)
		lp.LogStateCleanerEvent(msg, HzQueueService, log.ErrorLevel)
		return 0, errors.New(msg)
	}

	size, err := queueToClean.Size(c.ctx)
	if err != nil {
		msg := fmt.Sprintf("unable to clean '%s' because size check failed with error: %v", payloadQueueName, err)
		lp.LogStateCleanerEvent(msg, HzQueueService, log.ErrorLevel)
		return 0, err
	}

	if size == 0 {
		lp.LogStateCleanerEvent(fmt.Sprintf("payload queue '%s' does not currently hold any items -- skipping", payloadQueueName), HzQueueService, log.TraceLevel)
		return 0, nil
	}

	lp.LogStateCleanerEvent(fmt.Sprintf("payload queue '%s' currently holds %d elements -- proceeding to clean", payloadQueueName, size), HzQueueService, log.TraceLevel)

	if err := queueToClean.Clear(c.ctx); err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon cleaning '%s': %v", payloadQueueName, err), HzQueueService, log.ErrorLevel)
		return 0, err
	}

	return size, nil

}

func (c *DefaultSingleQueueCleaner) Clean(name string) SingleCleanResult {

	return runGenericSingleClean(
		c.ctx,
		c.cih,
		c.t,
		name,
		c.cfg,
		c.retrieveAndClean,
	)

}

func (c *DefaultBatchQueueCleaner) Clean() (int, error) {

	defer func() {
		_ = c.ch.Shutdown(c.ctx)
	}()

	if !c.cfg.enabled {
		lp.LogStateCleanerEvent(fmt.Sprintf("queue cleaner '%s' not enabled; won't run", c.name), HzQueueService, log.InfoLevel)
		return 0, nil
	}

	b := DefaultSingleQueueCleanerBuilder{}
	sc, _ := b.Build(&SingleQueueCleanerBuildValues{
		ctx:       c.ctx,
		qs:        c.qs,
		ms:        c.ms,
		t:         c.t,
		cih:       c.cih,
		cleanMode: c.cfg.cleanMode,
	})

	start := time.Now()
	numCleaned, err := runGenericBatchClean(
		c.ctx,
		c.ois,
		HzQueueService,
		c.cfg,
		sc,
	)
	elapsed := time.Since(start).Milliseconds()
	lp.LogTimingEvent("batch queue clean", "N/A", int(elapsed), log.InfoLevel)

	return numCleaned, err

}

func buildCleanerAndInvokeClean(b BatchCleanerBuilder, g status.Gatherer, hzCluster string, hzMembers []string) error {

	c, hzService, err := b.Build(&hazelcastwrapper.DefaultHzClientHandler{}, context.TODO(), g, hzCluster, hzMembers)
	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder for hazelcast due to error: %v", err), hzService, log.ErrorLevel)
		return err
	}

	listenReady := make(chan struct{})
	go g.Listen(listenReady)
	defer g.StopListen()

	<-listenReady

	if numCleanedDataStructures, err := c.Clean(); err != nil {
		if numCleanedDataStructures > 0 {
			lp.LogStateCleanerEvent(fmt.Sprintf("%d data structure/-s were cleaned before encountering error: %v", numCleanedDataStructures, err), hzService, log.ErrorLevel)
		} else {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean data structures: %v", err), hzService, log.ErrorLevel)
		}
		return err
	} else {
		if numCleanedDataStructures > 0 {
			lp.LogStateCleanerEvent(fmt.Sprintf("successfully cleaned state in %d data structure/-s", numCleanedDataStructures), hzService, log.InfoLevel)
		} else {
			lp.LogStateCleanerEvent("cleaner either disabled or no payload data structures were susceptible to cleaning", hzService, log.InfoLevel)
		}
	}

	return nil

}

func RunCleaners(hzCluster string, hzMembers []string) error {

	errorChan := make(chan error, len(builders))

	var wg sync.WaitGroup
	wg.Add(len(builders))

	for _, b := range builders {

		go func(b BatchCleanerBuilder) {
			defer wg.Done()
			if err := buildCleanerAndInvokeClean(b, status.NewGatherer(), hzCluster, hzMembers); err != nil {
				errorChan <- err
			}
		}(b)

	}

	wg.Wait()
	close(errorChan)

	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil

}

func (b batchCleanerConfigBuilder) populateConfig() (*batchCleanerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var cleanMode DataStructureCleanMode
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".cleanMode", client.ValidateString, func(a any) {
			cleanMode = DataStructureCleanMode(a.(string))
		})
	})

	var usePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.enabled", client.ValidateBool, func(a any) {
			usePrefix = a.(bool)
		})
	})

	var prefix string
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.prefix", client.ValidateString, func(a any) {
			prefix = a.(string)
		})
	})

	var parallelCleanNumDataStructuresDivisor uint16
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".parallelCleanNumDataStructuresDivisor", client.ValidateInt, func(a any) {
			parallelCleanNumDataStructuresDivisor = uint16(a.(int))
		})
	})

	var useCleanAgainThreshold bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".cleanAgainThreshold.enabled", client.ValidateBool, func(a any) {
			useCleanAgainThreshold = a.(bool)
		})
	})

	var cleanAgainThresholdMs uint64
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".cleanAgainThreshold.thresholdMs", client.ValidateInt, func(a any) {
			cleanAgainThresholdMs = uint64(a.(int))
		})
	})

	var cleanErrorBehavior ErrorDuringCleanBehavior
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".errorBehavior", ValidateErrorDuringCleanBehavior, func(a any) {
			cleanErrorBehavior = ErrorDuringCleanBehavior(a.(string))
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &batchCleanerConfig{
		enabled:                               enabled,
		cleanMode:                             cleanMode,
		usePrefix:                             usePrefix,
		prefix:                                prefix,
		parallelCleanNumDataStructuresDivisor: parallelCleanNumDataStructuresDivisor,
		useCleanAgainThreshold:                useCleanAgainThreshold,
		cleanAgainThresholdMs:                 cleanAgainThresholdMs,
		errorBehavior:                         cleanErrorBehavior,
	}, nil

}

func ValidateCleanMode(keyPath string, a any) error {
	if err := client.ValidateString(keyPath, a); err != nil {
		return err
	}

	switch a {
	case string(Destroy), string(Evict):
		return nil
	default:
		return fmt.Errorf("expected clean mode to be one of '%s' or '%s', got %v", Destroy, Evict, a)
	}
}

func ValidateErrorDuringCleanBehavior(keyPath string, a any) error {
	if err := client.ValidateString(keyPath, a); err != nil {
		return err
	}

	switch a {
	case string(Ignore), string(Fail):
		return nil
	default:
		return fmt.Errorf("exprected pre-run error behavior to be either '%s' or '%s', got %v", Ignore, Fail, a)
	}

}
