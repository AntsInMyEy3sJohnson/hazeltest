package hazelcastwrapper

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"time"
)

type (
	MapStore interface {
		HzClientInitializer
		GetMap(ctx context.Context, name string) (Map, error)
		HzClientCloser
	}
	Map interface {
		ContainsKey(ctx context.Context, key any) (bool, error)
		Set(ctx context.Context, key any, value any) error
		SetWithTTLAndMaxIdle(ctx context.Context, key, value any, ttl time.Duration, maxIdle time.Duration) error
		Get(ctx context.Context, key any) (any, error)
		Remove(ctx context.Context, key any) (any, error)
		Destroy(ctx context.Context) error
		Size(ctx context.Context) (int, error)
		RemoveAll(ctx context.Context, predicate predicate.Predicate) error
		EvictAll(ctx context.Context) error
		TryLock(ctx context.Context, key any) (bool, error)
		Unlock(ctx context.Context, key any) error
	}
	DefaultMapStore struct {
		Client *hazelcast.Client
	}
)

type (
	QueueStore interface {
		HzClientInitializer
		GetQueue(ctx context.Context, name string) (Queue, error)
	}
	Queue interface {
		Clear(ctx context.Context) error
		Size(ctx context.Context) (int, error)
		Put(ctx context.Context, element any) error
		Poll(ctx context.Context) (any, error)
		RemainingCapacity(ctx context.Context) (int, error)
		Destroy(ctx context.Context) error
	}
	DefaultQueueStore struct {
		Client *hazelcast.Client
	}
)

type (
	ObjectInfo interface {
		GetName() string
		GetServiceName() string
	}
	ObjectInfoStore interface {
		GetDistributedObjectsInfo(ctx context.Context) ([]ObjectInfo, error)
	}
	DefaultObjectInfoStore struct {
		Client *hazelcast.Client
	}
	SimpleObjectInfo struct {
		name, serviceName string
	}
)

func (d *DefaultMapStore) Shutdown(ctx context.Context) error {
	return d.Client.Shutdown(ctx)
}

func (d *DefaultMapStore) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) {
	d.Client = NewHzClientHelper().AssembleHazelcastClient(ctx, runnerName, hzCluster, hzMembers)
}

func (d *DefaultMapStore) GetMap(ctx context.Context, name string) (Map, error) {
	return d.Client.GetMap(ctx, name)
}

func (d *DefaultQueueStore) Shutdown(ctx context.Context) error {
	return d.Client.Shutdown(ctx)
}

func (d *DefaultQueueStore) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) {
	d.Client = NewHzClientHelper().AssembleHazelcastClient(ctx, runnerName, hzCluster, hzMembers)
}

func (d *DefaultQueueStore) GetQueue(ctx context.Context, name string) (Queue, error) {
	return d.Client.GetQueue(ctx, name)
}

func (ois *DefaultObjectInfoStore) GetDistributedObjectsInfo(ctx context.Context) ([]ObjectInfo, error) {

	infos, err := ois.Client.GetDistributedObjectsInfo(ctx)

	if err != nil {
		return nil, err
	}

	var result []ObjectInfo

	for _, v := range infos {
		i := &SimpleObjectInfo{
			name:        v.Name,
			serviceName: v.ServiceName,
		}
		result = append(result, i)
	}

	return result, nil

}

func (i SimpleObjectInfo) GetName() string {

	return i.name

}

func (i SimpleObjectInfo) GetServiceName() string {

	return i.serviceName

}
