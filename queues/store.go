package queues

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"hazeltest/client"
)

type (
	hzQueue interface {
		Put(ctx context.Context, element any) error
		Poll(ctx context.Context) (any, error)
		RemainingCapacity(ctx context.Context) (int, error)
		Destroy(ctx context.Context) error
	}
	hzQueueStore interface {
		client.HzClientInitializer
		GetQueue(ctx context.Context, name string) (hzQueue, error)
		client.HzClientCloser
	}
	defaultHzQueueStore struct {
		client *hazelcast.Client
	}
)

func (d *defaultHzQueueStore) Shutdown(ctx context.Context) error {
	return d.client.Shutdown(ctx)
}

func (d *defaultHzQueueStore) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) {
	d.client = client.NewHzClientHelper().AssembleHazelcastClient(ctx, runnerName, hzCluster, hzMembers)
}

func (d *defaultHzQueueStore) GetQueue(ctx context.Context, name string) (hzQueue, error) {
	return d.client.GetQueue(ctx, name)
}
