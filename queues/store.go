package queues

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"hazeltest/client"
)

type (
	hzQueueStore interface {
		client.HzClientInitializer
		GetQueue(ctx context.Context, name string) (*hazelcast.Queue, error)
		client.HzClientCloser
	}
	defaultHzQueueStore struct {
		client *hazelcast.Client
	}
)

func (d defaultHzQueueStore) Shutdown(ctx context.Context) error {
	return d.client.Shutdown(ctx)
}

func (d defaultHzQueueStore) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) {
	d.client = client.NewHzClientHelper().InitHazelcastClient(ctx, runnerName, hzCluster, hzMembers)
}

func (d defaultHzQueueStore) GetQueue(ctx context.Context, name string) (*hazelcast.Queue, error) {
	return d.client.GetQueue(ctx, name)
}
