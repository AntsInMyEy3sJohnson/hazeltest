package maps

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"hazeltest/client"
)

type (
	hzMap interface {
		ContainsKey(ctx context.Context, key any) (bool, error)
		Set(ctx context.Context, key any, value any) error
		Get(ctx context.Context, key any) (any, error)
		Remove(ctx context.Context, key any) (any, error)
		Destroy(ctx context.Context) error
		Size(ctx context.Context) (int, error)
		GetKeySetWithPredicate(ctx context.Context, predicate predicate.Predicate) ([]any, error)
	}
	hzMapStore interface {
		client.HzClientInitializer
		GetMap(ctx context.Context, name string) (hzMap, error)
		client.HzClientCloser
	}
	defaultHzMapStore struct {
		hzClient *hazelcast.Client
	}
)

func (d *defaultHzMapStore) Shutdown(ctx context.Context) error {
	return d.hzClient.Shutdown(ctx)
}

func (d *defaultHzMapStore) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) {
	d.hzClient = client.NewHzClientHelper().InitHazelcastClient(ctx, runnerName, hzCluster, hzMembers)
}

func (d *defaultHzMapStore) GetMap(ctx context.Context, name string) (hzMap, error) {
	return d.hzClient.GetMap(ctx, name)
}
