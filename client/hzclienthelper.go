package client

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
)

func InitHazelcastClient(ctx context.Context, hzCluster string, hzMemberAddresses []string) (*hazelcast.Client, error) {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = "hazeltest-map-tester"
	hzConfig.Cluster.Name = "hazelcastimdg"
	hzConfig.Cluster.Network.SetAddresses(hzMemberAddresses...)

	return hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

}