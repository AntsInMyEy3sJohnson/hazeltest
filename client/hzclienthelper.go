package client

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client"
)

func InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMemberAddresses []string) (*hazelcast.Client, error) {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = clientName
	hzConfig.Cluster.Name = hzCluster
	hzConfig.Cluster.Network.SetAddresses(hzMemberAddresses...)

	return hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

}
