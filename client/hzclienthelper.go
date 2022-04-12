package client

import (
	"context"
	"hazeltest/client/config"

	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
)

func InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMemberAddresses []string) (*hazelcast.Client, error) {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = clientName
	hzConfig.Cluster.Name = hzCluster

	useUniSocketClient, ok := config.RetrieveArgValue(config.ArgUseUniSocketClient).(bool)
	if !ok {
		log.WithFields(log.Fields{
			"kind": "invalid or incomplete configuration",
			"value": "use-unisocket-client",
			"source": "command line",
			"client": ClientID(),
		}).Warn("unable to convert value into bool -- using default instead")
		useUniSocketClient = false
	}
	hzConfig.Cluster.Unisocket = useUniSocketClient

	log.WithFields(log.Fields{
		"kind": "internal state information",
		"client": ClientID(),
	}).Infof("hazelcast client config: %+v", hzConfig)

	hzConfig.Cluster.Network.SetAddresses(hzMemberAddresses...)

	return hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

}
