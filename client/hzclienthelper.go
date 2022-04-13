package client

import (
	"context"
	"fmt"
	"hazeltest/client/config"
	"hazeltest/logging"

	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
)

func InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMemberAddresses []string) (*hazelcast.Client, error) {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = clientName
	hzConfig.Cluster.Name = hzCluster

	useUniSocketClient, ok := config.RetrieveArgValue(config.ArgUseUniSocketClient).(bool)
	if !ok {
		logConfigurationError(config.ArgUseUniSocketClient, "command line", "unable to convert value into bool -- using default instead")
		useUniSocketClient = false
	}
	hzConfig.Cluster.Unisocket = useUniSocketClient

	logInternalStateInfo(fmt.Sprintf("hazelcast client config: %+v", hzConfig))

	hzConfig.Cluster.Network.SetAddresses(hzMemberAddresses...)

	return hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

}

func logInternalStateInfo(msg string) {

	log.WithFields(log.Fields{
		"kind": logging.InternalStateInfo,
		"client": ClientID(),
	}).Info(msg)

}

func logConfigurationError(configValue string, source string, msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.ConfigurationError,
		"value": configValue,
		"source": source,
		"client": ClientID(),
	}).Warn(msg)

}
