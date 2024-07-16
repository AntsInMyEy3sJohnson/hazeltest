package hazelcastwrapper

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
)

type (
	HzClientHelper struct {
		clientID uuid.UUID
		lp       *logging.LogProvider
	}
	HzClientInitializer interface {
		InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string)
	}
	HzClientCloser interface {
		Shutdown(ctx context.Context) error
	}
)

func NewHzClientHelper() HzClientHelper {
	return HzClientHelper{client.ID(), &logging.LogProvider{ClientID: client.ID()}}
}

func (h HzClientHelper) AssembleHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) *hazelcast.Client {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = fmt.Sprintf("%s-%s", h.clientID, clientName)
	hzConfig.Cluster.Name = hzCluster

	hzConfig.Cluster.Unisocket = client.RetrieveArgValue(client.ArgUseUniSocketClient).(bool)

	h.lp.LogInternalStateInfo(fmt.Sprintf("hazelcast client config: %+v", hzConfig), log.InfoLevel)

	hzConfig.Cluster.Network.SetAddresses(hzMembers...)

	hzClient, err := hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

	if err != nil {
		// Causes log.Exit(1), which in turn calls os.Exit(1)
		h.lp.LogHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s", err), log.FatalLevel)
	}

	return hzClient

}
