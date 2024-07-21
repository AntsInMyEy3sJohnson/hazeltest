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
	HzClientInitializer interface {
		InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string)
	}
	HzClientCloser interface {
		Shutdown(ctx context.Context) error
	}
	HzClientHandler interface {
		GetClient() *hazelcast.Client
		HzClientInitializer
		HzClientCloser
	}
	DefaultHzClientHandler struct {
		hzClient *hazelcast.Client
	}
	HzClientAssembler struct {
		clientID uuid.UUID
		lp       *logging.LogProvider
	}
)

func (ch *DefaultHzClientHandler) InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) {
	ch.hzClient = NewHzClientHelper().Assemble(ctx, clientName, hzCluster, hzMembers)
}

func (ch *DefaultHzClientHandler) Shutdown(ctx context.Context) error {
	return ch.hzClient.Shutdown(ctx)
}

func (ch *DefaultHzClientHandler) GetClient() *hazelcast.Client {
	return ch.hzClient
}

func NewHzClientHelper() HzClientAssembler {
	return HzClientAssembler{client.ID(), &logging.LogProvider{ClientID: client.ID()}}
}

func (h HzClientAssembler) Assemble(ctx context.Context, clientName string, hzCluster string, hzMembers []string) *hazelcast.Client {

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
