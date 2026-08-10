package hazelcastwrapper

import (
	"context"
	"fmt"
	"hazeltest/client"

	"github.com/google/uuid"
	"github.com/hazelcast/hazelcast-go-client"
	log "go.uber.org/zap/zapcore"
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
		GetClusterName() string
		GetClusterMembers() []string
		HzClientInitializer
		HzClientCloser
	}
	DefaultHzClientHandler struct {
		HzCluster string
		HzMembers []string
		hzClient  *hazelcast.Client
	}
	HzClientAssembler struct {
		clientID uuid.UUID
		lp       *client.LogProvider
	}
)

const loggingComponent = "hzClientAssembler"

func (ch *DefaultHzClientHandler) InitHazelcastClient(ctx context.Context, clientName string, hzCluster string, hzMembers []string) {
	ch.hzClient = NewHzClientHelper().Assemble(ctx, clientName, hzCluster, hzMembers)
}

func (ch *DefaultHzClientHandler) Shutdown(ctx context.Context) error {
	return ch.hzClient.Shutdown(ctx)
}

func (ch *DefaultHzClientHandler) GetClusterName() string {
	return ch.HzCluster
}

func (ch *DefaultHzClientHandler) GetClusterMembers() []string {
	return ch.HzMembers
}

func (ch *DefaultHzClientHandler) GetClient() *hazelcast.Client {
	return ch.hzClient
}

func NewHzClientHelper() HzClientAssembler {

	lp, err := client.AssembleLogProviderInstance(client.ID(), loggingComponent)

	if err != nil {
		panic(err)
	}

	return HzClientAssembler{client.ID(), lp}
}

func (h HzClientAssembler) Assemble(ctx context.Context, clientName string, hzCluster string, hzMembers []string) *hazelcast.Client {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = fmt.Sprintf("%s-%s", h.clientID, clientName)
	hzConfig.Cluster.Name = hzCluster

	hzConfig.Cluster.Unisocket = client.RetrieveArgValue(client.ArgUseUniSocketClient).(bool)

	h.lp.Log(func() string { return fmt.Sprintf("hazelcast client config: %+v", hzConfig) }, client.InternalStateEvent, log.InfoLevel)

	hzConfig.Cluster.Network.SetAddresses(hzMembers...)

	hzClient, err := hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

	if err != nil {
		h.lp.Log(func() string { return fmt.Sprintf("unable to initialize hazelcast client: %s", err) }, client.HzEvent, log.FatalLevel)
	}

	return hzClient

}
