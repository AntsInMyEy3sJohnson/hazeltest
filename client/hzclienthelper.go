package client

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"hazeltest/logging"

	"github.com/hazelcast/hazelcast-go-client"
	log "github.com/sirupsen/logrus"
)

type (
	HzClientHelper struct {
		clientID uuid.UUID
		lp       *logging.LogProvider
	}
	HzClientInitializer interface {
		InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string)
	}
	HzClientCloser interface {
		Shutdown(ctx context.Context) error
	}
)

func NewHzClientHelper() HzClientHelper {
	return HzClientHelper{clientID, &logging.LogProvider{ClientID: clientID}}
}

func (h HzClientHelper) InitHazelcastClient(ctx context.Context, runnerName string, hzCluster string, hzMembers []string) *hazelcast.Client {

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = fmt.Sprintf("%s-%s", h.clientID, runnerName)
	hzConfig.Cluster.Name = hzCluster

	hzConfig.Cluster.Unisocket = RetrieveArgValue(ArgUseUniSocketClient).(bool)

	logInternalStateInfo(fmt.Sprintf("hazelcast client config: %+v", hzConfig))

	hzConfig.Cluster.Network.SetAddresses(hzMembers...)

	hzClient, err := hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

	if err != nil {
		// Causes log.Exit(1), which in turn calls os.Exit(1)
		h.lp.LogHzEvent(fmt.Sprintf("unable to initialize hazelcast client: %s", err), log.FatalLevel)
	}

	return hzClient

}

func logInternalStateInfo(msg string) {

	log.WithFields(log.Fields{
		"kind":   logging.InternalStateEvent,
		"client": ID(),
	}).Info(msg)

}
