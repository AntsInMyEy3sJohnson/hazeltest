package tweets

import (
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/client/config"
	"hazeltest/logging"
	"hazeltest/queues"
)

type Runner struct{}

func init() {
	queues.Register(Runner{})
}

func (r Runner) RunQueueTests(hzCluster string, hzMembers []string) {

	runnerConfig := populateConfig()

	if !runnerConfig.Enabled {
		logInternalStateEvent("tweetrunner not enabled -- won't run", log.InfoLevel)
		return
	}

}

func populateConfig() *queues.RunnerConfig {

	parsedConfig := config.GetParsedConfig()

	return queues.RunnerConfigBuilder{
		RunnerKeyPath: "queuetests.tweets",
		ParsedConfig:  parsedConfig,
	}.PopulateConfig()

}

func logInternalStateEvent(msg string, logLevel log.Level) {

	fields := log.Fields{
		"kind":   logging.InternalStateInfo,
		"client": client.ClientID(),
	}

	if logLevel == log.TraceLevel {
		log.WithFields(fields).Trace(msg)
	} else {
		log.WithFields(fields).Info(msg)
	}

}
