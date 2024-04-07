package state

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hazeltest/client"
	"hazeltest/logging"
)

type (
	cleanerBuilder interface {
		build() (cleaner, error)
	}
	cleaner interface {
		clean() error
	}
	cleanerConfig struct {
		enabled   bool
		usePrefix bool
		prefix    string
	}
	cleanerConfigBuilder struct {
		keyPath string
		a       client.ConfigPropertyAssigner
	}
	mapCleanerBuilder struct {
		cfb cleanerConfigBuilder
	}
	mapCleaner struct {
		keyPath string
		c       *cleanerConfig
	}
)

const (
	baseKeyPath = "stateCleaner"
)

var (
	builders []cleanerBuilder
	lp       *logging.LogProvider
)

func init() {
	register(newMapCleanerBuilder())
	lp = &logging.LogProvider{ClientID: client.ID()}
}

func newMapCleanerBuilder() *mapCleanerBuilder {

	return &mapCleanerBuilder{
		cfb: cleanerConfigBuilder{
			keyPath: baseKeyPath + ".maps",
			a:       client.DefaultConfigPropertyAssigner{},
		},
	}

}

func register(cb cleanerBuilder) {
	builders = append(builders, cb)
}

func (b *mapCleanerBuilder) build() (cleaner, error) {

	config, err := b.cfb.populateConfig()

	if err != nil {
		lp.LogStateCleanerEvent(fmt.Sprintf("unable to populate state cleaner config for key path '%s' due to error: %v", b.cfb.keyPath, err), log.ErrorLevel)
		return nil, err
	}

	return &mapCleaner{
		keyPath: b.cfb.keyPath,
		c:       config,
	}, nil

}

func (c *mapCleaner) clean() error {

	return nil

}

func RunCleaners(hzCluster string, hzMembers []string) error {

	for _, b := range builders {

		c, err := b.build()
		if err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("unable to construct state cleaning builder due to error: %v", err), log.ErrorLevel)
			return err
		}

		if err := c.clean(); err != nil {
			lp.LogStateCleanerEvent(fmt.Sprintf("encountered error upon attempt to clean state: %v", err), log.ErrorLevel)
			return err
		}
	}

	return nil

}

func (b cleanerConfigBuilder) populateConfig() (*cleanerConfig, error) {

	var assignmentOps []func() error

	var enabled bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".enabled", client.ValidateBool, func(a any) {
			enabled = a.(bool)
		})
	})

	var usePrefix bool
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.enabled", client.ValidateBool, func(a any) {
			usePrefix = a.(bool)
		})
	})

	var prefix string
	assignmentOps = append(assignmentOps, func() error {
		return b.a.Assign(b.keyPath+".prefix.prefix", client.ValidateString, func(a any) {
			prefix = a.(string)
		})
	})

	for _, f := range assignmentOps {
		if err := f(); err != nil {
			return nil, err
		}
	}

	return &cleanerConfig{
		enabled:   enabled,
		usePrefix: usePrefix,
		prefix:    prefix,
	}, nil

}
