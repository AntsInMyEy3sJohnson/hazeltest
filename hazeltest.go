package main

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {

	ctx := context.TODO()

	hzConfig := &hazelcast.Config{}
	hzConfig.ClientName = "hazeltest"
	hzConfig.Cluster.Name = "hazelcastimdg"
	hzConfig.Cluster.Network.SetAddresses("10.211.55.6")

	hzClient, err := hazelcast.StartNewClientWithConfig(ctx, *hzConfig)

	if err != nil {
		panic(err)
	}

	strings, err := hzClient.GetMap(ctx, "strings")

	if err != nil {
		panic(err)
	}

	if err := strings.Set(ctx, "mykey", "myvalue"); err != nil {
		panic(err)
	}

	if value, err := strings.Get(ctx, "mykey"); err != nil {
		panic(err)
	} else {
		fmt.Printf("Retrieved value from map: %s\n", value)
	}

	hzClient.Shutdown(ctx)

}