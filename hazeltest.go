package main

import (
	"hazeltest/maps"
)

func main() {

	mapTester := maps.MapTester{HzCluster: "hazelcastimdg", HzMemberAddresses: []string{"10.211.55.6"}}
	mapTester.TestMaps()

}