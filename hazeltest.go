package main

import (
	"hazeltest/maps"
	"log"
	"os"
	"strings"
)

func main() {

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		log.Fatal("HZ_CLUSTER environment variable must be provided")
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		log.Fatal("HZ_MEMBERS environment variable must be provided")
	}

	mapTester := maps.MapTester{HzCluster: hzCluster, HzMembers: strings.Split(hzMembers, ",")}
	mapTester.TestMaps()

}
