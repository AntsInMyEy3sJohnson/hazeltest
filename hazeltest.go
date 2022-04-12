package main

import (
	"hazeltest/maps"
	_ "hazeltest/maps/pokedex"
	"log"
	"os"
	"strings"
)

var (
	errLogger *log.Logger
)

func init() {
	flags := log.Ldate | log.Ltime | log.Lshortfile
	errLogger = log.New(os.Stderr, "ERROR: ", flags)
}

func main() {

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		errLogger.Fatal("HZ_CLUSTER environment variable must be provided")
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		errLogger.Fatal("HZ_MEMBERS environment variable must be provided")
	}

	mapTester := maps.MapTester{HzCluster: hzCluster, HzMembers: strings.Split(hzMembers, ",")}
	mapTester.TestMaps()

}
