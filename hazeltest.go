package main

import (
	"hazeltest/maps"
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
)

var (
	trace *log.Logger
	err *log.Logger
)

func init() {
	flags := log.Ldate|log.Ltime|log.Lshortfile
	trace = log.New(os.Stdout, "TRACE: ", flags)
	err = log.New(os.Stderr, "ERROR: ", flags)
}

func main() {

	hzCluster := os.Getenv("HZ_CLUSTER")
	if hzCluster == "" {
		err.Fatal("HZ_CLUSTER environment variable must be provided")
	}

	hzMembers := os.Getenv("HZ_MEMBERS")
	if hzMembers == "" {
		err.Fatal("HZ_MEMBERS environment variable must be provided")
	}

	mapTester := maps.MapTester{ClientID: clientID, HzCluster: hzCluster, HzMembers: strings.Split(hzMembers, ",")}
	mapTester.TestMaps()

}
