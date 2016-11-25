package main

import (
	"github.com/funkygao/gohelix"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	zkSvr      = "localhost:2181"
	cluster    = "foobar"
	node1      = "localhost_10925"
	node2      = "localhost_10926"
	stateModel = gohelix.StateModelOnlineOffline
	resource   = "redis"
	partitions = 5
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	admin := gohelix.NewZKHelixAdmin(zkSvr)
	admin.AddCluster(cluster)
	log.Printf("added cluster: %s", cluster)
	//	defer admin.DropCluster(cluster)
	admin.AllowParticipantAutoJoin(cluster, true)

	// add 2 nodes to the cluster
	err := admin.AddNode(cluster, node1)
	must(err)
	err = admin.AddNode(cluster, node2)
	must(err)
	log.Printf("node: %s %s added to cluster[%s]", node1, node2, cluster)

	// define the resource and partition
	err = admin.AddResource(cluster, resource, partitions, stateModel)
	must(err)
	log.Printf("resource[%s] partitions:%d model:%s added to cluster[%s]", resource,
		partitions, stateModel, cluster)

	// start contoller
	go func() {
		err = gohelix.StartController(cluster)
		must(err)
	}()
	log.Println("controller started")

	manager := gohelix.NewHelixManager(zkSvr)
	participant := manager.NewParticipant(cluster, "localhost", "12925")
	participant.AddPreConnectCallback(func() {
		log.Println("participant trying conn...")
	})

	// creaet OnlineOffline state model
	sm := gohelix.NewStateModel([]gohelix.Transition{
		{"ONLINE", "OFFLINE", func(partition string) {
			log.Println("ONLINE-->OFFLINE")
		}},
		{"OFFLINE", "ONLINE", func(partition string) {
			log.Println("OFFLINE-->ONLINE")
		}},
	})

	participant.RegisterStateModel(stateModel, sm)

	err = participant.Connect()
	must(err)
	log.Println("participant connected")

	go func() {
		err = gohelix.Rebalance(cluster, resource, "2")
		must(err)
	}()
	log.Println("rebalanced")

	log.Println("waiting Ctrl-C...")

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
