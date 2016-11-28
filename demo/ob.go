package main

import (
	"github.com/funkygao/gohelix"
	"log"
	"time"
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
	manager := gohelix.NewHelixManager(zkSvr)
	s := manager.NewSpectator(cluster)
	s.Connect()
	defer s.Disconnect()

	s.AddExternalViewChangeListener(func(externalViews []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%+v %+v", externalViews, context)
	})
	s.AddMessageListener(node1, func(instance string, messages []*gohelix.Record, context *gohelix.Context) {
		log.Println(instance)
	})
	s.AddLiveInstanceChangeListener(func(liveInstances []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%+v %+v", liveInstances, context)
	})
	s.AddControllerMessageListener(func(messages []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%+v %+v", messages, context)
	})
	s.AddCurrentStateChangeListener(node1, func(instance string, currentState []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%s %+v %+v", instance, currentState, context)
	})
	s.AddIdealStateChangeListener(func(idealState []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%+v %+v", idealState, context)
	})
	s.AddInstanceConfigChangeListener(func(configs []*gohelix.Record, context *gohelix.Context) {
		log.Printf("%+v %+v", configs, context)
	})

	time.Sleep(time.Minute)
}
