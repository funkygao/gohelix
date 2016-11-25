package main

import (
	"github.com/funkygao/gohelix"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	manager := gohelix.NewHelixManager("localhost:2199")
	participant := manager.NewParticipant("this", "localhost", "10009")

	// creaet OnlineOffline state model
	sm := gohelix.NewStateModel([]gohelix.Transition{
		{"ONLINE", "OFFLINE", func(partition string) {
			log.Println("ONLINE-->OFFLINE")
		}},
		{"OFFLINE", "ONLINE", func(partition string) {
			log.Println("OFFLINE-->ONLINE")
		}},
	})

	participant.RegisterStateModel(gohelix.StateModelOnlineOffline, sm)

	err := participant.Connect()
	if err != nil {
		log.Println(err.Error())
		return
	}

	// block until SIGINT and SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
