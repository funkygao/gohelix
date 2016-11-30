package gohelix

import "fmt"

// The Helix manager is a common component that connects each system component with the controller.
type HelixManager struct {
	zkSvr string
	conn  *connection
}

// NewHelixManager creates a new instance of HelixManager from a zookeeper connection string
func NewHelixManager(zkSvr string) *HelixManager {
	return &HelixManager{
		zkSvr: zkSvr,
	}
}

// NewSpectator creates a new Helix Spectator instance. This role handles most "read-only"
// operations of a Helix client.
func (m *HelixManager) NewSpectator(clusterID string) *Spectator {
	return &Spectator{
		ClusterID: clusterID,
		zkSvr:     m.zkSvr,
		kb:        keyBuilder{clusterID: clusterID},

		// listeners
		externalViewListeners:       []ExternalViewChangeListener{},
		liveInstanceChangeListeners: []LiveInstanceChangeListener{},
		currentStateChangeListeners: map[string][]CurrentStateChangeListener{},
		messageListeners:            map[string][]MessageListener{},
		idealStateChangeListeners:   []IdealStateChangeListener{},

		// control channels
		stop: make(chan bool),
		externalViewResourceMap: map[string]bool{},
		idealStateResourceMap:   map[string]bool{},
		instanceConfigMap:       map[string]bool{},

		changeNotificationChan: make(chan changeNotification, 1000),

		stopCurrentStateWatch: make(map[string]chan interface{}),

		// channel for receiving instance messages
		instanceMessageChannel: make(chan string, 100),
	}
}

// NewParticipant creates a new Helix Participant. This instance will act as a live instance
// of the Helix cluster when connected, and will participate the state model transition.
func (m *HelixManager) NewParticipant(clusterID string, host string, port string) *Participant {
	return &Participant{
		ClusterID:     clusterID,
		Host:          host,
		Port:          port,
		ParticipantID: fmt.Sprintf("%s_%s", host, port), // node id
		zkSvr:         m.zkSvr,
		started:       make(chan interface{}),
		stop:          make(chan bool),
		stopWatch:     make(chan bool),
		kb:            keyBuilder{clusterID: clusterID},
	}
}
