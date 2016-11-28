package gohelix

import "fmt"

type changeNotificationType uint8
type changeNotification struct {
	changeType changeNotificationType
	changeData interface{}
}

const (
	exteralViewChanged        changeNotificationType = 0
	liveInstanceChanged       changeNotificationType = 1
	idealStateChanged         changeNotificationType = 2
	currentStateChanged       changeNotificationType = 3
	instanceConfigChanged     changeNotificationType = 4
	controllerMessagesChanged changeNotificationType = 5
	instanceMessagesChanged   changeNotificationType = 6
)

type (
	// ExternalViewChangeListener is triggered when the external view is updated
	ExternalViewChangeListener func(externalViews []*Record, context *Context)

	// LiveInstanceChangeListener is triggered when live instances of the cluster are updated
	LiveInstanceChangeListener func(liveInstances []*Record, context *Context)

	// CurrentStateChangeListener is triggered when the current state of a participant changed
	CurrentStateChangeListener func(instance string, currentState []*Record, context *Context)

	// IdealStateChangeListener is triggered when the ideal state changed
	IdealStateChangeListener func(idealState []*Record, context *Context)

	// InstanceConfigChangeListener is triggered when the instance configs are updated
	InstanceConfigChangeListener func(configs []*Record, context *Context)

	// ControllerMessageListener is triggered when the controller messages are updated
	ControllerMessageListener func(messages []*Record, context *Context)

	// MessageListener is triggered when the instance received new messages
	MessageListener func(instance string, messages []*Record, context *Context)
)

// The Helix manager is a common component that connects each system component with the controller.
type HelixManager struct {
	zkAddress string
	conn      *connection
}

// NewHelixManager creates a new instance of HelixManager from a zookeeper connection string
func NewHelixManager(zkAddress string) *HelixManager {
	return &HelixManager{
		zkAddress: zkAddress,
	}
}

// NewSpectator creates a new Helix Spectator instance. This role handles most "read-only"
// operations of a Helix client.
func (m *HelixManager) NewSpectator(clusterID string) *Spectator {
	return &Spectator{
		ClusterID: clusterID,
		zkConnStr: m.zkAddress,
		keys:      KeyBuilder{clusterID},

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
		ParticipantID: fmt.Sprintf("%s_%s", host, port),
		zkConnStr:     m.zkAddress,
		started:       make(chan interface{}),
		stop:          make(chan bool),
		stopWatch:     make(chan bool),
		keys:          KeyBuilder{ClusterID: clusterID},
	}
}
