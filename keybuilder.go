package gohelix

import (
	"fmt"
)

// keyBuilder geenrate a Zookeeper path
type keyBuilder struct {
	ClusterID string
}

func (k *keyBuilder) cluster() string {
	return fmt.Sprintf("/%s", k.ClusterID)
}

func (k *keyBuilder) configs() string {
	return fmt.Sprintf("/%s/CONFIGS", k.ClusterID)
}

func (k *keyBuilder) clusterConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/CLUSTER", k.ClusterID)
}

func (k *keyBuilder) clusterConfig() string {
	return fmt.Sprintf("/%s/CONFIGS/CLUSTER/%s", k.ClusterID, k.ClusterID)
}

func (k *keyBuilder) externalView() string {
	return fmt.Sprintf("/%s/EXTERNALVIEW", k.ClusterID)
}

func (k *keyBuilder) externalViewForResource(resource string) string {
	return fmt.Sprintf("/%s/EXTERNALVIEW/%s", k.ClusterID, resource)
}

func (k *keyBuilder) propertyStore() string {
	return fmt.Sprintf("/%s/PROPERTYSTORE", k.ClusterID)
}

func (k *keyBuilder) controller() string {
	return fmt.Sprintf("/%s/CONTROLLER", k.ClusterID)
}

func (k *keyBuilder) controllerErrors() string {
	return fmt.Sprintf("/%s/CONTROLLER/ERRORS", k.ClusterID)
}

func (k *keyBuilder) controllerHistory() string {
	return fmt.Sprintf("/%s/CONTROLLER/HISTORY", k.ClusterID)
}

func (k *keyBuilder) controllerMessages() string {
	return fmt.Sprintf("/%s/CONTROLLER/MESSAGES", k.ClusterID)
}

func (k *keyBuilder) controllerMessage(ID string) string {
	return fmt.Sprintf("/%s/CONTROLLER/MESSAGES/%s", k.ClusterID, ID)
}

func (k *keyBuilder) controllerStatusUpdates() string {
	return fmt.Sprintf("/%s/CONTROLLER/STATUSUPDATES", k.ClusterID)
}

func (k *keyBuilder) idealStates() string {
	return fmt.Sprintf("/%s/IDEALSTATES", k.ClusterID)
}

func (k *keyBuilder) idealStateForResource(resource string) string {
	return fmt.Sprintf("/%s/IDEALSTATES/%s", k.ClusterID, resource)
}

func (k *keyBuilder) resourceConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE", k.ClusterID)
}

func (k *keyBuilder) resourceConfig(resource string) string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE/%s", k.ClusterID, resource)
}

func (k *keyBuilder) participantConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT", k.ClusterID)
}

func (k *keyBuilder) participantConfig(participantID string) string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT/%s", k.ClusterID, participantID)
}

func (k *keyBuilder) liveInstances() string {
	return fmt.Sprintf("/%s/LIVEINSTANCES", k.ClusterID)
}

func (k *keyBuilder) instances() string {
	return fmt.Sprintf("/%s/INSTANCES", k.ClusterID)
}

func (k *keyBuilder) instance(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s", k.ClusterID, participantID)
}

func (k *keyBuilder) liveInstance(partipantID string) string {
	return fmt.Sprintf("/%s/LIVEINSTANCES/%s", k.ClusterID, partipantID)
}

func (k *keyBuilder) currentStates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES", k.ClusterID, participantID)
}

func (k *keyBuilder) currentStatesForSession(participantID string, sessionID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES/%s", k.ClusterID, participantID, sessionID)
}

func (k *keyBuilder) currentStateForResource(participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES/%s/%s", k.ClusterID, participantID, sessionID, resourceID)
}

func (k *keyBuilder) errorsR(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/ERRORS", k.ClusterID, participantID)
}

func (k *keyBuilder) errors(participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/ERRORS/%s/%s", k.ClusterID, participantID, sessionID, resourceID)
}

func (k *keyBuilder) healthReport(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/HEALTHREPORT", k.ClusterID, participantID)
}

func (k *keyBuilder) statusUpdates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/STATUSUPDATES", k.ClusterID, participantID)
}

func (k *keyBuilder) stateModels() string {
	return fmt.Sprintf("/%s/STATEMODELDEFS", k.ClusterID)
}

func (k *keyBuilder) stateModel(resourceID string) string {
	return fmt.Sprintf("/%s/STATEMODELDEFS/%s", k.ClusterID, resourceID)
}

func (k *keyBuilder) messages(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES", k.ClusterID, participantID)
}

func (k *keyBuilder) message(participantID string, messageID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES/%s", k.ClusterID, participantID, messageID)
}
