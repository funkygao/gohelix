package gohelix

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// Admin handles the administration task for the Helix cluster. Many of the operations
// are mirroring the implementions documented at
// http://helix.apache.org/0.7.0-incubating-docs/Quickstart.html
type Admin struct {
	zkSvr string

	conn      *connection
	connected bool
}

func NewZKHelixAdmin(zkSvr string) *Admin {
	return &Admin{zkSvr: zkSvr}
}

func (adm *Admin) Connect() error {
	adm.conn = newConnection(adm.zkSvr)
	if err := adm.conn.Connect(); err != nil {
		return err
	}

	adm.connected = true
	return nil
}

func (adm *Admin) Disconnect() {
	if adm.connected {
		adm.conn.Disconnect()
		adm.connected = false
	}
}

// AddCluster add a cluster to Helix. As a result, a znode will be created in zookeeper
// root named after the cluster name, and corresponding data structures are populated
// under this znode.
func (adm Admin) AddCluster(cluster string) error {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return err
	}
	defer conn.Disconnect()

	kb := keyBuilder{ClusterID: cluster}

	// avoid dup cluster
	exists, err := conn.Exists(kb.cluster())
	if err != nil {
		return err
	}
	if exists {
		return ErrNodeAlreadyExists
	}

	conn.CreateEmptyNode(kb.cluster())
	conn.CreateEmptyNode(kb.propertyStore())
	conn.CreateEmptyNode(kb.instances())
	conn.CreateEmptyNode(kb.idealStates())
	conn.CreateEmptyNode(kb.externalView())
	conn.CreateEmptyNode(kb.liveInstances())

	conn.CreateEmptyNode(kb.stateModels())
	conn.CreateRecordWithData(kb.stateModel(StateModelLeaderStandby), HelixDefaultNodes[StateModelLeaderStandby])
	conn.CreateRecordWithData(kb.stateModel(StateModelMasterSlave), HelixDefaultNodes[StateModelMasterSlave])
	conn.CreateRecordWithData(kb.stateModel(StateModelOnlineOffline), HelixDefaultNodes[StateModelOnlineOffline])
	conn.CreateRecordWithData(kb.stateModel("STORAGE_DEFAULT_SM_SCHEMATA"), HelixDefaultNodes["STORAGE_DEFAULT_SM_SCHEMATA"])
	conn.CreateRecordWithData(kb.stateModel(StateModelSchedulerTaskQueue), HelixDefaultNodes[StateModelSchedulerTaskQueue])
	conn.CreateRecordWithData(kb.stateModel(StateModelTask), HelixDefaultNodes[StateModelTask])

	conn.CreateEmptyNode(kb.configs())
	conn.CreateEmptyNode(kb.participantConfigs())
	conn.CreateEmptyNode(kb.resourceConfigs())
	conn.CreateEmptyNode(kb.clusterConfigs())

	clusterNode := NewRecord(cluster)
	conn.CreateRecordWithPath(kb.clusterConfig(), clusterNode)

	conn.CreateEmptyNode(kb.controller())
	conn.CreateEmptyNode(kb.controllerErrors())
	conn.CreateEmptyNode(kb.controllerHistory())
	conn.CreateEmptyNode(kb.controllerMessages())
	conn.CreateEmptyNode(kb.controllerStatusUpdates())

	return nil
}

// DropCluster removes a helix cluster from zookeeper. This will remove the
// znode named after the cluster name from the zookeeper root.
func (adm Admin) DropCluster(cluster string) error {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return err
	}
	defer conn.Disconnect()

	kb := keyBuilder{ClusterID: cluster}
	return conn.DeleteTree(kb.cluster())
}

func (adm Admin) AllowParticipantAutoJoin(cluster string, yes bool) error {
	var properties = map[string]string{
		"allowParticipantAutoJoin": "false",
	}
	if yes {
		properties["allowParticipantAutoJoin"] = "true"
	}
	return adm.SetConfig(cluster, "CLUSTER", properties)
}

// ListClusterInfo shows the existing resources and instances in the glaster
func (adm Admin) ListClusterInfo(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}
	isPath := keys.idealStates()
	instancesPath := keys.instances()

	resources, err := conn.Children(isPath)
	if err != nil {
		return "", err
	}

	instances, err := conn.Children(instancesPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	buffer.WriteString("\nInstances in cluster " + cluster + ":\n")
	for _, i := range instances {
		buffer.WriteString("  " + i + "\n")
	}
	return buffer.String(), nil
}

// ListClusters shows all Helix managed clusters in the connected zookeeper cluster
func (adm Admin) ListClusters() ([]string, error) {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return nil, err
	}
	defer conn.Disconnect()

	children, err := conn.Children("/")
	if err != nil {
		return nil, err
	}

	var clusters []string
	for _, cluster := range children {
		if ok, err := conn.IsClusterSetup(cluster); ok && err == nil {
			clusters = append(clusters, cluster)
		}
	}

	return clusters, nil
}

// SetConfig set the configuration values for the cluster, defined by the config scope
func (adm Admin) SetConfig(cluster string, scope string, properties map[string]string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	switch strings.ToUpper(scope) {
	case "CLUSTER":
		if allow, ok := properties["allowParticipantAutoJoin"]; ok {
			keys := keyBuilder{cluster}
			path := keys.clusterConfig()

			if strings.ToLower(allow) == "true" {
				conn.UpdateSimpleField(path, "allowParticipantAutoJoin", "true")
			}
		}
	case "CONSTRAINT":
	case "PARTICIPANT":
	case "PARTITION":
	case "RESOURCE":
	}

	return nil
}

// GetConfig obtains the configuration value of a property, defined by a config scope
func (adm Admin) GetConfig(cluster string, scope string, keys []string) map[string]interface{} {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return nil
	}
	defer conn.Disconnect()

	result := make(map[string]interface{})

	switch scope {
	case "CLUSTER":
		kb := keyBuilder{cluster}
		path := kb.clusterConfig()

		for _, k := range keys {
			result[k] = conn.GetSimpleFieldValueByKey(path, k)
		}
	case "CONSTRAINT":
	case "PARTICIPANT":
	case "PARTITION":
	case "RESOURCE":
	}

	return result
}

func (adm Admin) AddInstance(cluster string, config InstanceConfig) error {
	return adm.AddNode(cluster, config.Node())
}

// AddNode is the internal implementation corresponding to command
// ./helix-admin.sh --zkSvr <ZookeeperServerAddress> --addNode <clusterName instanceId>
// node is in the form of host_port
func (adm Admin) AddNode(cluster string, node string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	if ok, err := conn.IsClusterSetup(cluster); ok == false || err != nil {
		return ErrClusterNotSetup
	}

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<NODE>
	kb := keyBuilder{ClusterID: cluster}
	path := kb.participantConfig(node)
	exists, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return ErrNodeAlreadyExists
	}

	// create new node for the participant
	parts := strings.Split(node, "_")
	n := NewRecord(node)
	n.SetSimpleField("HELIX_HOST", parts[0])
	n.SetSimpleField("HELIX_PORT", parts[1])

	conn.CreateRecordWithPath(path, n)
	conn.CreateEmptyNode(kb.instance(node))
	conn.CreateEmptyNode(kb.messages(node))
	conn.CreateEmptyNode(kb.currentStates(node))
	conn.CreateEmptyNode(kb.errorsR(node))
	conn.CreateEmptyNode(kb.statusUpdates(node))

	return nil
}

// DropNode removes a node from a cluster. The corresponding znodes
// in zookeeper will be removed.
func (adm Admin) DropNode(cluster string, node string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<node>
	keys := keyBuilder{cluster}
	if exists, err := conn.Exists(keys.participantConfig(node)); !exists || err != nil {
		return ErrNodeNotExist
	}

	// check if node exist under instance: /<cluster>/INSTANCES/<node>
	if exists, err := conn.Exists(keys.instance(node)); !exists || err != nil {
		return ErrInstanceNotExist
	}

	// delete /<cluster>/CONFIGS/PARTICIPANT/<node>
	conn.DeleteTree(keys.participantConfig(node))

	// delete /<cluster>/INSTANCES/<node>
	conn.DeleteTree(keys.instance(node))

	return nil
}

// AddResource implements the helix-admin.sh --addResource
// # helix-admin.sh --zkSvr <zk_address> --addResource <clustername> <resourceName> <numPartitions> <StateModelName>
// ./helix-admin.sh --zkSvr localhost:2199 --addResource MYCLUSTER myDB 6 MasterSlave
func (adm Admin) AddResource(cluster string, resource string, partitions int, stateModel string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	keys := keyBuilder{ClusterID: cluster}

	// make sure the state model def exists
	if exists, err := conn.Exists(keys.stateModel(stateModel)); !exists || err != nil {
		return ErrStateModelDefNotExist
	}

	// make sure the path for the ideal state does not exit
	isPath := keys.idealStates() + "/" + resource
	if exists, err := conn.Exists(isPath); exists || err != nil {
		if exists {
			return ErrResourceExists
		}
		return err
	}

	// create the idealstate for the resource
	// is := NewIdealState(resource)
	// is.SetNumPartitions(partitions)
	// is.SetReplicas(0)
	// is.SetRebalanceMode("SEMI_AUTO")
	// is.SetStateModelDefRef(stateModel)
	// // save the ideal state in zookeeper
	// is.Save(conn, cluster)

	is := NewRecord(resource)
	is.SetSimpleField("NUM_PARTITIONS", strconv.Itoa(partitions))
	is.SetSimpleField("REPLICAS", strconv.Itoa(0))                    // TODO
	is.SetSimpleField("REBALANCE_MODE", strings.ToUpper("SEMI_AUTO")) // TODO
	is.SetSimpleField("STATE_MODEL_DEF_REF", stateModel)
	conn.CreateRecordWithPath(isPath, is)

	return nil
}

// DropResource removes the specified resource from the cluster.
func (adm Admin) DropResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}

	// make sure the path for the ideal state does not exit
	conn.DeleteTree(keys.idealStates() + "/" + resource)
	conn.DeleteTree(keys.resourceConfig(resource))

	return nil
}

// EnableResource enables the specified resource in the cluster
func (adm Admin) EnableResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}

	isPath := keys.idealStates() + "/" + resource

	if exists, err := conn.Exists(isPath); !exists || err != nil {
		if !exists {
			return ErrResourceNotExists
		}
		return err
	}

	// TODO: set the value at leaf node instead of the record level
	conn.UpdateSimpleField(isPath, "HELIX_ENABLED", "true")
	return nil
}

// DisableResource disables the specified resource in the cluster.
func (adm Admin) DisableResource(cluster string, resource string) error {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}

	isPath := keys.idealStates() + "/" + resource

	if exists, err := conn.Exists(isPath); !exists || err != nil {
		if !exists {
			return ErrResourceNotExists
		}

		return err
	}

	conn.UpdateSimpleField(isPath, "HELIX_ENABLED", "false")

	return nil
}

// Rebalance not implemented yet TODO
func (adm Admin) Rebalance(cluster string, resource string, replicationFactor int) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		fmt.Println("Failed to connect to zookeeper.")
		return
	}
	defer conn.Disconnect()

	fmt.Println("Not implemented")
}

// ListResources shows a list of resources managed by the helix cluster
func (adm Admin) ListResources(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}
	isPath := keys.idealStates()
	resources, err := conn.Children(isPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstances shows a list of instances participating the cluster.
func (adm Admin) ListInstances(cluster string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}
	isPath := keys.instances()
	instances, err := conn.Children(isPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Existing instances in cluster %s:\n", cluster))

	for _, r := range instances {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstanceInfo shows detailed information of an inspace in the helix cluster
func (adm Admin) ListInstanceInfo(cluster string, instance string) (string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return "", err
	}
	defer conn.Disconnect()

	// make sure the cluster is already setup
	if ok, err := conn.IsClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	keys := keyBuilder{cluster}
	instanceCfg := keys.participantConfig(instance)

	if exists, err := conn.Exists(instanceCfg); !exists || err != nil {
		if !exists {
			return "", ErrNodeNotExist
		}
		return "", err
	}

	r, err := conn.GetRecordFromPath(instanceCfg)
	if err != nil {
		return "", err
	}
	return r.String(), nil
}

// GetInstances returns lists of instances
func (adm Admin) GetInstances(cluster string) ([]string, error) {
	conn := newConnection(adm.zkSvr)
	err := conn.Connect()
	if err != nil {
		return nil, err
	}
	defer conn.Disconnect()

	kb := keyBuilder{cluster}
	return conn.Children(kb.instances())
}

// DropInstance removes a participating instance from the helix cluster
func (adm Admin) DropInstance(zkSvr string, cluster string, instance string) error {
	conn := newConnection(adm.zkSvr)
	if err := conn.Connect(); err != nil {
		return err
	}
	defer conn.Disconnect()

	kb := keyBuilder{cluster}
	instanceKey := kb.instance(instance)
	return conn.Delete(instanceKey)
}
