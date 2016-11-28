package gohelix

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yichen/go-zookeeper/zk"
	"github.com/yichen/retry"
)

var (
	zkRetryOptions = retry.RetryOptions{
		"zookeeper",           // tag
		time.Millisecond * 10, // backoff
		time.Second * 1,       // max backoff
		1,                     // default backoff constant
		0,                     // infinit retry
		false,                 // use V(1) level for log messages
	}
)

type connection struct {
	sync.RWMutex

	zkSvr       string
	chroot      string
	isConnected bool

	zkConn *zk.Conn
	stat   *zk.Stat
}

func newConnection(zkSvr string) *connection {
	conn := connection{
		zkSvr: zkSvr,
	}

	return &conn
}

// TODO chroot
func (conn *connection) Connect() error {
	zkServers := strings.Split(strings.TrimSpace(conn.zkSvr), ",")
	zkConn, _, err := zk.Connect(zkServers, 15*time.Second)
	if err != nil {
		return err
	}

	if err = conn.waitUntilConnected(); err != nil {
		return err
	}

	conn.isConnected = true
	conn.zkConn = zkConn

	return nil
}

func (conn *connection) waitUntilConnected() error {
	if _, _, err := zkConn.Exists("/zookeeper"); err != nil {
		return err
	}

	return nil
}

func (conn *connection) IsConnected() bool {
	if conn == nil || conn.isConnected == false {
		return false
	}

	if err := conn.waitUntilConnected(); err != nil {
		conn.isConnected = false
		return false
	}

	conn.isConnected = true
	return true
}

func (conn *connection) GetSessionID() string {
	return strconv.FormatInt(conn.zkConn.SessionID, 10)
}

func (conn *connection) Disconnect() {
	if conn.zkConn != nil {
		conn.zkConn.Close()
	}
	conn.isConnected = false
}

func (conn *connection) isClusterSetup(cluster string) bool {
	return true // TODO
}

func (conn *connection) CreateEmptyNode(path string) error {
	return conn.CreateRecordWithData(path, "")
}

func (conn *connection) CreateRecordWithData(path string, data string) error {
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	_, err := conn.Create(path, []byte(data), flags, acl)
	return err
}

func (conn *connection) CreateRecordWithPath(p string, r *Record) error {
	parent := path.Dir(p)
	conn.ensurePathExists(parent)

	data, err := r.Marshal()
	if err != nil {
		return err
	}

	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err = conn.Create(p, data, flags, acl)
	return err
}

func (conn *connection) Exists(path string) (bool, error) {
	var result bool
	var stat *zk.Stat

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		r, s, err := conn.zkConn.Exists(path)
		if err != nil {
			return retry.RetryContinue, nil
		}
		result = r
		stat = s
		return retry.RetryBreak, nil
	})

	conn.stat = stat
	return result, err
}

func (conn *connection) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, err := conn.Exists(path); err != nil || exists == false {
			println(path, "not exists")
			return exists, err
		}
	}

	return true, nil
}

func (conn *connection) Get(path string) ([]byte, error) {
	var data []byte

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, err := conn.zkConn.Get(path)
		if err != nil {
			return retry.RetryContinue, nil
		}
		data = d
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return data, err
}

func (conn *connection) GetW(path string) ([]byte, <-chan zk.Event, error) {
	var data []byte
	var events <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		d, s, evts, err := conn.zkConn.GetW(path)
		if err != nil {
			return retry.RetryContinue, nil
		}
		data = d
		conn.stat = s
		events = evts
		return retry.RetryBreak, nil
	})

	return data, events, err
}

func (conn *connection) Set(path string, data []byte) error {
	_, err := conn.zkConn.Set(path, data, conn.stat.Version)
	return err
}

func (conn *connection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	return conn.zkConn.Create(path, data, flags, acl)
}

func (conn *connection) Children(path string) ([]string, error) {
	var children []string

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, err := conn.zkConn.Children(path)
		if err != nil {
			return retry.RetryContinue, nil
		}
		children = c
		conn.stat = s
		return retry.RetryBreak, nil
	})

	return children, err
}

func (conn *connection) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
	var children []string
	var eventChan <-chan zk.Event

	err := retry.RetryWithBackoff(zkRetryOptions, func() (retry.RetryStatus, error) {
		c, s, evts, err := conn.zkConn.ChildrenW(path)
		if err != nil {
			return retry.RetryContinue, nil
		}
		children = c
		conn.stat = s
		eventChan = evts
		return retry.RetryBreak, nil
	})

	return children, eventChan, err
}

// update a map field for the znode. path is the znode path. key is the top-level key in
// the MapFields, mapProperty is the inner key, and value is the. For example:
//
// mapFields":{
// "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE":{
//   "CURRENT_STATE":"ONLINE"
//   ,"INFO":""
// }
// if we want to set the CURRENT_STATE to ONLINE, we call
// UpdateMapField("/RELAY/INSTANCES/{instance}/CURRENT_STATE/{sessionID}/{db}", "eat1-app993.stg.linkedin.com_11932,BizProfile,p31_1,SLAVE", "CURRENT_STATE", "ONLINE")
func (conn *connection) UpdateMapField(path string, key string, property string, value string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Record
	node, err := NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	// update the value
	node.SetMapField(key, property, value)

	// mashall to bytes
	data, err = node.Marshal()
	if err != nil {
		return err
	}

	// copy back to zookeeper
	err = conn.Set(path, data)
	return err
}

func (conn *connection) UpdateSimpleField(path string, key string, value string) {

	// get the current node
	data, err := conn.Get(path)
	must(err)

	// convert the result into Record
	node, err := NewRecordFromBytes(data)
	must(err)

	// update the value
	node.SetSimpleField(key, value)

	// mashall to bytes
	data, err = node.Marshal()
	must(err)

	// copy back to zookeeper
	err = conn.Set(path, data)
	must(err)
}

func (conn *connection) GetSimpleFieldValueByKey(path string, key string) string {
	data, err := conn.Get(path)
	must(err)

	node, err := NewRecordFromBytes(data)
	must(err)

	if node.SimpleFields == nil {
		return ""
	}

	v := node.GetSimpleField(key)
	if v == nil {
		return ""
	}
	return v.(string)
}

func (conn *connection) GetSimpleFieldBool(path string, key string) bool {
	result := conn.GetSimpleFieldValueByKey(path, key)
	return strings.ToUpper(result) == "TRUE"
}

func (conn *connection) Delete(path string) error {
	return conn.zkConn.Delete(path, -1)
}

func (conn *connection) DeleteTree(path string) error {
	if exists, err := conn.Exists(path); !exists || err != nil {
		return err
	}

	children, err := conn.Children(path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		err := conn.zkConn.Delete(path, -1)
		return err
	}

	for _, c := range children {
		p := path + "/" + c
		e := conn.DeleteTree(p)
		if e != nil {
			return e
		}
	}

	return conn.Delete(path)
}

func (conn *connection) RemoveMapFieldKey(path string, key string) error {
	data, err := conn.Get(path)
	if err != nil {
		return err
	}

	node, err := NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	node.RemoveMapField(key)

	data, err = node.Marshal()
	if err != nil {
		return err
	}

	// save the data back to zookeeper
	err = conn.Set(path, data)
	return err
}

func (conn *connection) IsClusterSetup(cluster string) (bool, error) {
	if conn.IsConnected() == false {
		if err := conn.Connect(); err != nil {
			return false, err
		}
	}

	keys := KeyBuilder{cluster}

	return conn.ExistsAll(
		keys.cluster(),
		keys.idealStates(),
		keys.participantConfigs(),
		keys.propertyStore(),
		keys.liveInstances(),
		keys.instances(),
		keys.externalView(),
		keys.stateModels(),
		keys.controller(),
		keys.controllerErrors(),
		keys.controllerHistory(),
		keys.controllerMessages(),
		keys.controllerStatusUpdates(),
	)
}

func (conn *connection) GetRecordFromPath(path string) (*Record, error) {
	data, err := conn.Get(path)
	if err != nil {
		return nil, err
	}
	return NewRecordFromBytes(data)
}

func (conn *connection) SetRecordForPath(path string, r *Record) error {
	if exists, _ := conn.Exists(path); !exists {
		conn.ensurePathExists(path)
	}

	data, err := r.Marshal()
	if err != nil {
		return err
	}

	// need to get the stat.version before calling set
	conn.Lock()

	if _, err := conn.Get(path); err != nil {
		conn.Unlock()
		return err
	}

	if err := conn.Set(path, data); err != nil {
		conn.Unlock()
		return err
	}

	conn.Unlock()
	return nil

}

// EnsurePath makes sure the specified path exists.
// If not, create it
func (conn *connection) ensurePathExists(p string) error {
	if exists, _ := conn.Exists(p); exists {
		return nil
	}

	parent := path.Dir(p)
	if exists, _ := conn.Exists(parent); !exists {
		conn.ensurePathExists(parent)
	}

	conn.CreateEmptyNode(p)
	return nil
}
