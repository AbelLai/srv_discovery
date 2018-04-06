package srv_discovery

import (
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	createdDifferentPathErrMsg string = "Created but return different path!"
)

type ZkRegistry struct {
	Conn       *zk.Conn
	node       ZkNode
	NodeParams ZkNodeParams
}

func NewZkRegistry(nodeParams ZkNodeParams) (zkr *ZkRegistry, err error) {
	if strings.Contains(nodeParams.SrvName, "-") {
		err = errors.New("Not allow the srv name contains '-' !!!")
		return
	}

	zkr = &ZkRegistry{
		Conn:       nil,
		NodeParams: nodeParams,
		node: ZkNode{
			RealRootPath: nodeParams.Root,
			RealSrvPath:  nodeParams.SrvName,
			IsLeader:     false,
			Data:         nodeParams.Data,
			User:         nodeParams.User,
		},
	}

	err = zkr.connect()
	return
}

// NOTE: Connect to Zookeeper server
func (z *ZkRegistry) connect() error {
	if !z.isConnected() {
		conn, connChan, err := zk.Connect(z.NodeParams.Servers, time.Second)
		if err != nil {
			return err
		}

		for {
			isConnected := false

			select {
			case connEvent := <-connChan:
				if connEvent.State == zk.StateConnected {
					isConnected = true
				}
			case _ = <-time.After(time.Second * 3):
				return errors.New("Connect to zookeeper server timeout!")
			}

			if isConnected {
				break
			}
		}

		err = conn.AddAuth("digest", []byte(z.node.User.UserName+":"+z.node.User.Password))
		if err != nil {
			return errors.New("AddAuth error: \n" + err.Error())
		}

		z.Conn = conn
	}

	return nil
}

func (z *ZkRegistry) isConnected() bool {
	if z.Conn == nil {
		return false
	} else if z.Conn.State() != zk.StateConnected {
		return false
	}
	return true
}

func (z *ZkRegistry) Run() (err error) {
	// 1. Register ephemeral sequential node
	err = z.register()

	// 2. Leader election
	if err == nil {
		err = z.elect()
	}

	if err == nil {
		if !z.node.IsLeader {
			// 3. Watch leader node
			err = z.watchLeader()

			// 4. Leader election again after previous leader dead
			if err == nil {
				err = z.elect()
			}
		}
	}

	return
}

// All nodes would be registered as ephemeral sequential node
func (z *ZkRegistry) register() error {
	err := z.connect()
	if err != nil {
		return err
	}

	// 1. Check root node existed or not. If not, create the root node
	isExist, _, err := z.Conn.Exists(z.NodeParams.Root)
	if err != nil {
		return errors.New("Check root path error: \n" + err.Error())
	}

	if !isExist {
		err = z.createNode(z.NodeParams.Root, nil, 0)
	}
	if err != nil {
		return errors.New("Create root path error: \n" + err.Error())
	}

	// 2. Use CreateProtectedEphemeralSequential method to create the node as a ephemeral sequential node.
	// Then the rule for the name like this: _c_ + guid + - + srvName + index
	// All about this, please check the code at here: https://github.com/samuel/go-zookeeper/blob/master/zk/conn.go#L1061
	srvFullPath := z.NodeParams.Root + "/" + z.NodeParams.SrvName
	realSrvFullPath, err := z.Conn.CreateProtectedEphemeralSequential(srvFullPath, z.NodeParams.Data, z.acl())

	if err != nil {
		return errors.New("Create zk node for srv failed: \n" + err.Error())
	}

	realSrvParts := strings.Split(realSrvFullPath, "/")
	z.node.RealSrvPath = realSrvParts[len(realSrvParts)-1]
	return nil
}

// Leader election
// The mininum index of the node would be the leader, and then the others would be the followers.
func (z *ZkRegistry) elect() error {
	fmt.Println("Starting to wait for 1 seconds")
	time.Sleep(time.Second)

	// 1. Get all ephemeral sequential nodes which are under the Root node
	childPaths, rootNodeStat, err := z.Conn.Children(z.node.RealRootPath)
	if err != nil {
		return err
	}
	if rootNodeStat.NumChildren == 0 {
		return errors.New("No any child path found!")
	}

	var (
		currentNodeIndex int
		srvNodes         map[int]string = make(map[int]string)
		nodeIndexArr     []int          = []int{}
	)

	for _, v := range childPaths {
		lastPart := strings.Split(v, "-")[1]
		indexStr := strings.Replace(lastPart, z.NodeParams.SrvName, "", -1)
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			break
		}
		nodeIndexArr = append(nodeIndexArr, index)
		srvNodes[index] = v

		if v == z.node.RealSrvPath {
			currentNodeIndex = index
		}
	}

	if err != nil {
		return errors.New("Error when electing: " + err.Error())
	}

	sort.Ints(nodeIndexArr)
	// 2. The mininum index of the node would be the leader, and then the others would be the followers.
	if nodeIndexArr[0] == currentNodeIndex {
		z.beALeader()
	} else {
		z.node.RealPrevSrvPath = srvNodes[currentNodeIndex-1]
	}

	return nil
}

// Follower will run this method and listen the first node which it's index is smaller than it is.
func (z *ZkRegistry) watchLeader() error {
	if z.node.IsLeader {
		return errors.New("No node watcher for Leader!!!")
	}

	leaderPath := z.node.RealRootPath + "/" + z.node.RealPrevSrvPath
	children, childrenState, eventChan, err := z.Conn.ChildrenW(leaderPath)
	if err != nil {
		return errors.New("Watch Leader node error: \n" + err.Error())
	}

	fmt.Println("Watch Leader Node, ", children, childrenState)
WATCH_LEADER_LOOP:
	for {
		select {
		case zkEvent := <-eventChan:
			if zkEvent.Type == zk.EventNodeDeleted {
				break WATCH_LEADER_LOOP
			}
		}
	}

	return nil
}

func (z *ZkRegistry) beALeader() {
	z.node.IsLeader = true
	z.node.RealPrevSrvPath = ""
	fmt.Println("I'm the leader now!")
}

func (z *ZkRegistry) createNode(path string, data []byte, flags int32) error {
	createdPath, err := z.Conn.Create(path, data, flags, z.acl())

	if err == nil && createdPath != path {
		err = errors.New(createdDifferentPathErrMsg)
	}

	return err
}

func (z *ZkRegistry) acl() []zk.ACL {
	return zk.DigestACL(zk.PermAll, z.node.User.UserName, z.node.User.Password)
}
