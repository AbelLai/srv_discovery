package srv_discovery

type User struct {
	UserName string
	Password string
}

type ZkNodeParams struct {
	Root    string
	SrvName string
	Data    []byte
	User    User
	Servers []string
}

type ZkNode struct {
	RealRootPath    string
	RealSrvPath     string
	RealPrevSrvPath string
	IsLeader        bool
	Data            []byte
	User            User
}
