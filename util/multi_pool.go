package util

import (
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
	"time"
)

/*
 *This package finished a multi connection pool for multi servers cluster, load balance and keep alive
 *If you set the SingleMaxIdleCount to 0, which would be short link
 */

type Factory interface {
	NewConnection() (Conn, error)
}

type MultiPool struct {
	ConnPool             map[string]*ConnPool
	ServerPool           *ServerPool
	Servers              []string
	ZkSessionTimeout     time.Duration
	ZkServers            []string
	ZkNode               string
	SingleMaxIdleCount   int
	SingleMaxActiveCount int
	IdleTimeOut          time.Duration
	SingleFactory        func(server string, timeout time.Duration, tryCount int) Factory
	ConnTimeout          time.Duration
	ConnTryCount         int
	closed               bool
	mu                   sync.Mutex
}

func (p *MultiPool) Get() (Conn, string, error) {
	if !p.closed {
		server, err := p.ServerPool.GetServer()
		serverName := server
		if err != nil || server == "" {
			return nil, serverName, errors.New("No Avalaible Server")
		}
		if v, ok := p.ConnPool[server]; ok {
			conn, err := v.Get()
			if err != nil {
				p.ServerPool.ServerDown(server)
			}
			return conn, serverName, err
		} else {
			p.ServerPool.ServerDown(server)
			return nil, serverName, errors.New("Server does not exist in pool")
		}
	} else {
		return nil, "", errors.New("multi pool is closed")
	}
}
func (p *MultiPool) Put(conn Conn, serverName string, forceClose bool) {
	if v, ok := p.ConnPool[serverName]; ok {
		v.Put(conn, forceClose)
	} else {
		logrus.Infof("[MultiPool.Put] no server pool for %s", serverName)
	}
}

func (p *MultiPool) InitFromZkChildrenNode() {
	if len(p.ZkServers) == 0 {
		logrus.Panic("[MultiPool.InitFromZk] should set the zk server address")
	}
	conn, _, err := zk.Connect(p.ZkServers, p.ZkSessionTimeout)
	defer conn.Close()
	if err != nil {
		logrus.Panicf("[MultiPool.InitFromZk] connect to zk error: %s", err)
	}
	p.ConnPool = map[string]*ConnPool{}
	f := func(conn *zk.Conn) {
		servers, _, err := conn.Children(p.ZkNode)
		if err != nil || len(servers) == 0 {
			panic("no child")
		}
		p.Servers = servers
		fmt.Printf("[MultiPool.InitFromZk] children node list:%+v", servers)
		serverPool := NewServerPool(servers)
		connPool := map[string]*ConnPool{}
		for _, server := range servers {
			factory := p.SingleFactory(server, p.ConnTimeout, p.ConnTryCount)
			pool := &ConnPool{
				MaxIdle:   p.SingleMaxIdleCount,
				MaxActive: p.SingleMaxActiveCount,
				Dial: func() (Conn, error) {
					return factory.NewConnection()
				},
				TestOnBorrow: func(conn Conn) error {
					return conn.Health()
				},
				IdleTimeout: p.IdleTimeOut,
			}
			connPool[server] = pool
		}
		p.mu.Lock()
		p.ServerPool = serverPool
		p.ConnPool = connPool
		p.mu.Unlock()
	}
	f(conn)
	go func() {
		for {
			func() {
				conn, _, err := zk.Connect(p.ZkServers, p.ZkSessionTimeout)
				defer conn.Close()
				if err != nil {
					time.Sleep(time.Second * 3)
					return
				}
				_, _, watchEvent, err := conn.ChildrenW(p.ZkNode)
				if err != nil {
					time.Sleep(time.Second * 3)
					return
				}
				f(conn)
				<-watchEvent
				fmt.Printf("[InitFromZkChildrenNode.watch] child node changed zkNode: %s", p.ZkNode)
			}()
		}
	}()
}

func (p *MultiPool) InitFromServerList() {
	if len(p.Servers) == 0 {
		logrus.Panic("servers should not be nil")
		return
	}
	serverPool := NewServerPool(p.Servers)
	connPool := map[string]*ConnPool{}
	for _, server := range p.Servers {
		factory := p.SingleFactory(server, p.ConnTimeout, p.ConnTryCount)
		pool := &ConnPool{
			MaxIdle:   p.SingleMaxIdleCount,
			MaxActive: p.SingleMaxActiveCount,
			Dial: func() (Conn, error) {
				return factory.NewConnection()
			},
			TestOnBorrow: func(conn Conn) error {
				return conn.Health()
			},
			IdleTimeout: p.IdleTimeOut,
		}
		connPool[server] = pool
	}
	p.mu.Lock()
	p.ConnPool = connPool
	p.ServerPool = serverPool
	p.mu.Unlock()
}

//TODO: close the multi server pool
func (p *MultiPool) Close() {
	if len(p.Servers) == 0 {
		return
	}
	for _, server := range p.Servers {
		go func(server string) {
			p.ConnPool[server].Close()
		}(server)
	}
	p.closed = true
}
