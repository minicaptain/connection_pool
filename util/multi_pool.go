package util

import (
	"errors"
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
	mu                   sync.Mutex
}

func (p *MultiPool) Get() (Conn, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
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
}
func (p *MultiPool) Put(conn Conn, serverName string, forceClose bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	if err != nil {
		logrus.Panicf("[MultiPool.InitFromZk] connect to zk error: %s", err)
	}
	defer conn.Close()
	p.ConnPool = map[string]*ConnPool{}
	f := func(conn *zk.Conn) {
		p.mu.Lock()
		defer p.mu.Unlock()
		servers, _, _ := conn.Children(p.ZkNode)
		p.Servers = servers
		p.ServerPool = NewServerPool(servers)
		for _, server := range servers {
			factory := p.SingleFactory(server, time.Second*1, 1)
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
			p.ConnPool[server] = pool
		}
	}
	f(conn)
	go func() {
		for {
			func() {
				c, _, _ := zk.Connect(p.ZkServers, p.ZkSessionTimeout)
				defer c.Close()
				_, _, watchEvent, _ := c.ChildrenW(p.ZkNode)
				select {
				case event := <-watchEvent:
					if event.Type == zk.EventNodeChildrenChanged {
						f(c)
					}
				}
			}()
		}
	}()
}

func (p *MultiPool) InitFromServerList() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Servers) == 0 {
		logrus.Panic("servers should not be nil")
		return
	}
	p.ServerPool = NewServerPool(p.Servers)
	p.ConnPool = map[string]*ConnPool{}
	for _, server := range p.Servers {
		factory := p.SingleFactory(server, time.Second*1, 1)
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
		p.ConnPool[server] = pool
	}
}

//TODO: close the multi server pool
func (p *MultiPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Servers) == 0 {
		return
	}
	for _, server := range p.Servers {
		go func(server string) {
			p.ConnPool[server].Close()
		}(server)
	}
	p.Servers = nil
	p.ConnPool = nil
	p.ServerPool = nil
}
