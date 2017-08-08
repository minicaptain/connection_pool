package server

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/samuel/go-zookeeper/zk"
	"multi_connection_pool/handler"
	"multi_connection_pool/thrift/gen-go/hello"
	"time"
)

type HelloServer struct {
	ServerAddress string
	server        *thrift.TSimpleServer
	handler       *handler.HelloHandler
	ZkServers     []string
	ZkNode        string
}

func (p *HelloServer) Init() (event <-chan zk.Event) {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	serverTransport, err := thrift.NewTServerSocket(p.ServerAddress)
	if err != nil {
		fmt.Errorf("init server error: %s", err)
		return
	}
	p.handler = &handler.HelloHandler{}
	processor := hello.NewRpcServiceProcessor(p.handler)
	p.server = thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	//create zk node
	f := func(conn *zk.Conn, node string, flag int32) error {
		_, err := conn.Create(node, nil, flag, zk.WorldACL(zk.PermAll))
		if err != nil {
			fmt.Printf("create parent node %s error :%s \n", node, err)
		}
		return err
	}
	go func() {
		for {
			conn, _, err := zk.Connect(p.ZkServers, time.Second*3)
			if err != nil {
				fmt.Errorf("connect to zk error")
				time.Sleep(time.Second * 3)
				conn.Close()
				continue
			}
			node := fmt.Sprintf("%s/%s", p.ZkNode, p.ServerAddress)
			if exist, _, _ := conn.Exists(p.ZkNode); !exist {
				err := f(conn, p.ZkNode, int32(0))
				if err != nil {
					fmt.Printf("create parent node %s error :%s \n", p.ZkNode, err)
					time.Sleep(time.Second * 3)
					conn.Close()
					continue
				}

			}
			if exist, _, _ := conn.Exists(node); !exist {
				err := f(conn, node, int32(zk.FlagEphemeral))
				if err != nil {
					fmt.Printf("create child node:%s error :%s \n", node, err)
					time.Sleep(time.Second * 3)
					conn.Close()
					continue
				}
			}
			_, _, event, _ = conn.ExistsW(node)
			return
		}
	}()
	return
}
func (p *HelloServer) Serve() (errChan chan error) {
	errChan <- p.server.Serve()
	return
}
