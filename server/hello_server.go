package server

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"multi_connection_pool/handler"
	"multi_connection_pool/thrift/gen-go/hello"
)

type HelloServer struct {
	ServerAddress string
	server        *thrift.TSimpleServer
	handler       *handler.HelloHandler
}

func (p *HelloServer) Init() {
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
}
func (p *HelloServer) Serve() error {
	return p.server.Serve()
}
