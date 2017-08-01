package client

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	log "github.com/Sirupsen/logrus"
	"io"
	"multi_connection_pool/thrift/gen-go/hello"
	"multi_connection_pool/util"
	"net"
	"time"
)

type HelloClientConnection struct {
	tryCount      int
	sock          *thrift.TSocket
	trans         thrift.TTransport
	proto         thrift.TBinaryProtocol
	client        *hello.RpcServiceClient
	ServerAddress string
}

type HelloClientConnectionFactory struct {
	ServerAddress string
	timeout       time.Duration
	tryCount      int
}

func NewHelloClientConnectionFactory(server string, timeout time.Duration, tryCount int) util.Factory {
	return &HelloClientConnectionFactory{
		ServerAddress: server,
		timeout:       timeout,
		tryCount:      tryCount,
	}
}

func (p *HelloClientConnectionFactory) NewConnection() (util.Conn, error) {
	pClient := HelloClientConnection{
		tryCount:      p.tryCount,
		ServerAddress: p.ServerAddress,
	}
	var err error
	pClient.sock, _ = thrift.NewTSocket(p.ServerAddress)
	pClient.sock.SetTimeout(p.timeout)
	pClient.trans = thrift.NewTTransportFactory().GetTransport(pClient.sock)
	pClient.client = hello.NewRpcServiceClientFactory(pClient.trans, thrift.NewTBinaryProtocolFactoryDefault())
	if err = pClient.sock.Open(); err != nil {
		pClient.sock.Close()
	}
	return pClient, err
}

func (p HelloClientConnection) HelloWorld(name string) error {
	for i := 0; i < p.tryCount; i++ {
		if !p.trans.IsOpen() {
			p.trans.Open()
		}
		return p.client.HelloWorld(name)
	}
	return nil
}

func (p HelloClientConnection) Close() error {
	return p.trans.Close()
}

func (p HelloClientConnection) Health() error {
	if !p.sock.IsOpen() {
		if err := p.sock.Open(); err != nil {
			return err
		}
	}
	p.sock.Conn().SetReadDeadline(time.Now().Add(time.Microsecond * 50))
	if _, err := p.sock.Conn().Read(make([]byte, 1)); err != nil {
		if err == io.EOF {
			log.Errorln("EOF in conn check:", err)
			return err
		}

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil
		}

		log.Errorln("Error in conn check:", err)
		return nil
	}

	log.Errorln("in conn check, should't be here")
	return nil
}
