package handler

import (
	"fmt"
	"github.com/go-zoo/bone"
	"multi_connection_pool/client"
	"multi_connection_pool/util"
	"net/http"
	"time"
)

type HelloClientHandler struct {
	connPool *util.MultiPool
}

func (p *HelloClientHandler) Init() {
	p.connPool = &util.MultiPool{
		SingleFactory:        client.NewHelloClientConnectionFactory,
		SingleMaxIdleCount:   2,
		SingleMaxActiveCount: 10,
		Servers:              []string{"localhost:8887", "localhost:8888"},
		IdleTimeOut:          time.Second * 30,
	}
	p.connPool.InitFromServerList()
	fmt.Println(p.connPool)
}

func (p *HelloClientHandler) Hello(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	conn, server, err := p.connPool.Get()
	fmt.Println("get server is ", server)
	defer p.connPool.Put(conn, server, false)
	if err != nil {
		fmt.Errorf("get conn error %s", err)
	}
	name := bone.GetValue(r, "name")
	helloConn := conn.(client.HelloClientConnection)
	helloConn.HelloWorld(name)
	w.Write([]byte("OK\n"))
}
