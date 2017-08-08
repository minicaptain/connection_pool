package main

import (
	"flag"
	"multi_connection_pool/server"
	"runtime"
	"strings"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	serverAddress := flag.String("server", "localhost:8888", "")
	zkServers := flag.String("zk", "127.0.0.1:2181", "")
	zkNode := flag.String("node", "/hello_server", "")
	flag.Parse()
	helloServer := server.HelloServer{
		ServerAddress: *serverAddress,
		ZkServers:     strings.Split(*zkServers, ","),
		ZkNode:        *zkNode,
	}
	select {
	case <-helloServer.Init():
		return
	case <-helloServer.Serve():
		return
	}
}
