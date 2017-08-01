package main

import (
	"flag"
	"multi_connection_pool/server"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	serverAddress := flag.String("server", "localhost:8888", "")
	flag.Parse()
	helloServer := server.HelloServer{
		ServerAddress: *serverAddress,
	}
	helloServer.Init()
	helloServer.Serve()
}
