package main

import (
	"github.com/go-zoo/bone"
	"multi_connection_pool/handler"
	"net/http"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	helloHandler := handler.HelloClientHandler{}
	helloHandler.Init()
	mux := bone.New()
	mux.Get("/:name", http.HandlerFunc(helloHandler.Hello))
	s := &http.Server{
		Addr:              ":6666",
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 2,
		WriteTimeout:      time.Second * 2,
	}
	s.ListenAndServe()
}
