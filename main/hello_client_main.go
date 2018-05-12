package main

import (
	"connection_pool/handler"
	"github.com/go-zoo/bone"
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
