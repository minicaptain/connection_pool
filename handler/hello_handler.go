package handler

import "fmt"

type HelloHandler struct {
}

func (p *HelloHandler) HelloWorld(name string) error {
	fmt.Println("hello world by ", name)
	return nil
}
