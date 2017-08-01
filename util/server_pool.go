package util

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

type ServerPool struct {

	// mu protects candidate server list
	mu sync.Mutex

	// candidate server stores all candidate servers
	CandidateServer []string

	//candidate server status
	CandidateStatus map[string]bool

	//candidate server error count
	CandidateErrorCount map[string]int

	// time interval, must supplied
	TimeInterval time.Duration
}

func NewServerPool(servers []string) *ServerPool {
	pool := &ServerPool{
		CandidateServer:     []string{},
		CandidateStatus:     map[string]bool{},
		CandidateErrorCount: map[string]int{},
	}

	for _, server := range servers {
		pool.CandidateServer = append(pool.CandidateServer, server)
		pool.CandidateStatus[server] = true
		pool.CandidateErrorCount[server] = 0
	}

	return pool
}

// GetServer returns header of servers list
func (p *ServerPool) GetServer() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	arrLen := len(p.CandidateServer)
	if arrLen > 0 {
		return p.CandidateServer[rand.Intn(arrLen)], nil
	}
	return "", errors.New("ServerPool: get server error")
}

// ServerDown remove server from candidate list, and put back to end of server list
func (p *ServerPool) ServerDown(server string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.CandidateErrorCount[server] += 1
	if p.CandidateErrorCount[server] > 10 {
		p.CandidateStatus[server] = false
		normal := []string{}
		for k, v := range p.CandidateStatus {
			if v {
				normal = append(normal, k)
			}
		}
		p.CandidateServer = normal
		p.CandidateErrorCount[server] = 0
		go func() {
			timer := time.NewTimer(time.Second * 30)
			<-timer.C
			p.mu.Lock()
			defer p.mu.Unlock()
			p.CandidateStatus[server] = true
			p.CandidateServer = append(p.CandidateServer, server)
		}()
	}
}
