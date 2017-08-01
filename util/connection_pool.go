package util

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

type Conn interface {
	Close() error
	Health() error
}

var nowFunc = time.Now

var (
	ErrPoolExhausted = errors.New("ConnectionPool: connection pool exhausted")
)

type ConnPool struct {
	Dial func() (Conn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application.
	TestOnBorrow func(conn Conn) error

	// Maximum number of idle connections in the pool
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time
	// When zero, there is no limit on the number of connections in the pool
	MaxActive int

	// Close connection after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// if Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields below
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front
	idle list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

func (p *ConnPool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()

	return active
}

// Close releases the resources used by the pool.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

func (p *ConnPool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

func (p *ConnPool) Get() (Conn, error) {
	p.mu.Lock()

	// Prune stale connections.

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.

		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.

		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("redigo: get on closed pool")
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *ConnPool) Put(c Conn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose && c != nil {
		p.idle.PushBack(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Front()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.Close()
}
