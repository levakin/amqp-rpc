package rpc

import (
	"sync"

	"github.com/levakin/amqp-rpc/status"
)

type pendingCall struct {
	st   *status.Status
	done chan struct{}
	data []byte
}

type calls struct {
	mu  sync.Mutex
	pcs map[string]pendingCall
}

func (c *calls) get(corrID string) (pendingCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pc, ok := c.pcs[corrID]
	return pc, ok
}

func (c *calls) set(corrID string, pc pendingCall) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pcs[corrID] = pc
}

func (c *calls) delete(corrID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pcs, corrID)
}
