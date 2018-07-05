package dht

import (
	"github.com/pkg/errors"
	"github.com/spacemeshos/go-spacemesh/p2p/identity"
	"time"
)

const (
	BootstrapTimeout = 5 * time.Minute
	LookupIntervals  = 3 * time.Second
)

var (
	ErrZeroConnections   = errors.New("can't bootstrap minimum connections set to 0")
	ErrConnectToBootNode = errors.New("failed to read or connect to any boot node")
	ErrFoundOurself      = errors.New("found ourselves in the routing table")
	ErrFailedToBoot      = errors.New("failed to bootstrap within time limit")
)

func (d *DHT) Bootstrap() error {

	c := int(d.config.RandomConnections)
	if c <= 0 {
		return ErrZeroConnections
	}
	// register bootstrap nodes
	bn := 0
	for _, n := range d.config.BootstrapNodes {
		node, err := identity.NewNodeFromString(n)
		if err != nil {
			// TODO : handle errors
			continue
		}
		d.rt.Update(node)
		bn++
	}

	if bn == 0 {
		return ErrConnectToBootNode
	}

	timeout := time.NewTimer(BootstrapTimeout)

HEALTHLOOP:
	for {
		reschan := make(chan error)

		go func() {
			_, err := d.Lookup(d.local.PublicKey())
			reschan <- err
		}()

		select {
		case <-timeout.C:
			return ErrFailedToBoot
			break HEALTHLOOP
		case err := <-reschan:
			if err == nil {
				return ErrFoundOurself
			}
			// We want to have lookup failed error
			// no one should return us ourselves.
			req := make(chan int)
			d.rt.Size(req)
			size := <-req
			if size >= c {
				break HEALTHLOOP
			}
			time.Sleep(LookupIntervals)
		}
	}
	return nil // succeed
}
