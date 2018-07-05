package dht

import (
	"sync"
	"github.com/spacemeshos/go-spacemesh/crypto"
	"github.com/spacemeshos/go-spacemesh/p2p/identity"
)

var mainChan = make(chan Message)
var keyToChan = make(map[string]chan Message)

var initRouting sync.Once

func MsgRouting() {
	go func() {
		for {
			mc := <-mainChan
			nodechan, ok := keyToChan[mc.(*msgMock).target.String()]
			if ok {
				nodechan <- mc
			}
		}
	}()
}

type msgMock struct {
	data []byte
	sender identity.Node
	target crypto.PublicKey
}

func (mm *msgMock) Data() []byte {
	return mm.data
}

func (mm *msgMock) Sender() identity.Node {
	return mm.sender
}
func (mm *msgMock) Target() crypto.PublicKey {
	return mm.target
}

type p2pMock struct {
	local identity.Node
	inchannel chan Message
	outchannel chan Message
}

func newP2PMock(local identity.Node) *p2pMock {
	p2p := &p2pMock{
		local: local,
		inchannel:make(chan Message, 3),
		outchannel:mainChan,
	}
	keyToChan[local.String()] = p2p.inchannel
	return p2p
}

func (p2p *p2pMock) RegisterProtocol(protocol string) chan Message {
	return p2p.inchannel
}

func (p2p *p2pMock) SendMessage(target crypto.PublicKey, msg []byte) {
	p2p.outchannel <- &msgMock{msg, p2p.local, target}
}