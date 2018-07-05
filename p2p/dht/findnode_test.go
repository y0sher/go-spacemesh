package dht

import (
	"testing"
	"github.com/spacemeshos/go-spacemesh/p2p/nodeconfig"
	"github.com/spacemeshos/go-spacemesh/p2p/identity"
	"github.com/stretchr/testify/assert"
)

func createTestFindNode(t *testing.T, config nodeconfig.Config) (*identity.LocalNode, *findNodeProtocol) {
	node, _ := identity.GenerateTestNode(t)
	initRouting.Do(MsgRouting)
	p2pmock := newP2PMock(node.Node)
	rt := NewRoutingTable(config.SwarmConfig.RoutingTableBucketSize, node.DhtID(), node.Logger)
	return node, newFindNodeProtocol(p2pmock, rt)
}


func TestFindNodeProtocol_FindNode(t *testing.T) {
	_, fnd1 := createTestFindNode(t, nodeconfig.DefaultConfig())
	node2, _ := createTestFindNode(t, nodeconfig.DefaultConfig())
	idarr, err := fnd1.FindNode(node2.Node, identity.GenerateRandomNodeData().String())

	assert.NoError(t, err, "Should not return error")
	assert.Equal(t, []identity.Node{}, idarr,"Should be an empty array")
}


func TestFindNodeProtocol_FindNode2(t *testing.T) {
	node1, fnd1 := createTestFindNode(t, nodeconfig.DefaultConfig())
	node2, fnd2 := createTestFindNode(t, nodeconfig.DefaultConfig())
	randnode := identity.GenerateRandomNodeData()

	fnd2.rt.Update(randnode)

	idarr, err := fnd1.FindNode(node2.Node, randnode.String())

	expected := []identity.Node{randnode}

	assert.NoError(t, err, "Should not return error")
	assert.Equal(t, expected, idarr,"Should be array that contains the node")

	for _, n := range identity.GenerateRandomNodesData(10) {
		fnd2.rt.Update(n)
		expected = append(expected, n)
	}

	// sort because this is how its returned
	expected = identity.SortByDhtID(expected, randnode.DhtID())

	idarr, err = fnd1.FindNode(node2.Node, randnode.String())

	assert.NoError(t, err, "Should not return error")
	assert.Equal(t, expected, idarr,"Should be array that contains the node")

	idarr, err = fnd2.FindNode(node1.Node, randnode.String())

	assert.NoError(t, err, "Should not return error")
	assert.Equal(t, expected, idarr,"Should be array that contains the node")
}

