// Package dht implements a Distributed Hash Table based on Kademlia
package dht

import (
	"github.com/spacemeshos/go-spacemesh/p2p/identity"
	"github.com/spacemeshos/go-spacemesh/p2p/nodeconfig"

	"github.com/pkg/errors"
	"github.com/spacemeshos/go-spacemesh/crypto"
	"time"
)

const LookupTimeout = 5 * time.Second

var (
	ErrLookupFailed      = errors.New("failed to find node in the network")
	ErrEmptyRoutingTable = errors.New("no nodes to query - routing table is empty")
)

type Message interface {
	Sender() identity.Node
	Data() []byte
}

type Service interface {
	RegisterProtocol(protocol string) chan Message
	SendMessage(target crypto.PublicKey, msg []byte)
}


type DHT struct {
	config nodeconfig.SwarmConfig

	local *identity.LocalNode

	rt  RoutingTable
	fnp *findNodeProtocol

	service Service
}

// New creates a new dht
func New(node *identity.LocalNode, config nodeconfig.SwarmConfig, service Service) *DHT {
	d := &DHT{
		config:  config,
		local:   node,
		rt:      NewRoutingTable(config.RoutingTableBucketSize, node.DhtID(), node.Logger),
		service: service,
	}
	d.fnp = newFindNodeProtocol(service, d.rt)
	return d
}

// Update insert or update a node in the routing table.
func (d *DHT) Update(node identity.Node) {
	d.rt.Update(node)
}

// Lookup finds a node in the dht by its public key, it issues a search inside the local routing table,
// if the node can't be found there it sends a query to the network.
func (d *DHT) Lookup(id crypto.PublicKey) (identity.Node, error) {
	dhtid := identity.NewDhtID(id.Bytes())
	poc := make(PeersOpChannel)
	d.rt.NearestPeers(NearestPeersReq{dhtid, d.config.RoutingTableAlpha, poc})
	res := (<-poc).Peers
	if len(res) == 0 {
		return identity.EmptyNode, ErrEmptyRoutingTable
	}

	if res[0].DhtID().Equals(dhtid) {
		return res[0], nil
	}

	return d.kadLookup(id, res)
}

// Implements the kad algo for locating a remote node
// Precondition - node is not in local routing table
// nodeId: - base58 node id string
// Returns requested node via the callback or nil if not found
// Also used as a bootstrap function to populate the routing table with the results.
func (d *DHT) kadLookup(id crypto.PublicKey, searchList []identity.Node) (identity.Node, error) {
	// save queried node ids for the operation
	queried := map[string]struct{}{}

	// iterative lookups for nodeId using searchList

	for {
		// if no new nodes found
		if len(searchList) == 0 {
			break
		}

		// is closestNode out target ?
		closestNode := searchList[0]
		if closestNode.PublicKey().String() == id.String() {
			return closestNode, nil
		}

		// pick up to alpha servers to query from the search list
		// servers that have been recently queried will not be returned
		servers := filterFindNodeServers(searchList, queried, d.config.RoutingTableAlpha)

		if len(servers) == 0 {
			// no more servers to query
			// target node was not found.
			return identity.EmptyNode, ErrLookupFailed
		}

		// lookup nodeId using the target servers
		res := d.findNodeOp(servers, queried, id, closestNode)
		if len(res) > 0 {

			// merge newly found nodes
			searchList = identity.Union(searchList, res)
			// sort by distance from target
			searchList = identity.SortByDhtID(res, identity.NewDhtID(id.Bytes()))
		}
		// keep iterating using new servers that were not queried yet from searchlist (if any)
	}

	return identity.EmptyNode, ErrLookupFailed
}

// FilterFindNodeServers picks up to count server who haven't been queried recently.
func filterFindNodeServers(nodes []identity.Node, queried map[string]struct{}, alpha int) []identity.Node {

	// If no server have been queried already, just make sure the list len is alpha
	if len(queried) == 0 {
		if len(nodes) > alpha {
			nodes = nodes[:alpha]
		}
		return nodes
	}

	// filter out queried servers.
	i := 0
	for _, v := range nodes {
		if _, exist := queried[v.String()]; !exist {
			nodes[i] = v
			i++
		}
		if i >= alpha {
			break
		}
	}

	return nodes[:i]
}

//// Lookup a target node on one or more servers
// Returns closest nodes which are closers than closestNode to targetId
// If node found it will be in top of results list
func (d *DHT) findNodeOp(servers []identity.Node, queried map[string]struct{}, id crypto.PublicKey, closestNode identity.Node) []identity.Node {
	l := len(servers)

	if l == 0 {
		return []identity.Node{}
	}

	// results channel
	results := make(chan []identity.Node)

	// Issue a parallel FindNode op to all servers on the list
	for i := 0; i < l; i++ {
		queried[servers[i].String()] = struct{}{}
		// find node protocol adds found nodes to the local routing table
		// populates queried node's routing table with us and return.
		go func(i int) {
			fnd, err := d.fnp.FindNode(servers[i], id.String())
			if err != nil {
				//TODO: handle errors
				return
			}
			results <- fnd
		}(i)
	}

	done := 0
	idSet := make(map[string]identity.Node)
	timeout := time.NewTimer(LookupTimeout)
Loop:
	for {
		select {
		case res := <-results:
			for i := 0; i < len(res); i++ {
				if _, ok := idSet[res[i].PublicKey().String()]; ok {
					continue
				}
				idSet[res[i].PublicKey().String()] = res[i]
				d.rt.Update(res[i])
			}
			done++
			if done == l {
				close(results)
				break Loop
			}
		case <-timeout.C:
			// we expected nodes to return results within a reasonable time frame
			break Loop
		}
	}

	// add unique node ids that are closer to target id than closest node
	res := make([]identity.Node, len(idSet))
	i := 0
	for _, n := range idSet {
		res[i] = n
		i++
	}

	// sort results by distance from target dht id
	return res
}
