package avalanche

import (
	"log"
	"strconv"
	"testing"

	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipld-cbor"
	"github.com/multiformats/go-multihash"
	"github.com/quorumcontrol/avalanche/storage"
	"github.com/stretchr/testify/assert"
)

type TestApplication struct {
}

func (ta *TestApplication) GetConflictSetId(at *AvalancheTransaction) string {
	return string(at.WireTransaction.Payload)
}

func TestDoesntError(t *testing.T) {
	system := &NodeSystem{
		N:       5,
		K:       3,
		Alpha:   2, // slight deviation to avoid floats, just calculate k*a from the paper
		BetaOne: 10,
		BetaTwo: 150,
		Metadata: map[string]interface{}{
			"ArtificialLatency": 0,
		},
	}

	app := &TestApplication{}

	holder := make(NodeHolder)
	for i := 0; i < system.N; i++ {
		store := storage.NewMemStorage()
		node := NewNode(system, store, app)
		node.Id = NodeId(strconv.Itoa(i)) // for readability of the logs
		holder[node.Id] = node
		node.OnQuery = OnQuery
		node.Start()
	}
	defer func() {
		for _, node := range holder {
			node.Stop()
		}
	}()

	system.Nodes = holder

	wire := &WireTransaction{
		Payload: []byte("conflictSet1"),
		Parents: []*cid.Cid{GenesisCid},
	}

	wireBytes, _ := cbornode.WrapObject(wire, multihash.SHA2_256, -1)

	node := system.Nodes.RandNode()
	log.Printf("random node from test is: %v", node.Id)

	resp, err := node.SendQuery(wireBytes)
	assert.Nil(t, err)
	assert.True(t, respToBool(resp))

	wire2 := &WireTransaction{
		Payload: []byte("conflictSet1"),
		Parents: []*cid.Cid{wire.Cid()},
	}

	wire2Bytes, _ := cbornode.WrapObject(wire2, multihash.SHA2_256, -1)

	log.Printf("sending wire2 to node %v", node.Id)

	// right now we want to send to the same node to make sure it's not strongly preferred
	// uncomment to choose a different node
	//node = system.Nodes.RandNode()
	resp, err = node.SendQuery(wire2Bytes)
	assert.Nil(t, err)
	assert.False(t, respToBool(resp))

	time.Sleep(5 * time.Second)

	//assert.True(t, false)
}

func respToBool(resp *cbornode.Node) bool {
	var trueOrFalse bool
	cbornode.DecodeInto(resp.RawData(), &trueOrFalse) //TODO: handle error
	return trueOrFalse
}
