package avalanche

import (
	"testing"
	"strconv"
	"github.com/stretchr/testify/assert"
	"github.com/quorumcontrol/avalanche/storage"
	"github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

type TestApplication struct {

}

func (ta *TestApplication) GetConflictSetId(at *AvalancheTransaction) string {
	return string(at.WireTransaction.Payload)
}

func TestDoesntError(t *testing.T) {
	system := &NodeSystem{
		N:       10,
		K:       5,
		Alpha:   4, // slight deviation to avoid floats, just calculate k*a from the paper
		BetaOne: 10,
		BetaTwo: 150,
		Metadata: map[string]interface{}{
			"ArtificialLatency": 100,
		},
	}

	app := &TestApplication{}

	holder := make(NodeHolder)
	for i := 0; i < system.N; i++ {
		store := storage.NewMemStorage()
		node :=  NewNode(system, store, app)
		node.Id = NodeId(strconv.Itoa(i)) // for readability of the logs
		holder[node.Id] = node
		node.Start()
		node.OnQuery = OnQuery
	}
	defer func() {
		for _,node := range holder {
			node.Stop()
		}
	}()

	system.Nodes = holder

	wire := &WireTransaction{
		Payload: nil,
		Parents: []*cid.Cid{GenesisCid},
	}

	wireBytes,_ := cbornode.WrapObject(wire, multihash.SHA2_256, -1)

	node := system.Nodes.RandNode()
	resp,err := node.SendQuery(wireBytes)
	assert.Nil(t, err)
	assert.Equal(t, false, resp)
}
