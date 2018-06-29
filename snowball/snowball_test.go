package snowball

import (
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-ipld-cbor"
	"github.com/multiformats/go-multihash"
	"github.com/quorumcontrol/avalanche/member"
	"github.com/quorumcontrol/avalanche/storage"
	"github.com/stretchr/testify/assert"
)

func TestNodes(t *testing.T) {
	system := &member.NodeSystem{
		N:       1000,
		K:       20,
		Alpha:   15, // slight deviation to avoid floats, just calculate k*a from the paper
		BetaOne: 120,
		Metadata: map[string]interface{}{
			"ArtificialLatency": 0,
		},
	}

	holder := make(member.NodeHolder)
	for i := 0; i < system.N; i++ {
		store := storage.NewMemStorage()
		node := member.NewNode(system, store)
		node.Id = member.NodeId(strconv.Itoa(i)) // for readability of the logs
		holder[node.Id] = node
		node.Start()
		node.OnQuery = OnQuery
	}
	defer func() {
		for _, node := range holder {
			node.Stop()
		}
	}()

	system.Nodes = holder

	state, _ := cbornode.WrapObject(true, multihash.SHA2_256, -1)

	node := system.Nodes.RandNode()
	resp, err := node.SendQuery(state)
	assert.Nil(t, err)
	assert.Equal(t, state, resp)

	start := time.Now()
	for {
		<-time.After(1 * time.Second)
		count := 0
		for _, node := range holder {
			if node.Accepted {
				count++
			}
		}
		if count == system.N {
			break
		}
	}
	stop := time.Now()
	t.Logf("start: %v, stop: %v: diff: %d", start, stop, stop.Unix()-start.Unix())

	for _, node := range holder {
		assert.True(t, node.Accepted, "node %v", node.Id)
		//t.Logf("node: %s, accepted? %v", node.Id, node.Accepted)
	}

}
