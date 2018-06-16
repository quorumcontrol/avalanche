package snowball

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"strconv"
)

func TestNodes(t *testing.T) {
	system := &NodeSystem{
		N: 1000,
		K: 10,
		Alpha: 8, // slight deviation to avoid floats, just calculate k*a from the paper
		Beta: 10,
		ArtificialLatency: 100, // in ms
	}

	holder := make(NodeHolder)
	for i := 0; i < system.N; i++ {
		node :=  NewNode(system)
		node.Id = NodeId(strconv.Itoa(i)) // for readability of the logs
		holder[node.Id] = node
		node.Start()
	}
	defer func() {
		for _,node := range holder {
			node.Stop()
		}
	}()

	system.Nodes = holder

	node := randNode(holder)
	resp,err := node.SendQuery("test")
	assert.Nil(t, err)
	assert.Equal(t, "test", resp)


	start := time.Now()
	for {
		<- time.After(1 * time.Second)
		count := 0
		for _,node := range holder {
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

	for _,node := range holder {
		assert.True(t, node.Accepted, "node %v", node.Id)
		//t.Logf("node: %s, accepted? %v", node.Id, node.Accepted)
	}

}
