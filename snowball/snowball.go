package snowball

import (
	"time"

	"github.com/ipfs/go-ipld-cbor"
	"github.com/quorumcontrol/avalanche/member"
)

func OnQuery(n *member.Node, transaction *cbornode.Node, responseChan chan *cbornode.Node) {
	//fmt.Printf("node %v received onQuery with state: %v\n", n.Id, state)
	if n.State == nil {
		n.State = transaction
		n.LastState = transaction
	}
	<-time.After(time.Duration(n.System.Metadata["ArtificialLatency"].(int)) * time.Millisecond)
	responseChan <- n.State

	ok, val := n.Metadata["didKickOff"].(bool)
	if !ok || !val {
		n.Metadata["didKickOff"] = true
		go func() {
			for {
				responseCounts := make(map[*cbornode.Node]int)
				responses := make([]chan *cbornode.Node, n.System.K)
				for i := 0; i < n.System.K; i++ {
					respChan := make(chan *cbornode.Node, 1)
					responses[i] = respChan
					//fmt.Printf("node %v is querying %v\n", n.Id, node.Id)
					go func(respChan chan *cbornode.Node) {
						node := n.System.Nodes.RandNode()
						resp, err := node.SendQuery(transaction)
						if err == nil {
							respChan <- resp
						} else {
							respChan <- nil
						}
					}(respChan)

					//fmt.Printf("node %v received response %v from %v\n", n.Id, resp, node.Id)
				}

				for _, responseChan := range responses {
					if resp := <-responseChan; resp != nil {
						responseCounts[resp]++
					}
				}

				//fmt.Printf("node %v responseCounts: %v\n", n.Id, responseCounts)
				for state, count := range responseCounts {
					if count > n.System.Alpha {
						n.Counts[state]++
						if n.Counts[state] > n.Counts[n.State] {
							n.State = state
						}
						if n.State == n.LastState {
							//fmt.Printf("node %v inc count from %d\n", n.Id, n.Count)
							n.Count++
						} else {
							//fmt.Printf("node %v reset count because %v != %v\n", n.Id, n.LastState, n.State)
							n.LastState = state
							n.Count = 0
						}
					} else {
						//fmt.Printf("node %v did not get to alpha\n", n.Id)
					}
				}
				if n.Count > n.System.BetaOne {
					//fmt.Printf("stopping because beta reached\n")
					break
				}
			}
			n.Accepted = true
		}()
	}
}
