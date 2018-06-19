package snowball

import (
	"time"
	"github.com/quorumcontrol/avalanche/member"
)


func OnQuery(n *member.Node, state string, responseChan chan string) {
	//fmt.Printf("node %v received onQuery with state: %v\n", n.Id, state)
	if n.State == "" {
		n.State = state
		n.LastState = state
	}
	<-time.After(time.Duration(n.System.ArtificialLatency) * time.Millisecond)
	responseChan <- n.State

	ok,val := n.Metadata["didKickOff"].(bool)
	if !ok || !val {
		n.Metadata["didKickOff"] = true
		go func() {
			for {
				responses := make(map[string]int)
				for i := 0; i < n.System.K; i++ {
					node := n.System.Nodes.RandNode()
					//fmt.Printf("node %v is querying %v\n", n.Id, node.Id)
					resp,err := node.SendQuery(state)
					if err == nil {
						responses[resp]++
					}
					//fmt.Printf("node %v received response %v from %v\n", n.Id, resp, node.Id)
				}
				//fmt.Printf("node %v responses: %v\n", n.Id, responses)
				for state,count := range responses {
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
				if n.Count > n.System.Beta {
					//fmt.Printf("stopping because beta reached\n")
					break
				}
			}
			n.Accepted = true
		}()
	}
}