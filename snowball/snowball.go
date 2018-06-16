package snowball

import (
	"math/rand"
	"time"
	"fmt"
	"github.com/google/uuid"
)

type NodeId string

type NodeSystem struct {
	N int
	K int
	Alpha int // slight deviation to avoid floats, just calculate k*a from the paper
	Beta int
	Nodes NodeHolder
	ArtificialLatency int // in ms
}

type stateQuery struct {
	state string
	responseChan chan string
}

type Node struct {
	Id NodeId
	State string
	LastState string
	Incoming chan stateQuery
	StopChan chan bool
	Counts map[string]int
	Count int
	System *NodeSystem
	Accepted bool
	didKickOff bool
}

type NodeHolder map[NodeId]*Node

func NewNode(system *NodeSystem) *Node {
	return &Node{
		Id: NodeId(uuid.New().String()),
		Incoming: make(chan stateQuery, system.Beta),
		StopChan: make(chan bool),
		Counts: make(map[string]int),
		System: system,
	}
}

func (n *Node) Start() error {
	go func() {
		for {
			select {
			case <-n.StopChan:
				break
			case query := <- n.Incoming:
				n.OnQuery(query.state, query.responseChan)
			}
		}
	}()
	return nil
}

func (n *Node) Stop() error {
	n.StopChan <- true
	return nil
}

func (n *Node) OnQuery(state string, responseChan chan string) {
	//fmt.Printf("node %v received onQuery with state: %v\n", n.Id, state)
	if n.State == "" {
		n.State = state
		n.LastState = state
	}
	<-time.After(time.Duration(n.System.ArtificialLatency) * time.Millisecond)
	responseChan <- n.State

	if !n.didKickOff {
		n.didKickOff = true
		go func() {
			for {
				responses := make(map[string]int)
				for i := 0; i < n.System.K; i++ {
					node := randNode(n.System.Nodes)
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

func (n *Node) SendQuery(state string) (string,error) {
	t := time.After(10 * time.Second)
	respChan := make(chan string)
	n.Incoming <- stateQuery{
		state: state,
		responseChan: respChan,
	}
	select {
	case <-t:
		fmt.Printf("timeout on sendquery")
		return "", fmt.Errorf("timeout")
	case resp := <-respChan:
		return resp,nil
	}
}

func randNode(nh NodeHolder) *Node {
	i := rand.Intn(len(nh))
	for _,node := range nh {
		if i == 0 {
			return node
		}
		i--
	}
	panic("never")
}