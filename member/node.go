package member

import (
	"github.com/google/uuid"
	"time"
	"fmt"
	"math/rand"
)

type NodeId string

type NodeSystem struct {
	N int
	K int
	Alpha int // slight deviation to avoid floats, just calculate k*a from the paper
	Beta int
	Nodes NodeHolder
	Metadata map[string]interface{}
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
	OnQuery func(*Node, string, chan string)
	Metadata map[string]interface{}
}

type NodeHolder map[NodeId]*Node

func NewNode(system *NodeSystem) *Node {
	return &Node{
		Id: NodeId(uuid.New().String()),
		Incoming: make(chan stateQuery, system.Beta),
		StopChan: make(chan bool),
		Counts: make(map[string]int),
		System: system,
		Metadata: make(map[string]interface{}),
	}
}

func (n *Node) Start() error {
	go func() {
		for {
			select {
			case <-n.StopChan:
				break
			case query := <- n.Incoming:
				n.OnQuery(n, query.state, query.responseChan)
			}
		}
	}()
	return nil
}

func (n *Node) Stop() error {
	n.StopChan <- true
	return nil
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

func (nh NodeHolder) RandNode() *Node {
	i := rand.Intn(len(nh))
	for _,node := range nh {
		if i == 0 {
			return node
		}
		i--
	}
	panic("never")
}
