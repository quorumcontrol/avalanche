package member

import (
	"github.com/google/uuid"
	"time"
	"fmt"
	"math/rand"
	"github.com/ipfs/go-ipld-cbor"
)

type NodeId string

type NodeSystem struct {
	N        int
	K        int
	Alpha    int // slight deviation to avoid floats, just calculate k*a from the paper
	BetaOne  int
	BetaTwo int
	Nodes    NodeHolder
	Metadata map[string]interface{}
}

type transactionQuery struct {
	transaction *cbornode.Node
	responseChan chan *cbornode.Node
}

type Node struct {
	Id NodeId
	State *cbornode.Node
	LastState *cbornode.Node
	Incoming chan transactionQuery
	StopChan chan bool
	Counts map[*cbornode.Node]int
	Count int
	System *NodeSystem
	Accepted bool
	OnQuery func(*Node, *cbornode.Node, chan *cbornode.Node)
	Metadata map[string]interface{}
}

type NodeHolder map[NodeId]*Node

func NewNode(system *NodeSystem) *Node {
	return &Node{
		Id: NodeId(uuid.New().String()),
		Incoming: make(chan transactionQuery, system.BetaOne),
		StopChan: make(chan bool),
		Counts: make(map[*cbornode.Node]int),
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
				n.OnQuery(n, query.transaction, query.responseChan)
			}
		}
	}()
	return nil
}

func (n *Node) Stop() error {
	n.StopChan <- true
	return nil
}


func (n *Node) SendQuery(state *cbornode.Node) (*cbornode.Node,error) {
	t := time.After(10 * time.Second)
	respChan := make(chan *cbornode.Node)
	n.Incoming <- transactionQuery{
		transaction: state,
		responseChan: respChan,
	}
	select {
	case <-t:
		fmt.Printf("timeout on sendquery")
		return nil, fmt.Errorf("timeout")
	case resp := <-respChan:
		return resp,nil
	}
}

//TODO: this is not cryptographically sound
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
