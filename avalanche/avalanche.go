package avalanche

import (
	"github.com/ipfs/go-ipld-cbor"
	"sync"
	"time"
	"github.com/multiformats/go-multihash"
	"github.com/ipfs/go-cid"
	"fmt"
	"math/rand"
	"github.com/google/uuid"
	"github.com/quorumcontrol/avalanche/storage"
)

var GenesisConflictSet *ConflictSet
var GenesisTransaction *AvalancheTransaction
var GenesisCid *cid.Cid

func init() {

	cbornode.RegisterCborType(WireTransaction{})
	cbornode.RegisterCborType(AvalancheTransaction{})
	cbornode.RegisterCborType(ConflictSet{})

	genesisObj,err := cbornode.WrapObject("genesis", multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}

	genesisWireTransaction := WireTransaction{
		Payload: genesisObj.RawData(),
		Parents: nil,
	}

	GenesisCid = genesisWireTransaction.Cid()

	GenesisTransaction = &AvalancheTransaction{
		WireTransaction: genesisWireTransaction,
		Chit: true,
		Accepted: true,
		Count: 1,
	}

	GenesisConflictSet = &ConflictSet{
		Transactions: []*cid.Cid{GenesisCid},
		Pref: GenesisCid,
		Last: GenesisCid,
		Count: 1,
	}
	GenesisTransaction.ConflictSetId = "GENESIS"
}


type NodeId string

var ConflicSetBucket = []byte("CONFLICT_SETS")
var TransactionBucket = []byte("AVALANCHE_TRANSACTIONS")

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
	Storage storage.Storage
	UnqueriedTransactions map[string]*AvalancheTransaction
	TransactionLock *sync.RWMutex
	UnqueriedTransactionLock *sync.RWMutex
	ConflictSetLock *sync.RWMutex
	App Application
}

type NodeHolder map[NodeId]*Node

func NewNode(system *NodeSystem, storage storage.Storage, app Application) *Node {
	storage.CreateBucketIfNotExists(ConflicSetBucket)
	storage.CreateBucketIfNotExists(TransactionBucket)

	n := &Node{
		Id: NodeId(uuid.New().String()),
		Incoming: make(chan transactionQuery, system.BetaOne),
		StopChan: make(chan bool),
		Counts: make(map[*cbornode.Node]int),
		System: system,
		Metadata: make(map[string]interface{}),
		Storage: storage,
		App: app,
		UnqueriedTransactions: make(map[string]*AvalancheTransaction),
		OnQuery: OnQuery,
		TransactionLock: &sync.RWMutex{},
		UnqueriedTransactionLock: &sync.RWMutex{},
		ConflictSetLock: &sync.RWMutex{},
	}

	GenesisTransaction.Save(n)

	return n
}

func (n *Node) Start() error {
	ticker := time.NewTicker(500 * time.Millisecond)

	go func() {
		for {
			select {
			case <-n.StopChan:
				ticker.Stop()
				break
			case query := <-n.Incoming:
				n.OnQuery(n, query.transaction, query.responseChan)
			case <-ticker.C:
				//TODO: unqueried transaction stuff
			}
		}
	}()
	return nil
}

func (n *Node) Stop() error {
	n.StopChan <- true
	return nil
}

func (n *Node) GetConflictSet(id string) (*ConflictSet, error) {
	csBytes,err := n.Storage.Get(ConflicSetBucket, []byte(id))
	if err != nil {
		return nil, fmt.Errorf("error getting %v: %v", id, err)
	}

	cs := &ConflictSet{}
	err = cbornode.DecodeInto(csBytes, cs)
	return cs, err
}

func (n *Node) GetTransaction(id *cid.Cid) (*AvalancheTransaction, error) {
	tBytes,err := n.Storage.Get(TransactionBucket, id.Bytes())
	if err != nil {
		return nil, fmt.Errorf("error getting %v: %v", id, err)
	}

	cs := &AvalancheTransaction{}
	err = cbornode.DecodeInto(tBytes, cs)
	return cs, err
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


type Application interface {
	GetConflictSetId(transaction *AvalancheTransaction) (string)
}

type WireTransaction struct {
	Payload []byte
	Parents []*cid.Cid
}

func (wt WireTransaction) Cid() *cid.Cid {
	id,err := cbornode.WrapObject(wt, multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return id.Cid()
}

type AvalancheTransaction struct {
	WireTransaction
	Chit bool
	Accepted bool
	Count int
	ConflictSetId string
}

func (at *AvalancheTransaction) Cid() *cid.Cid {
	return at.WireTransaction.Cid()
}

func (at *AvalancheTransaction) Save(n *Node) error {
	atBytes,err := cbornode.WrapObject(at, multihash.SHA2_256, -1)
	if err != nil {
		return err
	}
	return n.Storage.Set(TransactionBucket, at.Cid().Bytes(), atBytes.RawData())
}

func (at *AvalancheTransaction) IsPreferred(n *Node) bool {
	n.ConflictSetLock.RLock()
	defer n.ConflictSetLock.RUnlock()

	conflicSet,err := n.GetConflictSet(at.ConflictSetId)
	if err != nil {
		panic(err) //TODO: no panic
	}
	return conflicSet.Pref == at.Cid()
}

func (at *AvalancheTransaction) IsStronglyPreferred(n *Node) bool {
	for _,parentCid := range at.Parents {
		parent,err := n.GetTransaction(parentCid)
		if err != nil {
			panic(err) //TODO: no panic
		}
		if !parent.IsStronglyPreferred(n) {
			return false
		}
	}
	return true
}

type ConflictSet struct {
	Id string
	Transactions []*cid.Cid
	Pref *cid.Cid
	Last *cid.Cid
	Count int
}

func (cs *ConflictSet) AddTransaction(trans *AvalancheTransaction) {
	cs.Transactions = append(cs.Transactions, trans.Cid())
	if len(cs.Transactions) == 1 {
		cs.Pref = trans.Cid()
		cs.Last = trans.Cid()
		cs.Count = 0
	}
}
func (cs *ConflictSet) Save(n *Node) error {
	csBytes,err := cbornode.WrapObject(cs, multihash.SHA2_256, -1)
	if err != nil {
		return err
	}
	return n.Storage.Set(ConflicSetBucket, []byte(cs.Id), csBytes.RawData())
}

func OnQuery(n *Node, transaction *cbornode.Node, responseChan chan *cbornode.Node) {
	wireTrans := &WireTransaction{}
	cbornode.DecodeInto(transaction.RawData(), wireTrans)

	n.TransactionLock.RLock()
	avalancheTransaction,_ := n.GetTransaction(wireTrans.Cid())

	if avalancheTransaction == nil {
		n.TransactionLock.Lock()
		n.ConflictSetLock.Lock()
		n.UnqueriedTransactionLock.Lock()

		avalancheTransaction = &AvalancheTransaction{
			WireTransaction: *wireTrans,
		}
		conflictSetId := n.App.GetConflictSetId(avalancheTransaction)

		avalancheTransaction.ConflictSetId = conflictSetId
		avalancheTransaction.Save(n)

		cs,err := n.GetConflictSet(conflictSetId)
		if err != nil {
			panic(err) //TODO: no panic
		}

		cs.AddTransaction(avalancheTransaction)
		cs.Save(n)

		n.UnqueriedTransactions[avalancheTransaction.Cid().String()] = avalancheTransaction

		n.UnqueriedTransactionLock.Unlock()
		n.TransactionLock.Unlock()
		n.ConflictSetLock.Unlock()
	}

	n.TransactionLock.RUnlock()

	resp,err := cbornode.WrapObject(avalancheTransaction.IsStronglyPreferred(n), multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}

	responseChan <- resp
}

