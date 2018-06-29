package avalanche

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipld-cbor"
	"github.com/multiformats/go-multihash"
	"github.com/quorumcontrol/avalanche/storage"
)

var GenesisConflictSet *ConflictSet
var GenesisTransaction *AvalancheTransaction
var GenesisCid *cid.Cid

func init() {

	cbornode.RegisterCborType(WireTransaction{})
	cbornode.RegisterCborType(AvalancheTransaction{})
	cbornode.RegisterCborType(ConflictSet{})

	genesisObj, err := cbornode.WrapObject("genesis", multihash.SHA2_256, -1)
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
		Chit:            true,
		Accepted:        true,
		Count:           0,
	}

	GenesisConflictSet = &ConflictSet{
		Id:           "GENESIS",
		Transactions: []*cid.Cid{GenesisCid},
		Pref:         GenesisCid,
		Last:         GenesisCid,
		Count:        1,
	}
	GenesisTransaction.ConflictSetId = "GENESIS"
	log.Printf("Genesis Cid: %v", GenesisCid)
}

type NodeId string

var ConflictSetBucket = []byte("CONFLICT_SETS")
var TransactionBucket = []byte("AVALANCHE_TRANSACTIONS")

type NodeSystem struct {
	N        int
	K        int
	Alpha    int // slight deviation to avoid floats, just calculate k*a from the paper
	BetaOne  int
	BetaTwo  int
	Nodes    NodeHolder
	Metadata map[string]interface{}
}

type transactionQuery struct {
	transaction  *cbornode.Node
	responseChan chan *cbornode.Node
}

type Node struct {
	Id                       NodeId
	State                    *cbornode.Node
	LastState                *cbornode.Node
	Incoming                 chan transactionQuery
	StopChan                 chan bool
	Counts                   map[*cbornode.Node]int
	Count                    int
	System                   *NodeSystem
	Accepted                 bool
	OnQuery                  func(*Node, *cbornode.Node, chan *cbornode.Node)
	Metadata                 map[string]interface{}
	Storage                  storage.Storage
	UnqueriedTransactions    chan *AvalancheTransaction
	TransactionLock          *sync.RWMutex
	UnqueriedTransactionLock *sync.RWMutex
	ConflictSetLock          *sync.RWMutex
	App                      Application
	waitingTransactions      map[string]chan bool
	waitTransLock            *sync.RWMutex
}

type NodeHolder map[NodeId]*Node

func NewNode(system *NodeSystem, storage storage.Storage, app Application) *Node {
	storage.CreateBucketIfNotExists(ConflictSetBucket)
	storage.CreateBucketIfNotExists(TransactionBucket)

	n := &Node{
		Id:       NodeId(uuid.New().String()),
		Incoming: make(chan transactionQuery, system.BetaTwo),
		StopChan: make(chan bool),
		Counts:   make(map[*cbornode.Node]int),
		System:   system,
		Metadata: make(map[string]interface{}),
		Storage:  storage,
		App:      app,
		UnqueriedTransactions:    make(chan *AvalancheTransaction, 1000),
		OnQuery:                  OnQuery,
		TransactionLock:          &sync.RWMutex{},
		UnqueriedTransactionLock: &sync.RWMutex{},
		ConflictSetLock:          &sync.RWMutex{},
		waitingTransactions:      make(map[string]chan bool),
		waitTransLock:            &sync.RWMutex{},
	}

	GenesisTransaction.Save(n)
	GenesisConflictSet.Save(n)

	return n
}

func (n *Node) Start() error {
	go func() {
		for {
			select {
			case <-n.StopChan:
				break
			case query := <-n.Incoming:
				n.OnQuery(n, query.transaction, query.responseChan)
			case trans := <-n.UnqueriedTransactions:
				go func() {
					n.UnqueriedTransactionLock.Lock()
					log.Printf("node %v querying an unqueried transaction", n.Id)
					wire, _ := trans.WireTransaction.CborNode() // TODO: handle error
					responseCount := make(map[bool]int)
					responses := make([]chan *cbornode.Node, n.System.K)
					for i := 0; i < n.System.K; i++ {
						respChan := make(chan *cbornode.Node)
						responses[i] = respChan
						go func(respChan chan *cbornode.Node) {
							var node *Node
							for node == nil || node.Id == n.Id {
								node = n.System.Nodes.RandNode()
							}
							log.Printf("node %v is querying node %v with trans %v", n.Id, node.Id, trans.Cid())

							respBytes, err := node.SendQuery(wire) //TODO: handle error
							if err != nil {
								respChan <- nil
							} else {
								respChan <- respBytes
							}
						}(respChan)
						//fmt.Printf("node %v received response %v from %v\n", n.Id, resp, node.Id)
					}

					for i := 0; i < n.System.K; i++ {
						respBytes := <-responses[i]
						if respBytes != nil {
							var resp bool
							cbornode.DecodeInto(respBytes.RawData(), &resp)
							responseCount[resp]++
						}
					}

					//fmt.Printf("node %v responseCount: %v\n", n.Id, responseCount)
					for state, count := range responseCount {
						if count > n.System.Alpha {
							if state {
								n.TransactionLock.RLock()
								trans, _ := n.GetTransaction(trans.Cid())
								n.TransactionLock.RUnlock()
								trans.Chit = true
								trans.UpdateCount(n)
								HandleUpdatedTransaction(n, trans)
							}
						} else {
							log.Printf("node %v did not get to alpha\n", n.Id)
						}
					}
					n.UnqueriedTransactionLock.Unlock()
				}()

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
	csBytes, err := n.Storage.Get(ConflictSetBucket, []byte(id))
	if err != nil {
		return nil, fmt.Errorf("error getting %v: %v", id, err)
	}

	if len(csBytes) == 0 {
		log.Printf("node %v, returning new conflict set (could not find %v in storage)", n.Id, id)
		return NewConflictSet(id), nil
	}

	cs := &ConflictSet{}
	err = cbornode.DecodeInto(csBytes, cs)
	return cs, err
}

func (n *Node) GetTransaction(id *cid.Cid) (*AvalancheTransaction, error) {
	log.Printf("node %v getting transaction %v", n.Id, id)
	tBytes, err := n.Storage.Get(TransactionBucket, id.Bytes())
	if err != nil {
		log.Printf("error getting %v: %v", id, err)
		return nil, fmt.Errorf("error getting %v: %v", id, err)
	}

	if len(tBytes) == 0 {
		log.Printf("node %v could not find %v", n.Id, id)
		return nil, nil
	}
	log.Printf("node %v returning saved transaction %v", n.Id, id)

	cs := &AvalancheTransaction{}
	err = cbornode.DecodeInto(tBytes, cs)
	return cs, err
}

func (n *Node) SendQuery(state *cbornode.Node) (*cbornode.Node, error) {
	t := time.After(20 * time.Second)
	respChan := make(chan *cbornode.Node, 1)
	n.Incoming <- transactionQuery{
		transaction:  state,
		responseChan: respChan,
	}
	select {
	case <-t:
		log.Printf("node %v timeout on sendquery", n.Id)
		return nil, fmt.Errorf("timeout")
	case resp := <-respChan:
		return resp, nil
	}
}

//TODO: this is not cryptographically sound
func (nh NodeHolder) RandNode() *Node {
	i := rand.Intn(len(nh))
	for _, node := range nh {
		if i == 0 {
			return node
		}
		i--
	}
	panic("never")
}

type Application interface {
	GetConflictSetId(transaction *AvalancheTransaction) string
}

type WireTransaction struct {
	Payload []byte
	Parents []*cid.Cid
}

func (wt WireTransaction) Cid() *cid.Cid {
	obj, err := wt.CborNode()
	if err != nil {
		panic(err)
	}
	return obj.Cid()
}

func (wt WireTransaction) CborNode() (*cbornode.Node, error) {
	return cbornode.WrapObject(wt, multihash.SHA2_256, -1)
}

type AvalancheTransaction struct {
	WireTransaction
	Ancestors     map[string]bool
	Chit          bool
	Accepted      bool
	Count         int
	ConflictSetId string
}

func (at *AvalancheTransaction) Cid() *cid.Cid {
	return at.WireTransaction.Cid()
}

func (at *AvalancheTransaction) InitializeAncestors() {
	if at.Ancestors == nil {
		at.Ancestors = make(map[string]bool)
	}
}

func (at *AvalancheTransaction) Save(n *Node) error {
	atBytes, err := cbornode.WrapObject(at, multihash.SHA2_256, -1)
	if err != nil {
		return err
	}

	for _, parentId := range at.Parents {
		log.Printf("node %v getting parent: %v", n.Id, parentId)
		parent, err := n.GetTransaction(parentId)
		if err == nil {
			parent.InitializeAncestors()
			_, ok := parent.Ancestors[at.Cid().KeyString()]
			if !ok {
				parent.Ancestors[at.Cid().KeyString()] = true
				parent.Save(n)
			}
		}
	}

	return n.Storage.Set(TransactionBucket, at.Cid().Bytes(), atBytes.RawData())
}

func (at *AvalancheTransaction) UpdateCount(n *Node) {
	count := 0
	var ancestorTrans []*AvalancheTransaction
	for stringId, _ := range at.Ancestors {
		cid, _ := cid.Cast([]byte(stringId))
		trans, _ := n.GetTransaction(cid)
		count += trans.Count
		ancestorTrans = append(ancestorTrans, trans)
	}
	if at.Chit {
		count++
	}
	if at.Count != count {
		at.Count = count

		for _, trans := range ancestorTrans {
			trans.UpdateCount(n)
		}
	}
}

func (at *AvalancheTransaction) IsPreferred(n *Node) bool {
	if at.Cid().Equals(GenesisCid) {
		return true
	}

	n.ConflictSetLock.RLock()
	log.Printf("node %v unlocking 	defer n.ConflictSetLock.RUnlock()", n.Id)
	defer n.ConflictSetLock.RUnlock()

	conflictSet, err := n.GetConflictSet(at.ConflictSetId)
	if err != nil {
		panic(err) //TODO: no panic
	}
	log.Printf("node %v isPreferred conflicSet %v isEqual? %v pref is %v and at Cid is %v", n.Id, conflictSet.Id, conflictSet.Pref.Equals(at.Cid()), conflictSet.Pref, at.Cid())

	return conflictSet.Pref.Equals(at.Cid())
}

func (at *AvalancheTransaction) IsStronglyPreferred(n *Node) bool {
	for _, parentCid := range at.Parents {
		parent, err := n.GetTransaction(parentCid)
		if err != nil {
			panic(err) //TODO: no panic
		}
		if parentCid.Equals(GenesisCid) {
			continue
		}
		if !parent.IsStronglyPreferred(n) {
			return false
		}
	}
	return at.IsPreferred(n)
}

type ConflictSet struct {
	Id           string
	Transactions []*cid.Cid
	Pref         *cid.Cid
	Last         *cid.Cid
	Count        int
}

func NewConflictSet(id string) *ConflictSet {
	return &ConflictSet{
		Id: id,
	}
}

func (cs *ConflictSet) AddTransaction(trans *AvalancheTransaction) {
	cs.Transactions = append(cs.Transactions, trans.Cid())
	if len(cs.Transactions) == 1 {
		cs.Pref = trans.Cid()
		cs.Last = trans.Cid()
		cs.Count = 0
	}
}

func HandleUpdatedTransaction(n *Node, trans *AvalancheTransaction) {
	csId := n.App.GetConflictSetId(trans)
	cs, _ := n.GetConflictSet(csId) //TODO: handle error\

	log.Printf("node %v, handle updated transaction: %v, pref: %v", n.Id, trans.Cid(), cs.Pref)
	pref, _ := n.GetTransaction(cs.Pref) //TODO: handle error
	if trans.Count > pref.Count {
		cs.Pref = trans.Cid()
	}
	if cs.Last.KeyString() != trans.Cid().KeyString() {
		cs.Last = trans.Cid()
		cs.Count = 0
	} else {
		cs.Count++
	}
	cs.Save(n)

	for stringId := range trans.Ancestors {
		cid, _ := cid.Cast([]byte(stringId))
		log.Printf("node %v, handle updated transaction getting ancestor %v", n.Id, cid)
		trans, _ := n.GetTransaction(cid)
		HandleUpdatedTransaction(n, trans)
	}
}

func (cs *ConflictSet) Save(n *Node) error {
	csBytes, err := cbornode.WrapObject(cs, multihash.SHA2_256, -1)
	if err != nil {
		return err
	}
	return n.Storage.Set(ConflictSetBucket, []byte(cs.Id), csBytes.RawData())
}

func OnQuery(n *Node, transaction *cbornode.Node, responseChan chan *cbornode.Node) {
	go func() {
		log.Printf("node %v processing %v", n.Id, transaction.Cid())
		wireTrans := &WireTransaction{}
		cbornode.DecodeInto(transaction.RawData(), wireTrans)
		log.Printf("node %v RLock transactions", n.Id)
		n.TransactionLock.RLock()
		avalancheTransaction, _ := n.GetTransaction(wireTrans.Cid())

		if avalancheTransaction == nil {
			log.Printf("new transaction %v", transaction.Cid())
			log.Printf("node %v unlocking 			n.TransactionLock.RUnlock()", n.Id)
			n.TransactionLock.RUnlock()

			avalancheTransaction = &AvalancheTransaction{
				WireTransaction: *wireTrans,
			}

			for _, parentId := range avalancheTransaction.Parents {
				log.Printf("node %v RLock transactions", n.Id)
				n.TransactionLock.RLock()
				parent, _ := n.GetTransaction(parentId) //TODO: handle error
				log.Printf("node %v unlocking 				n.TransactionLock.RUnlock()", n.Id)
				n.TransactionLock.RUnlock()

				if parent == nil {
					log.Printf("node %v locking n.waitTransLock.Lock()", n.Id)
					n.waitTransLock.Lock()
					n.waitingTransactions[parentId.KeyString()] = make(chan bool)
					log.Printf("node %v unlocking 					n.waitTransLock.Unlock()", n.Id)
					n.waitTransLock.Unlock()
					log.Printf("node %v waiting for parent %v", n.Id, parentId)
					<-n.waitingTransactions[parentId.KeyString()]

					log.Printf("node %v received parent %v", n.Id, parentId)
					log.Printf("node %v locking n.waitTransLock.Lock()", n.Id)
					n.waitTransLock.Lock()
					delete(n.waitingTransactions, parentId.KeyString())
					log.Printf("node %v unlocking 					n.waitTransLock.Unlock()", n.Id)
					n.waitTransLock.Unlock()
				}
			}

			conflictSetId := n.App.GetConflictSetId(avalancheTransaction)

			log.Printf("node %v locking n.TransactionLock.Lock()", n.Id)
			n.TransactionLock.Lock()
			log.Printf("node %v locking n.ConflictSetLock.Lock()", n.Id)
			n.ConflictSetLock.Lock()

			avalancheTransaction.ConflictSetId = conflictSetId
			avalancheTransaction.Save(n)

			cs, err := n.GetConflictSet(conflictSetId)
			if err != nil {
				panic(err) //TODO: no panic
			}

			cs.AddTransaction(avalancheTransaction)
			cs.Save(n)

			log.Printf("node %v unlocking 			n.TransactionLock.Unlock()", n.Id)
			n.TransactionLock.Unlock()
			log.Printf("node %v unlocking 			n.ConflictSetLock.Unlock()", n.Id)
			n.ConflictSetLock.Unlock()

			n.UnqueriedTransactions <- avalancheTransaction

		} else {
			log.Printf("node %v unlocking 			n.TransactionLock.RUnlock()", n.Id)
			n.TransactionLock.RUnlock()
		}

		isStronglyPreferred := avalancheTransaction.IsStronglyPreferred(n)

		resp, err := cbornode.WrapObject(isStronglyPreferred, multihash.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		log.Printf("node %v returning IsStronglyPreferred: %v on trans: %v", n.Id, isStronglyPreferred, avalancheTransaction.Cid())

		responseChan <- resp
		log.Printf("node %v RLock waitTransLock", n.Id)
		n.waitTransLock.RLock()
		waitChan, ok := n.waitingTransactions[avalancheTransaction.Cid().KeyString()]
		if ok {
			waitChan <- true
		}
		log.Printf("node %v unlocking 		n.waitTransLock.RUnlock()", n.Id)
		n.waitTransLock.RUnlock()
		log.Printf("node %v onQuery finished for trans %v", n.Id, transaction.Cid())
	}()
}
