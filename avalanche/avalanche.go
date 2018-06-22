package avalanche

import (
	"github.com/ipfs/go-ipld-cbor"
	"github.com/quorumcontrol/avalanche/member"
	"sync"
)

var GenesisConflictSet *ConflictSet
var GenesisTransaction *AvalancheTransaction

func init() {
	GenesisTransaction = &AvalancheTransaction{
		Id: "GENESIS",
		Chit: true,
		Accepted: true,
	}
	GenesisConflictSet = &ConflictSet{
		Transactions: map[string]*AvalancheTransaction{GenesisTransaction.Id: GenesisTransaction},
		Pref: GenesisTransaction,
		Last: GenesisTransaction,
		Count: 1,
	}
	GenesisTransaction.ConflictSet = GenesisConflictSet
}

type Application interface {
	GetConflicSet(transaction *AvalancheTransaction) (*ConflictSet)
}

type AvalancheTransaction struct {
	Id string
	Payload *cbornode.Node
	Parents []*AvalancheTransaction
	Chit bool
	Accepted bool
	ConflictSet *ConflictSet
}

func (at *AvalancheTransaction) IsPreffered() bool {
	return at.ConflictSet.Pref == at
}

type ConflictSet struct {
	Transactions map[string]*AvalancheTransaction
	Pref *AvalancheTransaction
	Last *AvalancheTransaction
	Count int
}

func NewAvalancheNode(system *member.NodeSystem) *member.Node {
	node := member.NewNode(system)
	node.Metadata["_UnqueriedTransactions"] = make(map[string]*AvalancheTransaction)
	node.Metadata["_UnQueriedTransactionsMutex"] = &sync.RWMutex{}
	node.Metadata["_App"] = system.Metadata["_App"]
	node.Metadata["_Transactions"] = make(map[string]*AvalancheTransaction)
	node.Metadata["_TransactionLock"] = &sync.RWMutex{}

	return node
}

func (cs *ConflictSet) AddTransaction(trans *AvalancheTransaction) {
	cs.Transactions[trans.Id] = trans
	if len(cs.Transactions) == 1 {
		cs.Pref = trans
		cs.Last = trans
		cs.Count = 0
	}
}

func OnQuery(n *member.Node, transaction *cbornode.Node, responseChan chan *cbornode.Node) {
	avalancheTrans := &AvalancheTransaction{}
	cbornode.DecodeInto(transaction.RawData(), avalancheTrans)
	transLock := n.Metadata["_TransactionLock"].(*sync.RWMutex)
	transLock.RLock()
	defer transLock.RUnlock()

	trans,ok := n.Metadata["_Transactions"].(map[string]*AvalancheTransaction)[avalancheTrans.Id]
	if !ok {
		transLock.Lock()
		n.Metadata["_Transactions"].(map[string]*AvalancheTransaction)[avalancheTrans.Id] = trans
		conflictSet := n.System.Metadata["_App"].(Application).GetConflicSet(avalancheTrans)
		avalancheTrans.Chit = false
		avalancheTrans.Accepted = false
		conflictSet.AddTransaction(avalancheTrans)

		transLock.Unlock()

		unqueriedLock := n.Metadata["_UnQueriedTransactionsMutex"].(*sync.RWMutex)
		unqueriedLock.Lock()
		n.Metadata["_UnqueriedTransactions"].(map[string]*AvalancheTransaction)[avalancheTrans.Id] = avalancheTrans
		unqueriedLock.Unlock()
	}
}

