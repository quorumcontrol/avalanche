package snowball

import "math/rand"

type NodeId string

type NodeSystem struct {
	N int
	K int
	Beta int
	Nodes NodeHolder
}

/*

procedure snowballLoop(u, col0 ∈ {R, B, ⊥})
2: col := col0, lastcol := col0, cnt := 0
3: d[R] := 0, d[B] := 0
4: while undecided do
5: if col = ⊥ then continue
6: K := sample(N \u, k)
7: P := [query(v, col) for v ∈ K]
8: for col0 ∈ {R, B} do
9: if P.count(col0
) ≥ α · k then
10: d[col0
]++
11: if d[col0
] > d[col] then
12: col := col0
13: if col0
6= lastcol then
14: lastcol := col0
, cnt := 0
15: else
16: if ++cnt > β then accept(col)
Figure 3: Snowball.
 */

type Node struct {
	Id NodeId
	State string
	LastState string
	Incoming chan string
	Counts map[string]int
	System *NodeSystem
}

type NodeHolder map[NodeId]*Node

func (n Node) OnReceive(state string) {
	sample := make([]*Node, n.System.K)
	for {
		for i := 0; i < len(sample); i++ {
			sample[i] = randNode(n.System.Nodes)
		}

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