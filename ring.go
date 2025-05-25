package theatre

import (
	"hash/fnv"
	"sort"
)

type Node struct {
	ID   string
	Hash uint64
}

type Ring struct {
	Nodes []Node
}

func (r *Ring) AddNode(id string) {
	h := hashKey(id)
	r.Nodes = append(r.Nodes, Node{ID: id, Hash: h})
	sort.Slice(r.Nodes, func(i, j int) bool {
		return r.Nodes[i].Hash < r.Nodes[j].Hash
	})
}

func (r *Ring) GetOwner(key string) string {
	kh := hashKey(key)
	for _, node := range r.Nodes {
		if kh <= node.Hash {
			return node.ID
		}
	}

	// wrap around to first node (ring)
	return r.Nodes[0].ID
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
