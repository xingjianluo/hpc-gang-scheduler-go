package scheduler

// Node 模拟集群中的一个计算节点。
type Node struct {
	ID       string   `json:"id"`
	Capacity Resource `json:"capacity"`
	Used     Resource `json:"used"`
	Alive    bool     `json:"alive"`
}

// Free 返回节点剩余可用资源。
func (n *Node) Free() Resource { return n.Capacity.Sub(n.Used) }
