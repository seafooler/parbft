package core

import (
	"fmt"
	"testing"
	"time"
)

func TestSimpleSMVBA(t *testing.T) {
	numNode := 4
	nodes := Setup(numNode, 9016, 3)

	originalDatas := make([][]byte, numNode)
	proofs := make([][]byte, numNode)

	for i, _ := range nodes {
		originalDatas[i] = []byte("seafooler" + fmt.Sprintf("%d%d%d%d", i, i, i, i))
		proofs[i] = nil
	}

	for _, node := range nodes {
		node.smvbaMap[0] = NewSMVBA(node, 0)
		node.abaMap[0] = NewABA(node, 0)
	}

	for i, node := range nodes {
		if i == 3 {
			time.Sleep(time.Millisecond * 100)
		}
		go node.smvbaMap[0].RunOneMVBAView(false, originalDatas[i], proofs[i], -1)
	}

	go func() {
		for {
			select {
			case output := <-nodes[0].smvbaMap[0].OutputCh():
				fmt.Printf("Output from node0, snv: %v, value: %s\n", output, nodes[0].smvbaMap[0].Output())
			case output := <-nodes[1].smvbaMap[0].OutputCh():
				fmt.Printf("Output from node1, snv: %v, value: %s\n", output, nodes[1].smvbaMap[0].Output())
			case output := <-nodes[2].smvbaMap[0].OutputCh():
				fmt.Printf("Output from node2, snv: %v, value: %s\n", output, nodes[2].smvbaMap[0].Output())
			case output := <-nodes[3].smvbaMap[0].OutputCh():
				fmt.Printf("Output from node3, snv: %v, value: %s\n", output, nodes[3].smvbaMap[0].Output())
			}
		}
	}()

	time.Sleep(time.Second * 6)
}
