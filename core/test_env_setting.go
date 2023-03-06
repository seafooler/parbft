package core

import (
	"github.com/seafooler/parbft/config"
	"github.com/seafooler/sign_tools"
	"strconv"
	"time"
)

func Setup(numNode int, startPort int, logLevel int) []*Node {
	id2NameMap := make(map[int]string, numNode)
	name2IdMap := make(map[string]int, numNode)
	id2AddrMap := make(map[int]string, numNode)
	id2PortMap := make(map[int]string, numNode)
	for i := 0; i < numNode; i++ {
		name := "node" + strconv.Itoa(i)
		addr := "127.0.0.1"
		port := strconv.Itoa(startPort + i)
		id2NameMap[i] = name
		name2IdMap[name] = i
		id2AddrMap[i] = addr
		id2PortMap[i] = port
	}

	shares, pubKey := sign_tools.GenTSKeys(numNode/3*2+1, numNode)

	nodes := make([]*Node, numNode)

	for id, name := range id2NameMap {
		conf := config.New(id, name, id2NameMap, name2IdMap, id2AddrMap[id], id2PortMap[id],
			shares[id], pubKey, id2AddrMap, id2PortMap, 10, logLevel, 500, 0, false,
			500, 512, 1000, 512, 20)

		nodes[id] = NewNode(conf)
	}

	for _, node := range nodes {
		if err := node.StartP2PListen(); err != nil {
			panic(err)
		}
	}

	for _, node := range nodes {
		go node.EstablishP2PConns()
	}

	//Wait the all the connections to be established
	time.Sleep(time.Second)

	for _, node := range nodes {
		go node.HandleMsgsLoop()
		go func(n *Node) {
			if n.Id == n.Hs.LeaderId {
				n.Hs.ProofReady <- &ProofData{
					Proof:  nil,
					Height: 0,
				}
			}
		}(node)
	}

	return nodes
}
