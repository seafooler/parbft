package core

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/seafooler/parbft/config"
	"github.com/seafooler/parbft/conn"
	"reflect"
	"sync"
	"time"
)

type Node struct {
	*config.Config
	Hs       *HS
	abaMap   map[int]*ABA
	smvbaMap map[int]*SMVBA

	logger hclog.Logger

	reflectedTypesMap map[uint8]reflect.Type

	trans *conn.NetworkTransport

	// readyData can be sent from the optimistic path or the final ABA
	readyData chan ReadyData

	cachedMsgs map[int][]interface{} // cache the messages arrived in advance

	committedHeight    int
	maxProofedHeight   int
	proofedHeight      map[int]bool
	startedSMVBAHeight int
	startedHSHeight    int

	sync.Mutex
}

func NewNode(conf *config.Config) *Node {
	node := &Node{
		Config:            conf,
		reflectedTypesMap: reflectedTypesMap,
		readyData:         make(chan ReadyData),
		cachedMsgs:        make(map[int][]interface{}),
		smvbaMap:          map[int]*SMVBA{},
		abaMap:            map[int]*ABA{},

		committedHeight: -1,
		proofedHeight:   make(map[int]bool),
	}

	node.logger = hclog.New(&hclog.LoggerOptions{
		Name:   "parbft-node",
		Output: hclog.DefaultOutput,
		Level:  hclog.Level(node.Config.LogLevel),
	})

	node.Hs = NewHS(node, 0)
	return node
}

// StartP2PListen starts the node to listen for P2P connection.
func (n *Node) StartP2PListen() error {
	var err error
	n.trans, err = conn.NewTCPTransport(":"+n.P2pPort, 2*time.Second,
		nil, n.MaxPool, n.reflectedTypesMap)
	if err != nil {
		return err
	}
	return nil
}

// HandleMsgsLoop starts a loop to deal with the msgs from other peers.
func (n *Node) HandleMsgsLoop() {
	msgCh := n.trans.MsgChan()
	for {
		select {
		case msg := <-msgCh:
			switch msgAsserted := msg.(type) {
			case HSProposalMsg:
				go n.Hs.ProcessHSProposalMsg(&msgAsserted)
			case HSVoteMsg:
				go n.Hs.ProcessHSVoteMsg(&msgAsserted)
			case SMVBAPBVALMessage:
				n.logger.Debug("Receive SMVBAPBVALMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].spb.processPBVALMsg(&msgAsserted)
				}
			case SMVBAPBVOTMessage:
				n.logger.Debug("Receive SMVBAPBVOTMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].spb.processPBVOTMsg(&msgAsserted)
				}
			case SMVBAFinishMessage:
				n.logger.Debug("Receive SMVBAFinishMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].HandleFinishMsg(&msgAsserted)
				}
			case SMVBADoneShareMessage:
				n.logger.Debug("Receive SMVBADoneShareMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].HandleDoneShareMsg(&msgAsserted)
				}
			case SMVBAPreVoteMessage:
				n.logger.Debug("Receive SMVBAPreVoteMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].HandlePreVoteMsg(&msgAsserted)
				}
			case SMVBAVoteMessage:
				n.logger.Debug("Receive SMVBAVoteMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].HandleVoteMsg(&msgAsserted)
				}
			case SMVBAHaltMessage:
				n.logger.Debug("Receive SMVBAHaltMessage", "replica", n.Name, "msg", msgAsserted)
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.smvbaMap[msgAsserted.Height].HandleHaltMsg(&msgAsserted)
				}
			case ABABvalRequestMsg:
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.abaMap[msgAsserted.Height].handleBvalRequest(&msgAsserted)
				}
			case ABAAuxRequestMsg:
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.abaMap[msgAsserted.Height].handleAuxRequest(&msgAsserted)
				}
			case ABAExitMsg:
				if n.processItNow(msgAsserted.Height, msgAsserted) {
					go n.abaMap[msgAsserted.Height].handleExitMessage(&msgAsserted)
				}
			default:
				n.logger.Error("Unknown type of the received message!")
			}
		case data := <-n.readyData:

			newBlock := &Block{
				Reqs:     nil,
				Height:   data.Height + 1,
				Proposer: n.Id,
			}

			if data.ComponentId == 0 {
				// update status by the optimistic path
				n.updateStatusByOptimisticData(&data)
			} else {
				if data.Height > n.committedHeight {
					// update status by the pessimistic path
					n.committedHeight = data.Height
					n.logger.Info("commit the block from the final ABA", "replica", n.Name, "block_index", data.Height)
				}
			}

			// continue the optimistic path
			if newBlock.Height == n.startedHSHeight+1 {
				n.LaunchOptimisticPath(newBlock)
				n.startedHSHeight++
			}

			// launch the pessimistic path
			n.LaunchPessimisticPath(newBlock)
		}
	}
}

func (n *Node) LaunchOptimisticPath(blk *Block) {
	go func(blk *Block) {
		if err := n.Hs.BroadcastProposalProof(blk); err != nil {
			n.logger.Error("fail to broadcast proposal and proof", "height", blk.Height,
				"err", err.Error())
		} else {
			n.logger.Debug("successfully broadcast a new proposal and proof", "height", blk.Height)
		}
	}(blk)
}

func (n *Node) LaunchPessimisticPath(blk *Block) {
	if _, ok := n.smvbaMap[blk.Height]; !ok {
		n.smvbaMap[blk.Height] = NewSMVBA(n, blk.Height)
		n.abaMap[blk.Height] = NewABA(n, blk.Height)
		n.startedSMVBAHeight = blk.Height
		n.restoreMessages(blk.Height)
		go func(blk *Block) {
			// For testing: to let the pessimistic path run slower
			time.Sleep(time.Millisecond * 500)

			n.smvbaMap[blk.Height].RunOneMVBAView(false,
				[]byte(fmt.Sprintf("%d", blk.Height)), nil, -1)
		}(blk)
	}
}

func (n *Node) updateStatusByOptimisticData(data *ReadyData) {
	n.logger.Info("Update the node status", "replica", n.Name, "data", data)
	n.Lock()
	defer n.Unlock()

	prevHeight := data.Height - 1

	if prevHeight <= n.committedHeight {
		// previous block of optimistic return height has been committed before, maybe by the final agreement
		return
	} else {
		// previous height has not been committed
		n.proofedHeight[prevHeight] = true
		n.maxProofedHeight = prevHeight

		// call ABA with the optimistic return
		if n.abaMap[prevHeight] == nil {
			n.abaMap[prevHeight] = NewABA(n, prevHeight)
			go n.abaMap[prevHeight].inputValue(prevHeight, 0)
		}
	}

	if prevHeight == n.committedHeight+1 {
		// pre-previous height has been committed
		return
	}

	// try to commit a pre-previous block
	n.tryCommit(data.Height)

	// if there is a subsequent-subsequent block, deal with it
	if _, ok := n.proofedHeight[data.Height+2]; ok {
		n.tryCommit(data.Height + 2)
	}
}

// tryCommit must be wrapped in a lock
func (n *Node) tryCommit(height int) error {
	if _, ok := n.proofedHeight[height-2]; ok {
		n.logger.Info("commit the block from the optimistic path", "replica", n.Name, "block_index", height-2)
		// Todo: check the consecutive commitment
		n.committedHeight = height - 2
		delete(n.proofedHeight, height-2)
	} else {
		n.logger.Info("do not commit the block from the optimistic path", "replica", n.Name, "block_index", height-2)

	}

	// if there is a subsequent-subsequent block, deal with it
	if _, ok := n.proofedHeight[height+2]; ok {
		n.tryCommit(height + 2)
	}

	return nil
}

// processItNow caches sMVBA and ABA messages from the future heights or ignore obsolete messages
// HS messages are cached by hs component itself
// processItNow must be called in a concurrent-safe environment
func (n *Node) processItNow(msgHeight int, msg interface{}) bool {
	if msgHeight <= n.committedHeight {
		// if receiving an obsolete message, ignore it
		return false
	}

	if _, ok := n.smvbaMap[msgHeight]; !ok {
		n.cachedMsgs[msgHeight] = append(n.cachedMsgs[msgHeight], msg)
		return false
	}

	return true
}

// restoreMessages process the cached messages from cachedMsgs
// restoreMessages must be called in a concurrent-safe environment
func (n *Node) restoreMessages(height int) {
	if _, ok := n.cachedMsgs[height]; !ok {
		return
	}
	msgs := n.cachedMsgs[height]
	for _, msg := range msgs {
		switch msgAsserted := msg.(type) {
		case ABABvalRequestMsg:
			go n.abaMap[height].handleBvalRequest(&msgAsserted)
		case ABAAuxRequestMsg:
			go n.abaMap[height].handleAuxRequest(&msgAsserted)
		case ABAExitMsg:
			go n.abaMap[height].handleExitMessage(&msgAsserted)
		case SMVBAPBVALMessage:
			go n.smvbaMap[height].spb.processPBVALMsg(&msgAsserted)
		case SMVBAPBVOTMessage:
			go n.smvbaMap[height].spb.processPBVOTMsg(&msgAsserted)
		case SMVBAFinishMessage:
			go n.smvbaMap[height].HandleFinishMsg(&msgAsserted)
		case SMVBADoneShareMessage:
			go n.smvbaMap[height].HandleDoneShareMsg(&msgAsserted)
		case SMVBAPreVoteMessage:
			go n.smvbaMap[height].HandlePreVoteMsg(&msgAsserted)
		case SMVBAVoteMessage:
			go n.smvbaMap[height].HandleVoteMsg(&msgAsserted)
		case SMVBAHaltMessage:
			go n.smvbaMap[height].HandleHaltMsg(&msgAsserted)
			break
		}
	}
	delete(n.cachedMsgs, height)
}

// EstablishP2PConns establishes P2P connections with other nodes.
func (n *Node) EstablishP2PConns() error {
	if n.trans == nil {
		return errors.New("networktransport has not been created")
	}
	for name, addr := range n.Id2AddrMap {
		addrWithPort := addr + ":" + n.Id2PortMap[name]
		conn, err := n.trans.GetConn(addrWithPort)
		if err != nil {
			return err
		}
		n.trans.ReturnConn(conn)
		n.logger.Debug("connection has been established", "sender", n.Name, "receiver", addr)
	}
	return nil
}

// SendMsg sends a message to another peer identified by the addrPort (e.g., 127.0.0.1:7788)
func (n *Node) SendMsg(tag byte, data interface{}, sig []byte, addrPort string) error {
	c, err := n.trans.GetConn(addrPort)
	if err != nil {
		return err
	}
	if err := conn.SendMsg(c, tag, data, sig); err != nil {
		return err
	}

	if err = n.trans.ReturnConn(c); err != nil {
		return err
	}
	return nil
}

// PlainBroadcast broadcasts data in its best effort
func (n *Node) PlainBroadcast(tag byte, data interface{}, sig []byte) error {
	for id, addr := range n.Id2AddrMap {
		port := n.Id2PortMap[id]
		addrPort := addr + ":" + port
		err := n.SendMsg(tag, data, sig, addrPort)
		if err != nil {
			return err
		}
	}
	return nil
}
