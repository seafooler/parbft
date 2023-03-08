package core

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/seafooler/parbft/config"
	"github.com/seafooler/parbft/conn"
	"math/rand"
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
	proofedHeight      map[int]int
	startedSMVBAHeight int
	startedHSHeight    int

	// mock the transactions sent from clients
	lastBlockCreatedTime time.Time
	maxCachedTxs         int

	optPathFinishCh map[int]chan struct{}

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
		proofedHeight:   make(map[int]int),
		maxCachedTxs:    conf.MaxPayloadCount * (conf.MaxPayloadSize / conf.TxSize),
		optPathFinishCh: make(map[int]chan struct{}),
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

// BroadcastPayLoad mocks the underlying payload broadcast
func (n *Node) BroadcastPayLoad() {
	payLoadFullTime := 1000 * float32(n.Config.MaxPayloadSize) / float32(n.Config.TxSize*n.Rate)
	for {
		time.Sleep(time.Duration(payLoadFullTime) * time.Millisecond)
		txNum := int(float32(n.Rate) * payLoadFullTime)
		payLoadMsg := PayLoadMsg{
			Reqs: make([][]byte, txNum),
		}
		for i := 0; i < txNum; i++ {
			payLoadMsg.Reqs[i] = make([]byte, n.Config.TxSize)
			payLoadMsg.Reqs[i][n.Config.TxSize-1] = '0'
		}
		n.PlainBroadcast(PayLoadMsgTag, payLoadMsg, nil)
		time.Sleep(time.Millisecond * 100)
	}
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
			case PayLoadMsg:
				continue
			case PaceSyncMsg:
				continue
			default:
				n.logger.Error("Unknown type of the received message!")
			}
		case data := <-n.readyData:

			if data.ComponentId == 0 && data.Height >= 2 {
				if sigCh, ok := n.optPathFinishCh[data.Height-2]; !ok {
					n.logger.Error("Receive a readydata but it is not broadcast by an optimistic path before",
						"height-2", data.Height-2)
				} else {
					n.logger.Debug("!!! Receive a readydata", "height-2", data.Height-2)
					go func(ch chan struct{}) {
						// two signals
						ch <- struct{}{}
						ch <- struct{}{}
					}(sigCh)

				}
			}

			curTime := time.Now()
			estimatdTxNum := int(curTime.Sub(n.lastBlockCreatedTime).Seconds() * float64(n.Config.Rate))
			if estimatdTxNum > n.maxCachedTxs {
				estimatdTxNum = n.maxCachedTxs
			}

			newBlock := &Block{
				TxNum:    estimatdTxNum,
				Reqs:     nil,
				Height:   data.Height + 1,
				Proposer: n.Id,
			}

			n.lastBlockCreatedTime = curTime

			sigCh := make(chan struct{}, 2)

			// initialize the pessimistic path
			n.InitializePessimisticPath(newBlock)

			if data.ComponentId == 0 {
				// timer is set as 5\Delta, namely 2.5 timeout
				timer := time.NewTimer(time.Duration(n.Config.Timeout/2*5) * time.Millisecond)
				// update status by the optimistic path
				n.updateStatusByOptimisticData(&data, sigCh, timer)
			} else {
				if data.Height > n.committedHeight {
					// update status by the pessimistic path
					n.committedHeight = data.Height
					// switch to the next leader
					n.Hs.LeaderId = (n.Hs.LeaderId + 1) % n.Config.N
					n.logger.Info("commit the block from the final ABA", "replica", n.Name, "block_index", data.Height)
				}
			}

			// continue the optimistic path
			if newBlock.Height == n.startedHSHeight+1 {
				n.optPathFinishCh[newBlock.Height] = sigCh
				n.LaunchOptimisticPath(newBlock)
				n.startedHSHeight++
			}

			// timer is set as 5\Delta, namely 2.5 timeout
			timer := time.NewTimer(time.Duration(n.Config.Timeout/2*5) * time.Millisecond)

			// launch the pessimistic path
			go func(t *time.Timer, ch chan struct{}, blk *Block) {
				select {
				case <-ch:
					n.logger.Debug("!!! Receive a channel signal", "height", blk.Height)
					return
				case <-t.C:
					n.logger.Debug("pessimistic path is launched", "height", blk.Height)
					//n.LaunchPessimisticPath(blk)
					n.smvbaMap[blk.Height].RunOneMVBAView(false,
						[]byte(fmt.Sprintf("%d", blk.Height)), nil, blk.TxNum, -1)
				}
			}(timer, sigCh, newBlock)
		}
	}
}

func (n *Node) LaunchOptimisticPath(blk *Block) {
	go func(blk *Block) {
		time.Sleep(time.Millisecond * time.Duration(n.DDoSDelay))
		if err := n.Hs.BroadcastProposalProof(blk); err != nil {
			n.logger.Error("fail to broadcast proposal and proof", "height", blk.Height,
				"err", err.Error())
		} else {
			n.logger.Debug("successfully broadcast a new proposal and proof", "height", blk.Height)
		}
	}(blk)
}

func (n *Node) InitializePessimisticPath(blk *Block) {
	if _, ok := n.smvbaMap[blk.Height]; !ok {
		n.smvbaMap[blk.Height] = NewSMVBA(n, blk.Height)
		if n.abaMap[blk.Height] == nil {
			n.abaMap[blk.Height] = NewABA(n, blk.Height)
		}
		n.startedSMVBAHeight = blk.Height
		n.restoreMessages(blk.Height)
	}
}

//func (n *Node) LaunchPessimisticPath(blk *Block) {
//	if _, ok := n.smvbaMap[blk.Height]; !ok {
//		n.Lock()
//		defer n.Unlock()
//		n.smvbaMap[blk.Height] = NewSMVBA(n, blk.Height)
//		if n.abaMap[blk.Height] == nil {
//			n.abaMap[blk.Height] = NewABA(n, blk.Height)
//		}
//		n.startedSMVBAHeight = blk.Height
//		n.restoreMessages(blk.Height)
//		n.smvbaMap[blk.Height].RunOneMVBAView(false,
//			[]byte(fmt.Sprintf("%d", blk.Height)), nil, blk.TxNum, -1)
//	}
//}

func (n *Node) updateStatusByOptimisticData(data *ReadyData, ch chan struct{}, t *time.Timer) {
	n.logger.Debug("Update the node status", "replica", n.Name, "data", data)
	n.Lock()
	defer n.Unlock()

	prevHeight := data.Height - 1

	if prevHeight <= n.committedHeight {
		// previous block of optimistic return height has been committed before, maybe by the final agreement
		return
	} else {
		// previous height has not been committed
		n.proofedHeight[prevHeight] = data.TxCount
		n.maxProofedHeight = prevHeight

		// initialize the ABA
		if n.abaMap[prevHeight] == nil {
			n.abaMap[prevHeight] = NewABA(n, prevHeight)
		}

		//call ABA with the optimistic return
		go func(dat *ReadyData, ch chan struct{}, t *time.Timer) {
			select {
			case <-ch:
				n.logger.Debug("!!! Receive a channel signal before launching ABA", "height", dat.Height)
				return
			case <-t.C:
				pH := dat.Height - 1
				go n.abaMap[pH].inputValue(pH, dat.TxCount, 0)
			}
		}(data, ch, t)

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
	if txNum, ok := n.proofedHeight[height-2]; ok {
		n.logger.Info("commit the block from the optimistic path", "replica", n.Name, "block_index", height-2,
			"tx_num", txNum)
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
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Millisecond * time.Duration(n.Config.MockLatency*rand.Intn(100)/100))
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
	for i, a := range n.Id2AddrMap {
		go func(id int, addr string) {
			port := n.Id2PortMap[id]
			addrPort := addr + ":" + port
			if err := n.SendMsg(tag, data, sig, addrPort); err != nil {
				panic(err)
			}
		}(i, a)
	}
	return nil
}

// BroadcastSyncLaunchMsgs sends the PaceSyncMsg to help all the replicas launch simultaneously
func (n *Node) BroadcastSyncLaunchMsgs() error {
	for i, a := range n.Id2AddrMap {
		go func(id int, addr string) {
			port := n.Id2PortMap[id]
			addrPort := addr + ":" + port
			c, err := n.trans.GetConn(addrPort)
			if err != nil {
				panic(err)
			}
			if err := conn.SendMsg(c, PaceSyncMsgTag, PaceSyncMsg{SN: -1, Sender: n.Id, Epoch: -1}, nil); err != nil {
				panic(err)
			}
			if err = n.trans.ReturnConn(c); err != nil {
				panic(err)
			}
		}(i, a)
	}
	return nil
}

func (n *Node) WaitForEnoughSyncLaunchMsgs() error {
	msgCh := n.trans.MsgChan()
	count := 0
	for {
		select {
		case msg := <-msgCh:
			switch msg.(type) {
			case PaceSyncMsg:
				count += 1
				if count >= n.N-3 {
					return nil
				}
			default:
				continue
			}
		}
	}
	return nil
}
