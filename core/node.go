package core

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/seafooler/parbft/config"
	"github.com/seafooler/parbft/conn"
	"github.com/valyala/gorpc"
	"log"
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

	rpcClientsMap     map[int]*gorpc.Client
	payLoads          map[[HASHSIZE]byte]bool
	committedPayloads map[[HASHSIZE]byte]bool
	proposedPayloads  map[[HASHSIZE]byte]bool
	maxNumInPayLoad   int

	// readyData can be sent from the optimistic path or the final ABA
	ReadyData chan ReadyData

	cachedMsgs map[int][]interface{} // cache the messages arrived in advance

	committedHeight    int
	maxProofedHeight   int
	proofedHeight      map[int][][HASHSIZE]byte
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
		ReadyData:         make(chan ReadyData),
		cachedMsgs:        make(map[int][]interface{}),
		smvbaMap:          map[int]*SMVBA{},
		abaMap:            map[int]*ABA{},

		committedHeight: -1,
		proofedHeight:   make(map[int][][HASHSIZE]byte),
		optPathFinishCh: make(map[int]chan struct{}),

		maxNumInPayLoad:   conf.MaxPayloadSize / conf.TxSize,
		payLoads:          make(map[[HASHSIZE]byte]bool),
		committedPayloads: make(map[[HASHSIZE]byte]bool),
		proposedPayloads:  make(map[[HASHSIZE]byte]bool),
		rpcClientsMap:     make(map[int]*gorpc.Client),
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

func (n *Node) StartListenRPC() {
	gorpc.RegisterType(&PayLoadMsg{})
	s := &gorpc.Server{
		// Accept clients on this TCP address.
		Addr: ":" + n.P2pPortPayload,

		SendBufferSize: 100 * 1024 * 1024,

		RecvBufferSize: 100 * 1024 * 1024,

		Handler: func(clientAddr string, payLoad interface{}) interface{} {
			assertedPayLoad, ok := payLoad.(*PayLoadMsg)
			if !ok {
				panic("message send is not a payload")
			}
			n.logger.Info("before acquiring a lock before in rpc handler, 1111")
			n.Lock()
			n.logger.Info("acquired a lock before in rpc handler, 2222")
			defer n.Unlock()
			if _, ok := n.committedPayloads[assertedPayLoad.Hash]; ok {
				n.logger.Debug("Receive an already committed payload", "sender", assertedPayLoad.Sender, "hash",
					string(assertedPayLoad.Hash[:]))
			} else {
				n.payLoads[assertedPayLoad.Hash] = true
				n.logger.Debug("Receive a payload", "sender", assertedPayLoad.Sender, "hash",
					string(assertedPayLoad.Hash[:]), "payload count", len(n.payLoads))
			}
			n.logger.Info("after releasing a lock before in rpc handler, 3333")
			return nil
		},
	}
	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot start rpc server: %s", err)
	}
}

// BroadcastPayLoad mocks the underlying payload broadcast
func (n *Node) BroadcastPayLoadLoop() {
	gorpc.RegisterType(&PayLoadMsg{})
	payLoadFullTime := float32(n.Config.MaxPayloadSize) / float32(n.Config.TxSize*n.Rate)

	n.logger.Info("payloadFullTime", "s", payLoadFullTime)

	for {
		time.Sleep(time.Duration(payLoadFullTime*1000) * time.Millisecond)
		txNum := int(float32(n.Rate) * payLoadFullTime)
		mockHash, err := genMsgHashSum([]byte(fmt.Sprintf("%d%v", n.Id, time.Now())))
		if err != nil {
			panic(err)
		}
		start := time.Now()
		var buf [HASHSIZE]byte
		copy(buf[0:HASHSIZE], mockHash[:])
		payLoadMsg := PayLoadMsg{
			Sender: n.Name,
			// Mock a hash
			Hash: buf,
			Reqs: make([][]byte, txNum),
		}
		n.logger.Debug("1st step takes", "ms", time.Now().Sub(start).Milliseconds())
		start = time.Now()
		for i := 0; i < txNum; i++ {
			payLoadMsg.Reqs[i] = make([]byte, n.Config.TxSize)
			payLoadMsg.Reqs[i][n.Config.TxSize-1] = '0'
		}
		n.logger.Debug("2nd step takes", "ms", time.Now().Sub(start).Milliseconds())
		n.BroadcastPayLoad(payLoadMsg, buf[:])
		//time.Sleep(time.Millisecond * 100)
	}
}

// BroadcastPayLoad broadcasts the payload in its best effort
func (n *Node) BroadcastPayLoad(data interface{}, hash []byte) error {
	for i, _ := range n.Id2AddrMap {
		go func(id int) {
			start := time.Now()
			if _, err := n.rpcClientsMap[id].Call(data); err != nil {
				panic(err)
			}
			n.logger.Debug("Sending a payload", "ms", time.Now().Sub(start).Milliseconds(), "hash", string(hash))
		}(i)
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
			case PayLoadMsg:
				continue
			case PaceSyncMsg:
				continue
			default:
				n.logger.Error("Unknown type of the received message!")
			}
		case data := <-n.ReadyData:

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

			//n.Lock()
			//payLoadHashes, cnt := n.createBlock()
			//n.Unlock()
			//
			//newBlock := &Block{
			//	TxNum:         cnt * n.maxNumInPayLoad,
			//	PayLoadHashes: payLoadHashes,
			//	Height:        data.Height + 1,
			//	Proposer:      n.Id,
			//}

			sigCh := make(chan struct{}, 2)

			// initialize the pessimistic path
			n.InitializePessimisticPath(data.Height + 1)

			if data.ComponentId == 0 {
				// timer is set as 5\Delta, namely 2.5 timeout
				timer := time.NewTimer(time.Duration(n.Config.Timeout/2*5) * time.Millisecond)
				// update status by the optimistic path
				n.updateStatusByOptimisticData(&data, sigCh, timer)
			} else {
				// For debug
				//if data.Height > n.committedHeight {
				//	// update status by the pessimistic path
				//	n.committedHeight = data.Height
				//	// switch to the next leader
				//	n.Hs.LeaderId = (n.Hs.LeaderId + 1) % n.Config.N
				//	n.logger.Info("commit the block from the final ABA", "replica", n.Name, "block_index", data.Height)
				//}
				n.logger.Info("receive a result from final ABA but do not commit", "replica", n.Name,
					"block_index", data.Height)
				continue
			}

			// continue the optimistic path
			//if newBlock.Height == n.startedHSHeight+1 {
			if _, ok := n.optPathFinishCh[data.Height+1]; ok {
				n.logger.Info("path of next height has been launched", "data.Height+1", data.Height+1)
				continue
			}

			n.logger.Debug("optPathFinishCh is set", "data.Height+1", data.Height+1)
			n.optPathFinishCh[data.Height+1] = sigCh
			n.LaunchOptimisticPath(data.Height + 1)
			//n.startedHSHeight++
			//}

			// timer is set as 5\Delta, namely 2.5 timeout
			timer := time.NewTimer(time.Duration(n.Config.Timeout/2*5) * time.Millisecond)

			// launch the pessimistic path
			go func(t *time.Timer, ch chan struct{}, height int) {
				select {
				case <-ch:
					n.logger.Debug("!!! Receive a channel signal", "height", height)
					return
				case <-t.C:
					n.logger.Debug("pessimistic path is launched", "height", height)
					//n.LaunchPessimisticPath(blk)
					n.logger.Info("before acquiring a lock before creating block in launch pes path, 1111")
					n.Lock()
					n.logger.Info("acquired a lock before creating block in launch pes path, 2222")
					payLoadHashes, cnt := n.createBlock(false)
					n.Unlock()
					n.logger.Info("after releasing a lock before creating block in launch pes path, 3333")
					n.smvbaMap[height].RunOneMVBAView(false,
						payLoadHashes, nil, cnt*n.maxNumInPayLoad, -1)
				}
			}(timer, sigCh, data.Height+1)
		}
	}
}

func (n *Node) LaunchOptimisticPath(height int) {
	go func(h int) {
		if err := n.Hs.BroadcastProposalProof(h); err != nil {
			n.logger.Error("fail to broadcast proposal and proof", "height", h,
				"err", err.Error())
		} else {
			n.logger.Debug("successfully broadcast a new proposal and proof", "height", h)
		}
	}(height)
}

func (n *Node) InitializePessimisticPath(height int) {
	if _, ok := n.smvbaMap[height]; !ok {
		n.smvbaMap[height] = NewSMVBA(n, height)
		if n.abaMap[height] == nil {
			n.abaMap[height] = NewABA(n, height)
		}
		n.startedSMVBAHeight = height
		n.restoreMessages(height)
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
	n.logger.Info("before acquiring a lock in updateStatusByOptimisticData, 1111")
	n.Lock()
	defer n.Unlock()
	n.logger.Info("acquired a lock in updateStatusByOptimisticData, 2222")
	prevHeight := data.Height - 1

	if prevHeight <= n.committedHeight {
		// previous block of optimistic return height has been committed before, maybe by the final agreement
		return
	} else {
		// previous height has not been committed
		n.proofedHeight[prevHeight] = data.PayLoadHashes
		n.maxProofedHeight = prevHeight

		// initialize the ABA
		if n.abaMap[prevHeight] == nil {
			n.abaMap[prevHeight] = NewABA(n, prevHeight)
		}

		// call ABA with the optimistic return
		go func(dat *ReadyData, ch chan struct{}, t *time.Timer) {
			select {
			case <-ch:
				n.logger.Debug("!!! Receive a channel signal before launching ABA", "height", dat.Height)
				return
			case <-t.C:
				pH := dat.Height - 1
				go n.abaMap[pH].inputValue(pH, dat.PayLoadHashes, 0)
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
	n.logger.Info("after releasing a lock in updateStatusByOptimisticData, 3333")
}

// tryCommit must be wrapped in a lock
func (n *Node) tryCommit(height int) error {
	if payLoadHashes, ok := n.proofedHeight[height-2]; ok {
		committedCount := 0
		//n.Lock()
		for _, plHash := range payLoadHashes {
			if _, ok := n.payLoads[plHash]; ok {
				delete(n.payLoads, plHash)
				committedCount++
			} else {
				if _, existed := n.committedPayloads[plHash]; !existed {
					n.committedPayloads[plHash] = true
					committedCount++
				}
			}
		}
		//n.Unlock()
		n.logger.Info("commit the block from the optimistic path", "replica", n.Name, "block_index", height-2,
			"committed_payload_cnt", committedCount, "payload_after_commit", len(n.payLoads))
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

// createBlock must be called in a concurrent context
func (n *Node) createBlock(opt bool) ([][HASHSIZE]byte, int) {
	var payLoadHashes [][HASHSIZE]byte
	payLoadCount := len(n.payLoads)
	if payLoadCount < n.MaxPayloadCount {
		payLoadHashes = make([][HASHSIZE]byte, payLoadCount)
	} else {
		payLoadHashes = make([][HASHSIZE]byte, n.MaxPayloadCount)
	}
	i := 0
	for ph, _ := range n.payLoads {
		if opt {
			if _, ok := n.proposedPayloads[ph]; !ok {
				payLoadHashes[i] = ph
			}
		} else {
			if _, ok := n.committedPayloads[ph]; !ok {
				payLoadHashes[i] = ph
			}
		}
		i++
		if i >= len(payLoadHashes) {
			break
		}
	}
	return payLoadHashes, i - 1
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

func (n *Node) EstablishRPCConns() {
	for name, addr := range n.Id2AddrMap {
		addrWithPort := addr + ":" + n.Id2PortPayLoadMap[name]
		c := &gorpc.Client{
			Addr:           addrWithPort,
			RequestTimeout: 100 * time.Second,
			SendBufferSize: 100 * 1024 * 1024,
			RecvBufferSize: 100 * 1024 * 1024,
		}
		n.rpcClientsMap[name] = c
		c.Start()
	}
}

// SendMsg sends a message to another peer identified by the addrPort (e.g., 127.0.0.1:7788)
func (n *Node) SendMsg(tag byte, data interface{}, sig []byte, addrPort string) error {
	c, err := n.trans.GetConn(addrPort)
	if err != nil {
		return err
	}
	time.Sleep(time.Millisecond * time.Duration(n.Config.MockLatency))
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
