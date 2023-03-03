package core

import (
	"github.com/hashicorp/go-hclog"
	"sync"
)

type HS struct {
	node *Node

	hLogger    hclog.Logger
	LeaderId   int
	ProofReady chan *ProofData

	cachedVoteMsgs       map[int]map[int][]byte
	cachedBlockProposals map[int]*HSProposalMsg
	cachedHeight         map[int]bool

	sync.Mutex
}

func NewHS(n *Node, leader int) *HS {
	return &HS{
		node: n,
		hLogger: hclog.New(&hclog.LoggerOptions{
			Name:   "parbft-hs",
			Output: hclog.DefaultOutput,
			Level:  hclog.Level(n.Config.LogLevel),
		}),
		LeaderId: leader,

		ProofReady:           make(chan *ProofData),
		cachedVoteMsgs:       make(map[int]map[int][]byte),
		cachedBlockProposals: make(map[int]*HSProposalMsg),
		cachedHeight:         make(map[int]bool),
	}
}

// BroadcastProposalProof broadcasts the new block and proof of previous block through the ProposalMsg
func (h *HS) BroadcastProposalProof(blk *Block) error {
	if h.LeaderId != h.node.Id {
		return nil
	}
	pr := <-h.ProofReady
	if pr.Height != blk.Height-1 {
		h.hLogger.Error("Height of proof is incorrect", "pr.Height", pr.Height, "blk.Height", blk.Height)
	}
	proposalMsg := HSProposalMsg{
		Block: *blk,
		Proof: pr.Proof,
	}
	if err := h.node.PlainBroadcast(HSProposalMsgTag, proposalMsg, nil); err != nil {
		return err
	}
	h.hLogger.Info("successfully broadcast a new proposal and proof",
		"height", blk.Height)
	return nil
}

// ProcessHSProposalMsg votes for the current proposal and commits the previous-previous block
func (h *HS) ProcessHSProposalMsg(pm *HSProposalMsg) error {
	h.hLogger.Debug("Process the HS Proposal Message", "block_index", pm.Height)
	h.Lock()
	defer h.Unlock()
	h.cachedHeight[pm.Height] = true
	h.cachedBlockProposals[pm.Height] = pm
	// do not retrieve the previous block nor verify the proof for the 0th block
	// try to cache a previous block
	h.tryCache(pm.Height, pm.Proof)

	// if there is already a subsequent block, deal with it
	if _, ok := h.cachedHeight[pm.Height+1]; ok {
		h.tryCache(pm.Height+1, h.cachedBlockProposals[pm.Height+1].Proof)
	}

	go func() {
		h.node.readyData <- ReadyData{
			ComponentId: 0,
			TxCount:     pm.TxNum,
			Height:      pm.Height,
		}
	}()

	// send the ts share of new block
	if err := h.sendVote(pm); err != nil {
		return err
	}

	if h.LeaderId == h.node.Id {
		return h.tryAssembleProof(pm.Height)
	} else {
		return nil
	}
}

// tryCache must be wrapped in a lock
func (h *HS) tryCache(height int, proof []byte) error {
	// retrieve the previous block
	pBlk, ok := h.cachedBlockProposals[height-1]
	if !ok {
		// Fixed: Todo: deal with the situation where the previous block has not been cached
		h.hLogger.Info("did not cache the previous block", "prev_block_index", height-1)
		// This is not a bug, maybe a previous block has not been cached
		return nil
	}

	//// verify the proof
	//blockBytes, err := encode(pBlk.Block)
	//if err != nil {
	//	h.hLogger.Error("fail to encode the block", "block_index", height)
	//	return err
	//}
	//
	//if _, err := sign_tools.VerifyTS(h.node.PubKeyTS, blockBytes, proof); err != nil {
	//	h.hLogger.Error("fail to verify proof of a previous block", "prev_block_index", pBlk.Height)
	//	return err
	//}

	delete(h.cachedHeight, pBlk.Height)

	// if there is already a subsequent block, deal with it
	if _, ok := h.cachedHeight[height+1]; ok {
		h.tryCache(height+1, h.cachedBlockProposals[height+1].Proof)
	}

	return nil
}

// send vote to the leader
func (h *HS) sendVote(pm *HSProposalMsg) error {
	// create the ts share of new block
	//blockBytes, err := encode(pm.Block)
	//if err != nil {
	//	h.hLogger.Error("fail to encode the block", "block_index", pm.Height)
	//	return err
	//}
	//share := sign_tools.SignTSPartial(h.node.PriKeyTS, blockBytes)

	// send the ts share to the leader
	hsVoteMsg := HSVoteMsg{
		Share:  nil,
		Height: pm.Height,
		Voter:  h.node.Id,
	}
	leaderAddrPort := h.node.Id2AddrMap[h.LeaderId] + ":" + h.node.Id2PortMap[h.LeaderId]
	err := h.node.SendMsg(HSVoteMsgTag, hsVoteMsg, nil, leaderAddrPort)
	if err != nil {
		h.hLogger.Error("fail to vote for the block", "block_index", pm.Height)
		return err
	} else {
		h.hLogger.Debug("successfully vote for the block", "block_index", pm.Height)
	}
	return nil
}

// ProcessHSVoteMsg stores the vote messages and attempts to create the ts proof
func (h *HS) ProcessHSVoteMsg(vm *HSVoteMsg) error {
	h.hLogger.Debug("Process the Hs Vote Message", "block_index", vm.Height)
	h.Lock()
	defer h.Unlock()
	if _, ok := h.cachedVoteMsgs[vm.Height]; !ok {
		h.cachedVoteMsgs[vm.Height] = make(map[int][]byte)
	}
	h.cachedVoteMsgs[vm.Height][vm.Voter] = vm.Share
	return h.tryAssembleProof(vm.Height)
}

// tryAssembleProof must be wrapped in a lock
// tryAssembleProof may be called by ProcessHsVoteMsg() or ProcessHsProposalMsg()
func (h *HS) tryAssembleProof(height int) error {
	if len(h.cachedVoteMsgs[height]) == h.node.N-h.node.F {
		shares := make([][]byte, h.node.N-h.node.F)
		i := 0
		for _, share := range h.cachedVoteMsgs[height] {
			shares[i] = share
			i++
		}

		//cBlk, ok := h.cachedBlockProposals[height]
		//if !ok {
		//	h.hLogger.Debug("cachedBlocks does not contain the block", "h.cachedBlocks", h.cachedBlockProposals,
		//		"vm.Height", height)
		//	// This is not an error, since HsProposalMsg may be delivered later
		//	return nil
		//}
		//
		//blockBytes, err := encode(cBlk.Block)
		//if err != nil {
		//	h.hLogger.Error("fail to encode the block", "block_index", height)
		//	return err
		//}
		//proof := sign_tools.AssembleIntactTSPartial(shares, h.node.PubKeyTS, blockBytes, h.node.N-h.node.F, h.node.N)

		go func() {
			h.ProofReady <- &ProofData{
				Proof:  nil,
				Height: height,
			}
		}()
	}
	return nil
}
