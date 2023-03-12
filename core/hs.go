package core

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"github.com/seafooler/sign_tools"
	"sync"
)

type HS struct {
	node *Node

	hLogger    hclog.Logger
	LeaderId   int
	ProofReady chan *ProofData

	cachedVoteMsgs       map[int]map[int][]byte
	cachedBlockProposals map[int]*HSProposalMsg
	cachedHeight         map[int][][HASHSIZE]byte

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
		cachedHeight:         make(map[int][][HASHSIZE]byte),
	}
}

// BroadcastProposalProof broadcasts the new block and proof of previous block through the ProposalMsg
func (h *HS) BroadcastProposalProof(height int) error {
	//if h.LeaderId != h.node.Id {
	//	return nil
	//}

	// the next leader is (height+1) %n
	if h.node.Id != height%h.node.N {
		h.hLogger.Debug("h.node.Id != height%h.node.N", "h.node.Id", h.node.Id,
			"height%h.node.N", height%h.node.N, "height", height, "h.node.N", h.node.N)
		return nil
	}

	pr := <-h.ProofReady

	if pr.Height != height-1 {
		h.hLogger.Error("Height of proof is incorrect", "pr.Height", pr.Height, "height-1", height-1)
		return errors.New("height of proof is incorrect")
	}

	h.hLogger.Info("before acquiring a lock before creating block, 1111", "pr.Height", pr.Height)
	h.node.Lock()
	h.hLogger.Info("got a lock before creating block, 2222", "pr.Height", pr.Height)
	payLoadHashes, cnt := h.node.createBlock(true)
	h.node.Unlock()
	h.hLogger.Info("after releasing a lock before creating block, 3333", "pr.Height", pr.Height)

	blk := &Block{
		TxNum:         cnt * h.node.maxNumInPayLoad,
		PayLoadHashes: payLoadHashes,
		Height:        pr.Height + 1,
		Proposer:      h.node.Id,
	}

	proposalMsg := HSProposalMsg{
		Block: *blk,
		Proof: pr.Proof,
	}
	if err := h.node.PlainBroadcast(HSProposalMsgTag, proposalMsg, nil); err != nil {
		return err
	}
	h.hLogger.Info("successfully broadcast a new proposal and proof", "txNum", proposalMsg.TxNum,
		"height", blk.Height)
	return nil
}

// ProcessHSProposalMsg votes for the current proposal and commits the previous-previous block
func (h *HS) ProcessHSProposalMsg(pm *HSProposalMsg) error {
	h.hLogger.Info("Process the HS Proposal Message", "block_index", pm.Height)
	h.Lock()
	defer h.Unlock()
	h.cachedHeight[pm.Height] = pm.PayLoadHashes
	h.cachedBlockProposals[pm.Height] = pm
	// do not retrieve the previous block nor verify the proof for the 0th block
	// try to cache a previous block
	h.tryCache(pm.Height, pm.Proof, pm.PayLoadHashes)

	// if there is already a subsequent block, deal with it
	if plHashes, ok := h.cachedHeight[pm.Height+1]; ok {
		h.tryCache(pm.Height+1, h.cachedBlockProposals[pm.Height+1].Proof, plHashes)
	}

	go func() {
		h.node.ReadyData <- ReadyData{
			ComponentId:   0,
			PayLoadHashes: pm.PayLoadHashes,
			Height:        pm.Height,
		}
	}()

	// send the ts share of new block
	if err := h.sendVote(pm); err != nil {
		return err
	}

	if h.node.Id == pm.Height%h.node.N {
		return h.tryAssembleProof(pm.Height)
	} else {
		return nil
	}
}

// tryCache must be wrapped in a lock
func (h *HS) tryCache(height int, proof map[int][]byte, plHashes [][HASHSIZE]byte) error {
	// retrieve the previous block
	pBlk, ok := h.cachedBlockProposals[height-1]
	if !ok {
		// Fixed: Todo: deal with the situation where the previous block has not been cached
		h.hLogger.Info("did not cache the previous block", "prev_block_index", height-1)
		// This is not a bug, maybe a previous block has not been cached
		return nil
	}

	// verify the proof
	blockBytes, err := encode(pBlk.Block)
	if err != nil {
		h.hLogger.Error("fail to encode the block", "block_index", height)
		return err
	}

	if len(proof) < h.node.N-h.node.F {
		h.hLogger.Error("the number of signatures in the proof is not enough", "needed", h.node.N-h.node.F,
			"len(proof)", len(proof))
		return nil
	}

	for i, sig := range proof {
		if _, err := sign_tools.VerifySignEd25519(h.node.PubKeyED[i], blockBytes, sig); err != nil {
			h.hLogger.Error("fail to verify proof of a previous block", "prev_block_index", pBlk.Height)
			return err
		}
	}

	//if _, err := sign_tools.VerifyTS(h.node.PubKeyTS, blockBytes, proof); err != nil {
	//	h.hLogger.Error("fail to verify proof of a previous block", "prev_block_index", pBlk.Height)
	//	return err
	//}

	delete(h.cachedHeight, pBlk.Height)

	h.hLogger.Info("before acquiring a lock in tryCache, 1111")
	h.node.Lock()
	h.hLogger.Info("acquired a lock in tryCache, 2222")
	for _, hx := range plHashes {
		h.node.proposedPayloads[hx] = true
	}
	h.node.Unlock()
	h.hLogger.Info("after releasing acquiring a lock in tryCache, 3333")

	// if there is already a subsequent block, deal with it
	if hashes, ok := h.cachedHeight[height+1]; ok {
		h.tryCache(height+1, h.cachedBlockProposals[height+1].Proof, hashes)
	}

	return nil
}

// send vote to the leader
func (h *HS) sendVote(pm *HSProposalMsg) error {
	// create the normal signature of new block
	blockBytes, err := encode(pm.Block)
	if err != nil {
		h.hLogger.Error("fail to encode the block", "block_index", pm.Height)
		return err
	}

	sig := sign_tools.SignEd25519(h.node.Config.PriKeyED, blockBytes)
	hsVoteMsg := HSVoteMsg{
		EDSig:  sig,
		Height: pm.Height,
		Voter:  h.node.Id,
	}

	// the next leader is (pm.Height+1)%b.node.N
	leaderAddrPort := h.node.Id2AddrMap[(pm.Height+1)%h.node.N] + ":" + h.node.Id2PortMap[(pm.Height+1)%h.node.N]
	err = h.node.SendMsg(HSVoteMsgTag, hsVoteMsg, nil, leaderAddrPort)
	if err != nil {
		h.hLogger.Error("fail to vote for the block", "block_index", pm.Height)
		return err
	} else {
		h.hLogger.Info("successfully vote for the block", "block_index", pm.Height, "addrPort", leaderAddrPort)
	}
	return nil
}

// ProcessHSVoteMsg stores the vote messages and attempts to create the ts proof
func (h *HS) ProcessHSVoteMsg(vm *HSVoteMsg) error {
	h.hLogger.Info("Process the Hs Vote Message", "height", vm.Height, "voter", vm.Voter)
	h.hLogger.Info("before acquiring the HS lock in ProcessHSVoteMsg", "height", vm.Height, "voter", vm.Voter)
	h.Lock()
	h.hLogger.Info("acquired the HS lock in ProcessHSVoteMsg", "height", vm.Height)
	defer h.Unlock()
	if _, ok := h.cachedVoteMsgs[vm.Height]; !ok {
		h.cachedVoteMsgs[vm.Height] = make(map[int][]byte)
	}
	h.cachedVoteMsgs[vm.Height][vm.Voter] = vm.EDSig
	err := h.tryAssembleProof(vm.Height)
	h.hLogger.Info("after releasing the HS lock in ProcessHSVoteMsg", "height", vm.Height, "len(votes)",
		len(h.cachedVoteMsgs[vm.Height]))
	return err
}

// tryAssembleProof must be wrapped in a lock
// tryAssembleProof may be called by ProcessHsVoteMsg() or ProcessHsProposalMsg()
func (h *HS) tryAssembleProof(height int) error {
	if len(h.cachedVoteMsgs[height]) == h.node.N-h.node.F {
		sigs := make(map[int][]byte)
		cnt := 0
		for i, sig := range h.cachedVoteMsgs[height] {
			sigs[i] = sig
			cnt++
			if cnt >= h.node.N-h.node.F {
				break
			}
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
			h.hLogger.Info("Assemble a proof ready data successfully", "height", height)
			h.ProofReady <- &ProofData{
				Proof:  sigs,
				Height: height,
			}
		}()
	}
	return nil
}
