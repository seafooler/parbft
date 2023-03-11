/**
Note that PB-related messages will not be cached
If a PB-related message is obsolete, either from a previous sn or a previous view, it will be omitted in the call
of processPBVALMsg or processPBVOTMsg;
If a PB-related message comes from the future, either from a future sn or a future view, it will be processed as well.
*/

package core

import (
	"github.com/seafooler/sign_tools"
	"sync"
)

type PB struct {
	spb *SPB

	id uint8

	pbOutputCh chan SMVBAQCedData

	dataToPB    [][HASHSIZE]byte
	partialSigs [][]byte
	dataHash    []byte

	mux sync.RWMutex
}

func NewPB(s *SPB, id uint8) *PB {
	return &PB{
		spb: s,
		id:  id,
	}
}

func (pb *PB) PBBroadcastData(data [][HASHSIZE]byte, proof []byte, txCount, view int, phase uint8) error {
	qcdChan := make(chan SMVBAQCedData)

	pb.mux.Lock()
	defer pb.mux.Unlock()

	// Although the node can process messages from different views at the same time,
	// each node will only start the SPB instance one by one.
	// Therefore, each time the function of PBBroadcastData is called, the PB variable should be reinitialized
	pb.pbOutputCh = qcdChan     // Each time a data is broadcast via pb, update the channel
	pb.partialSigs = [][]byte{} // Each time a data is broadcast via pb, clean partial signature slices
	pb.dataToPB = data          // Each time a data is broadcast via pb, update the dataToPB

	valMsg := SMVBAPBVALMessage{
		Height:  pb.spb.s.height,
		TxCount: txCount,
		Data:    data,
		Proof:   proof,
		Dealer:  pb.spb.s.node.Name,
		SMVBAViewPhase: SMVBAViewPhase{
			View:  view,
			Phase: phase,
		},
	}

	if err := pb.spb.s.node.PlainBroadcast(SMVBAPBValTag, valMsg, nil); err != nil {
		return err
	}

	return nil
}

func (pb *PB) handlePBVALMsg(valMsg *SMVBAPBVALMessage) error {
	pb.spb.s.logger.Debug("handlePBVALMsg is called", "replica", pb.spb.s.node.Name,
		"height", valMsg.Height, "view", valMsg.View, "pbid", valMsg.Phase, "Dealer", valMsg.Dealer)

	dealerID := pb.spb.s.node.Name2IdMap[valMsg.Dealer]
	addrPort := pb.spb.s.node.Id2AddrMap[dealerID] + ":" + pb.spb.s.node.Id2PortMap[dealerID]

	// TODO: should sign over the data plus SNView rather than the only data
	// TODO: simply use the first hash
	//hash, err := genMsgHashSum(valMsg.Data[0][:])
	hash, err := genMsgHashSum([]byte("mock data"))
	if err != nil {
		return err
	}
	pb.dataHash = hash
	partialSig := sign_tools.SignTSPartial(pb.spb.s.node.PriKeyTS, hash)
	votMsg := SMVBAPBVOTMessage{
		Height:         valMsg.Height,
		TxCount:        valMsg.TxCount,
		PartialSig:     partialSig,
		Dealer:         valMsg.Dealer,
		Sender:         pb.spb.s.node.Name,
		SMVBAViewPhase: valMsg.SMVBAViewPhase,
		Hash:           hash,
	}

	go pb.spb.s.node.SendMsg(SMVBAPBVoteTag, votMsg, nil, addrPort)
	return nil
}

func (pb *PB) handlePBVOTMsg(votMsg *SMVBAPBVOTMessage) error {
	pb.spb.s.node.logger.Debug("HandlePBVOTMsg is called", "replica", pb.spb.s.node.Name,
		"Height", pb.spb.s.height, "node.view", pb.spb.s.view, "msg.Height", votMsg.Height, "view", votMsg.View,
		"id", pb.id, "sender", votMsg.Sender, "hash", string(votMsg.Hash))
	pb.mux.Lock()
	defer pb.mux.Unlock()

	// Check if the SPBs in this view are abandoned
	if pb.spb.s.abandon[votMsg.View] {
		return nil
	}

	pb.partialSigs = append(pb.partialSigs, votMsg.PartialSig)

	if len(pb.partialSigs) == pb.spb.s.node.N-pb.spb.s.node.F {
		partialSigs := pb.partialSigs
		//fmt.Println("pb.partialSigs:", partialSigs, "votMsg: ", string(votMsg.Data))
		intactSig := sign_tools.AssembleIntactTSPartial(partialSigs, pb.spb.s.node.PubKeyTS,
			votMsg.Hash, pb.spb.s.node.N-pb.spb.s.node.F, pb.spb.s.node.N)

		qcedData := SMVBAQCedData{
			Height:         votMsg.Height,
			TxCount:        votMsg.TxCount,
			Hash:           pb.dataHash,
			QC:             intactSig,
			SMVBAViewPhase: votMsg.SMVBAViewPhase,
		}

		pb.pbOutputCh <- qcedData
	}

	return nil
}
