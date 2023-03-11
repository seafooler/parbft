package core

import (
	"errors"
)

type SPB struct {
	s *SMVBA

	// Although the node can process messages from different views at the same time,
	// each node will only start the SPB instance one by one.
	// Therefore, each time the function of PBBroadcastData is called, the PB variable should be reinitialized
	pb1, pb2 *PB
}

func NewSPB(s *SMVBA) *SPB {
	spb := &SPB{
		s: s,
	}

	spb.pb1 = NewPB(spb, 1)
	spb.pb2 = NewPB(spb, 2)

	return spb

}

func (spb *SPB) SPBBroadcastData(rawData [][HASHSIZE]byte, proof []byte, txCount, view int) (chan SMVBAQCedData, error) {
	// Invoke the 1st PB
	if err := spb.pb1.PBBroadcastData(rawData, proof, txCount, view, 1); err != nil {
		return nil, err
	}

	// TODO: there may be a bug here, what if qcdChan1 never outputs
	// Fix: never mind, the node will stop this SPB once receiving the stopCh in node.RunOneMVBAView() method
	outputFrom1PB := <-spb.pb1.pbOutputCh

	spb.s.logger.Debug("receive message from pbOutputCh", "replica", spb.s.node.Name,
		"outputFrom1PB", outputFrom1PB)

	// Invoke the 2nd PB
	if err := spb.pb2.PBBroadcastData(rawData, outputFrom1PB.QC, outputFrom1PB.TxCount, view, 2); err != nil {
		return nil, err
	}

	return spb.pb2.pbOutputCh, nil
}

func (spb *SPB) processPBVALMsg(valMsg *SMVBAPBVALMessage) error {
	spb.s.logger.Debug("processPBVALMsg is called", "replica", spb.s.node.Name,
		"msg.Height", valMsg.Height, "view", valMsg.View, "Dealer", valMsg.Dealer)

	// Check if output has been decided
	// This check need not wrapping with a lock, since it is used to accelerate stopping without sacrificing safety
	if spb.s.output != nil {
		spb.s.logger.Debug("handlePBVALMsg is called, but output has been decided", "replica", spb.s.node.Name,
			"output", string(spb.s.output))
		return nil
	}

	spb.s.Lock()
	defer spb.s.Unlock()

	// Check if the SPBs in this view are abandoned
	if spb.s.abandon[valMsg.View] {
		return nil
	}

	// If VotMsg's sn is larger than node's, deals with it as usual

	switch valMsg.Phase {
	case 1:
		spb.pb1.handlePBVALMsg(valMsg)
	case 2:
		if _, ok := spb.s.lockMessageMap[valMsg.View]; !ok {
			spb.s.lockMessageMap[valMsg.View] = make(map[string]*SMVBAPBVALMessage)
		}
		spb.s.lockMessageMap[valMsg.View][valMsg.Dealer] = valMsg
		spb.pb2.handlePBVALMsg(valMsg)
	default:
		errStr := "unknown phase of the received val message"
		spb.s.logger.Error(errStr)
		return errors.New(errStr)
	}
	return nil
}

func (spb *SPB) processPBVOTMsg(votMsg *SMVBAPBVOTMessage) error {
	spb.s.logger.Debug("processPBVOTMsg is called", "replica", spb.s.node.Name,
		"msg.Height", votMsg.Height, "view", votMsg.View, "Dealer", votMsg.Dealer)

	if spb.s.output != nil {
		spb.s.logger.Debug("processPBVOTMsg is called, but output has been decided", "replica", spb.s.node.Name,
			"output", string(spb.s.output))
		return nil
	}

	spb.s.Lock()
	defer spb.s.Unlock()

	if spb.s.abandon[votMsg.View] {
		return nil
	}

	// VotMsg's sn will never be larger than node's, because SPB is initiated by itself
	// To be more specific, if a votMsg with sn1 is received, it means an SPB with sn1 has been initiated
	// and the node must be at least with sn1

	switch votMsg.Phase {
	case 1:
		go spb.pb1.handlePBVOTMsg(votMsg)
	case 2:
		go spb.pb2.handlePBVOTMsg(votMsg)
	default:
		errStr := "unknown phase of the received vot message"
		spb.s.logger.Error(errStr)
		return errors.New(errStr)
	}
	return nil
}
