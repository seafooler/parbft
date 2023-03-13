package core

import (
	"encoding/binary"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/seafooler/sign_tools"
	"sync"
)

type SMVBA struct {
	node *Node

	height int

	sync.Mutex

	view int

	spb *SPB

	logger hclog.Logger

	inProcessProof []byte
	inProcessData  [][HASHSIZE]byte
	inProcessTxNum int

	finishMessagesMap map[int]map[string]*SMVBAFinishMessage
	lockMessageMap    map[int]map[string]*SMVBAPBVALMessage

	// indicate if the done/elect message has been sent
	doneMessageMap  map[int]map[string]*SMVBADoneShareMessage
	doneMessageSent map[int]bool

	output   []byte
	outputCh chan int

	stopCh chan int

	abandon map[int]bool
	coin    map[int]int

	preVoteSent map[int]bool
	voteSent    map[int]bool

	preVoteMessageMap map[int]map[string]*SMVBAPreVoteMessage
	voteMessageMap    map[int]map[string]*SMVBAVoteMessage

	readyViewDataMap map[int]*SMVBAReadyViewData

	payLoadHashesMap map[string][][HASHSIZE]byte
}

func NewSMVBA(n *Node, height int) *SMVBA {
	sMVBA := &SMVBA{
		node:   n,
		height: height,

		view: -1,

		finishMessagesMap: make(map[int]map[string]*SMVBAFinishMessage),
		doneMessageMap:    make(map[int]map[string]*SMVBADoneShareMessage),
		doneMessageSent:   make(map[int]bool),
		lockMessageMap:    make(map[int]map[string]*SMVBAPBVALMessage),
		preVoteMessageMap: make(map[int]map[string]*SMVBAPreVoteMessage),
		voteMessageMap:    make(map[int]map[string]*SMVBAVoteMessage),
		preVoteSent:       make(map[int]bool),
		voteSent:          make(map[int]bool),
		abandon:           make(map[int]bool),
		readyViewDataMap:  make(map[int]*SMVBAReadyViewData),
		outputCh:          make(chan int),
		stopCh:            make(chan int),
		payLoadHashesMap:  make(map[string][][HASHSIZE]byte),

		coin: make(map[int]int),
	}

	sMVBA.logger = hclog.New(&hclog.LoggerOptions{
		Name:   "parbft-smvba",
		Output: hclog.DefaultOutput,
		Level:  hclog.Level(n.LogLevel),
	})

	sMVBA.spb = NewSPB(sMVBA)

	return sMVBA
}

// OutputCh returns the outputCh variable
func (s *SMVBA) OutputCh() chan int {
	return s.outputCh
}

// Output returns the output variable
func (s *SMVBA) Output() []byte {
	return s.output
}

// BroadcastViaSPB encapsulates the inner functions of calling SPB
func (s *SMVBA) BroadcastViaSPB(data [][HASHSIZE]byte, proof []byte, view int) (chan SMVBAQCedData, error) {
	return s.spb.SPBBroadcastData(data, proof, view)
}

// VerifyTS verifies the data's threshold signature
func (s *SMVBA) VerifyTS(data, tsSig []byte) (bool, error) {
	return sign_tools.VerifyTS(s.node.PubKeyTS, data, tsSig)
}

// HandleFinishMsg processes the finish messages
func (s *SMVBA) HandleFinishMsg(fMsg *SMVBAFinishMessage) {
	s.logger.Debug("HandleFinishMsg is called", "replica", s.node.Name,
		"height", fMsg.Height, "view", fMsg.View, "dealer", fMsg.Dealer)

	if s.output != nil {
		s.logger.Debug("HandleFinishMsg is called, but output has been decided", "replica", s.node.Name,
			"output", string(s.output))
		return
	}

	// Check if the Finish message is obsolete, the first check is used to accelerate the return from obsolete/future messages
	if fMsg.View < s.view {
		s.logger.Debug("Receive an obsolete Finish message", "replica", s.node.Name, "s.Height", s.height,
			"node.view", s.view, "msg.height", fMsg.Height, "msg.view", fMsg.View)
		// No need to deal with an obsolete Finish message, just return
		// The reason is that if it is obsolete, the replica must have entered the next view successfully
		return
	}

	// doneShareMessage's Height&View is assigned based on fMsg's Height&View, so no mutex is needed here
	coinShare := sign_tools.SignTSPartial(s.node.PriKeyTS, []byte(fmt.Sprintf("%s%v%v",
		msgTagNameMap[SMVBADoneShareTag], fMsg.Height, fMsg.View)))
	doneShareMsg := SMVBADoneShareMessage{
		Height:  fMsg.Height,
		TSShare: coinShare,
		Sender:  s.node.Name,
		Data:    fMsg.Data,
		View:    fMsg.View,
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.finishMessagesMap[fMsg.View]; !ok {
		s.finishMessagesMap[fMsg.View] = make(map[string]*SMVBAFinishMessage)
	}
	s.finishMessagesMap[fMsg.View][fMsg.Dealer] = fMsg
	s.logger.Debug("FinishMessageMap inserted", "replica", s.node.Name, "dealer", fMsg.Dealer)
	if len(s.finishMessagesMap[fMsg.View]) == 2*s.node.F+1 {
		if !s.doneMessageSent[fMsg.View] {
			s.doneMessageSent[fMsg.View] = true
			go func() {
				if err := s.node.PlainBroadcast(SMVBADoneShareTag, doneShareMsg, nil); err != nil {
					s.logger.Error(err.Error())
				}
			}()
		}
		// If coin is already revealed, it indicates that it is waiting for 2f+1 Finish messages
		if coin, ok := s.coin[fMsg.View]; ok {
			// Both 2f+1 Finish messages and 2f+1 Done messages are collected, abandon all the SPBs corresponding to msg.View
			s.abandon[fMsg.View] = true
			s.logger.Debug("Abandon all the SPBs", "replica", s.node.Name, "s.Height", s.height, "view", s.view,
				"fMsg.View", fMsg.View)
			coin2NodeName := s.node.Id2NameMap[coin]
			s.HaltOrPreVote(fMsg.Height, fMsg.View, fMsg.Data, coin2NodeName)
		}
	}
}

func (s *SMVBA) RunOneMVBAView(usePrevData bool, data [][HASHSIZE]byte, proof []byte, txCount, v int) error {
	s.logger.Debug("RunOneMVBAView is called", "replica", s.node.Name, "Height", s.height, "view", v)
	s.Lock()

	if s.output != nil {
		s.logger.Debug("RunOneMVBAView is called, but output has been decided", "replica", s.node.Name,
			"output", string(s.output))
		s.Unlock()
		return nil
	}

	// check if the previous view is currently processed
	if v < s.view {
		s.logger.Debug("RunOneMVBAView with an obsolete call", "replica", s.node.Name, "s.Height", s.height,
			"view", s.view, "call.v", v)
		s.Unlock()
		return nil
	} else if v > s.view {
		s.logger.Debug("RunOneMVBAView receives a future call", "replica", s.node.Name, "s.Height", s.height,
			"view", s.view, "call.v", v)
		// cache the call
		s.readyViewDataMap[s.view] = &SMVBAReadyViewData{
			usePrevData: usePrevData,
			txCount:     txCount,
			data:        data,
			proof:       proof,
		}
		s.Unlock()
		return nil
	}

	s.logger.Debug("RunOneMVBAView with a correct Height&View", "replica", s.node.Name, "s.Height", s.height,
		"s.view", s.view)

	s.AdvanceView()
	if !usePrevData {
		s.inProcessData = data
		s.inProcessTxNum = txCount
	}
	s.inProcessProof = proof
	s.Unlock()

	// Check if there is a cached readyViewData
	if rvd, ok := s.readyViewDataMap[s.view]; ok {
		s.logger.Debug("Retrieve a cached readyViewData and run the next view directly", "replica", s.node.Name,
			"s.Height", s.height, "s.view", s.view)
		go s.RunOneMVBAView(rvd.usePrevData, rvd.data, rvd.proof, rvd.txCount, s.view)
		return nil
	}

	// Phase 1: SPB phase
	s.logger.Debug("Run a new view by calling the SPB", "replica", s.node.Name, "s.Height", s.height,
		"view", s.view, "data", s.inProcessData, "usePrevData", usePrevData)
	qcedDataCh, err := s.BroadcastViaSPB(s.inProcessData, s.inProcessProof, s.view)
	if err != nil {
		return err
	}

	// Phase 2: Broadcast finish messages
	s.logger.Debug("Return from SPB broadcast", "replica", s.node.Name, "s.Height", s.height,
		"view", s.view)
	// TODO: there may be a bug here, what if qcedDataCh never outputs
	// Fix: if it receives a signal from the stopCh, it will return
	var qcedData SMVBAQCedData
	select {
	case qcedData = <-qcedDataCh:
	case <-s.stopCh:
		s.logger.Debug("This round of MVBA is stopped", "replica", s.node.Name, "s.Height", s.height,
			"view", s.view)
		return nil
	}

	s.logger.Debug("Receive from qcedDataCh", "replica", s.node.Name, "s.Height", s.height,
		"view", s.view)

	finishMsg := SMVBAFinishMessage{
		Height: qcedData.Height,
		Data:   qcedData.Data,
		Hash:   qcedData.Hash,
		QC:     qcedData.QC,
		Dealer: s.node.Name,
		View:   s.view,
	}

	s.logger.Debug("finishMsg's Data", "replica", s.node.Name,
		"finishMsg.Hash", string(finishMsg.Hash))

	if err := s.node.PlainBroadcast(SMVBAFinishTag, finishMsg, nil); err != nil {
		return err
	}

	return nil
}

func (s *SMVBA) AdvanceView() {
	s.logger.Debug("+++++++++++++++ advance to the next view", "s.Height", s.height, "replica", s.node.Name,
		"next-view", s.view+1)
	s.view = s.view + 1

}

func (s *SMVBA) HandleDoneShareMsg(msg *SMVBADoneShareMessage) {
	s.logger.Debug("HandleDoneShareMsg is called", "replica", s.node.Name,
		"s.Height", s.height, "view", msg.View, "sender", msg.Sender)

	if s.output != nil {
		s.logger.Debug("HandleDoneShareMsg is called, but output has been decided", "replica", s.node.Name,
			"output", string(s.output))
		return
	}

	coinShare := sign_tools.SignTSPartial(s.node.PriKeyTS, []byte(fmt.Sprintf("%s%v%v",
		msgTagNameMap[SMVBADoneShareTag], msg.Height, msg.View)))
	doneShareMsg := SMVBADoneShareMessage{
		Height:  msg.Height,
		TSShare: coinShare,
		Sender:  s.node.Name,
		View:    msg.View,
	}

	// store the Done/Share messages
	s.Lock()
	defer s.Unlock()

	if _, ok := s.doneMessageMap[msg.View]; !ok {
		s.doneMessageMap[msg.View] = make(map[string]*SMVBADoneShareMessage)
	}
	s.doneMessageMap[msg.View][msg.Sender] = msg
	// Amplify broadcasting done/share messages
	if len(s.doneMessageMap[msg.View]) == s.node.F+1 && !s.doneMessageSent[msg.View] {
		s.doneMessageSent[msg.View] = true
		go func() {
			if err := s.node.PlainBroadcast(SMVBADoneShareTag, doneShareMsg, nil); err != nil {
				s.logger.Error(err.Error())
			}
		}()
	}

	// if more than 2f+1 Done/Share messages are collected, reveal the coin
	if len(s.doneMessageMap[msg.View]) != 2*s.node.F+1 {
		return
	}
	partialSigs := make([][]byte, 2*s.node.F+1)
	count := 0
	for _, dsMsg := range s.doneMessageMap[msg.View] {
		partialSigs[count] = dsMsg.TSShare
		count++
		if count == 2*s.node.F+1 {
			break
		}
	}
	data := []byte(fmt.Sprintf("%s%v%v", msgTagNameMap[SMVBADoneShareTag], msg.Height, msg.View))
	intactTS := sign_tools.AssembleIntactTSPartial(partialSigs, s.node.PubKeyTS, data, 2*s.node.F+1, s.node.N)
	coin := binary.BigEndian.Uint64(intactTS) % uint64(s.node.N)

	s.coin[msg.View] = int(coin)
	s.logger.Debug("Coin is revealed", "Height", msg.Height, "View", msg.View, "replica", s.node.Name,
		"coin", coin)

	s.logger.Debug("The current finishMessageMap status", "replica", s.node.Name, "finishMessageMap", s.finishMessagesMap)

	// Check if the Finish message corresponding to the coin is received.
	coin2NodeName := s.node.Id2NameMap[int(coin)]

	// Check if more than 2f+1 Finish messages are received
	// TODO: Here, we do not need its own SPB is finished, which may need checking
	if _, ok := s.finishMessagesMap[msg.View]; !ok {
		return
	}
	if len(s.finishMessagesMap[msg.View]) < 2*s.node.F+1 {
		return
	}

	// Both 2f+1 Finish messages and 2f+1 Done messages are collected, abandon all the SPBs corresponding to msg.View
	s.abandon[msg.View] = true
	s.logger.Debug("Abandon all the SPBs", "replica", s.node.Name, "s.Height", s.height, "s.view", s.view, "msg.View", msg.View)

	s.HaltOrPreVote(msg.Height, msg.View, msg.Data, coin2NodeName)
}

// HaltOrPreVote must be called by a wrapped lock
func (s *SMVBA) HaltOrPreVote(h, v int, data [][HASHSIZE]byte, coinNode string) {
	s.logger.Debug("HaltOrPreVote is called", "replica", s.node.Name,
		"s.Height", s.height, "view", v)
	finishMsgByCoin := s.finishMessagesMap[v][coinNode]
	if finishMsgByCoin != nil && s.output == nil {
		// If true, output it
		s.output = finishMsgByCoin.Hash
		s.logger.Debug("Data is output after consensus", "replica", s.node.Name, "s.Height", s.height, "View", v,
			"dealer", finishMsgByCoin.Dealer, "data", string(s.output))
		// broadcast halt messages
		hm := SMVBAHaltMessage{
			Height: h,
			Data:   data,
			Hash:   finishMsgByCoin.Hash,
			Proof:  finishMsgByCoin.QC,
			View:   v,
			Dealer: finishMsgByCoin.Dealer,
		}

		go s.node.PlainBroadcast(SMVBAHaltTag, hm, nil)

		payLoadHashes, ok := s.payLoadHashesMap[coinNode]
		if !ok {
			s.logger.Error("payLoadHashes has not received by the node")
		}

		s.logger.Info("Return from SMVBA", "replica", s.node.Name, "Height", h, "View", v,
			"dealer", finishMsgByCoin.Dealer, "data", string(s.output))
		go s.node.abaMap[hm.Height].inputValue(hm.Height, payLoadHashes, 1)

		// send a signal to suspend this round of MVBA or receiving a signal that MVBA has been suspended
		s.logger.Debug("Before sending a signal to outputCh or receiving a signal from stopCh in HaltOrPreVote",
			"replica", s.node.Name, "Height", h, "View", v)
		select {
		case s.outputCh <- v:
			s.logger.Debug("Successfully send a signal to outputCh in HaltOrPreVote",
				"replica", s.node.Name, "Height", h, "View", v)
		case snC := <-s.stopCh:
			s.logger.Debug("To send a signal to outputCh in HaltOrPreVote, while finding MVBA is stopped",
				"replica", s.node.Name, "sn", snC)
			return
		}

	} else if !s.preVoteSent[v] {
		s.preVoteSent[v] = true
		s.logger.Debug("Broadcast PreVote message", "replica", s.node.Name, "s.Height", s.height, "View", v)
		// No Finish message corresponding to the coin is received, start the view change
		go s.BroadcastPreVote(h, v, data)
	}
}

func (s *SMVBA) updatePayloads(payLoadHashes [][HASHSIZE]byte) int {
	committedCount := 0
	s.node.Lock()
	for _, plHash := range payLoadHashes {
		if _, ok := s.node.payLoads[plHash]; ok {
			delete(s.node.payLoads, plHash)
			committedCount++
		} else {
			if _, existed := s.node.committedPayloads[plHash]; !existed {
				s.node.committedPayloads[plHash] = true
				committedCount++
			}
		}
	}
	s.node.Unlock()
	return committedCount
}

func (s *SMVBA) BroadcastPreVote(h, v int, data [][HASHSIZE]byte) error {
	s.logger.Debug("BroadcastPreVote is called", "replica", s.node.Name,
		"s.Height", s.height, "View", v)
	// Attempt to fetch the lock data corresponding to coin
	coin2NodeName := s.node.Id2NameMap[s.coin[v]] // s.coin[snv.View] will never be nil
	// TODO: bug?
	lockData, ok := s.lockMessageMap[v][coin2NodeName]
	pvm := SMVBAPreVoteMessage{
		Height: h,
		Data:   data,
		Dealer: coin2NodeName,
		Sender: s.node.Name,
		View:   v,
	}
	if ok {
		// lock data exists
		pvm.Flag = true
		pvm.Data = lockData.Data
		pvm.ProofOrPartialSig = lockData.Proof
	} else {
		pvm.Flag = false
		pvm.Hash = nil
		pvm.ProofOrPartialSig = sign_tools.SignTSPartial(s.node.PriKeyTS, []byte(fmt.Sprintf("%s%v%v",
			msgTagNameMap[SMVBAPreVoteTag], h, v)))
	}

	go s.node.PlainBroadcast(SMVBAPreVoteTag, pvm, nil)
	return nil
}

func (s *SMVBA) HandlePreVoteMsg(pvm *SMVBAPreVoteMessage) {
	s.logger.Debug("HandlePreVoteMsg is called", "replica", s.node.Name,
		"s.Height", s.height, "View", pvm.View, "sender", pvm.Sender)

	if s.output != nil {
		s.logger.Debug("HandlePreVoteMsg is called, but output has been decided", "replica", s.node.Name,
			"output", string(s.output))
		return
	}

	coin2NodeName := s.node.Id2NameMap[s.coin[pvm.View]]
	vm := SMVBAVoteMessage{
		Height: pvm.Height,
		Data:   pvm.Data,
		Dealer: coin2NodeName,
		Sender: s.node.Name,
		View:   pvm.View,
	}

	// Check if pvm has a flag of true
	if pvm.Flag {
		// TODO: check the proof of PVM
		vm.Flag = true
		vm.Hash = pvm.Hash
		vm.Proof = pvm.ProofOrPartialSig
		vm.Pho = sign_tools.SignTSPartial(s.node.PriKeyTS, []byte(fmt.Sprintf("%s%v%v%T",
			msgTagNameMap[SMVBAVoteTag], pvm.Height, pvm.View, true)))

		s.Lock()

		if !s.voteSent[pvm.View] {
			s.voteSent[pvm.View] = true
			go s.node.PlainBroadcast(SMVBAVoteTag, vm, nil)
			s.Unlock()
			return
		}
		s.Unlock()
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.preVoteMessageMap[pvm.View]; !ok {
		s.preVoteMessageMap[pvm.View] = make(map[string]*SMVBAPreVoteMessage)
	}
	s.preVoteMessageMap[pvm.View][pvm.Sender] = pvm
	if len(s.preVoteMessageMap[pvm.View]) == 2*s.node.F+1 && !s.voteSent[pvm.View] {
		s.voteSent[pvm.View] = true
		vm.Flag = false
		vm.Hash = nil
		partialSigs := make([][]byte, 2*s.node.F+1)
		count := 0
		for _, msg := range s.preVoteMessageMap[pvm.View] {
			partialSigs[count] = msg.ProofOrPartialSig
			count++
			if count == 2*s.node.F+1 {
				break
			}
		}
		data := []byte(fmt.Sprintf("%s%v%v", msgTagNameMap[SMVBAPreVoteTag], pvm.Height, pvm.View))
		intactTS := sign_tools.AssembleIntactTSPartial(partialSigs, s.node.PubKeyTS,
			data, s.node.N-s.node.F, s.node.N)
		vm.Proof = intactTS
		vm.Pho = sign_tools.SignTSPartial(s.node.PriKeyTS, []byte(fmt.Sprintf("%s%v%v%T",
			msgTagNameMap[SMVBAVoteTag], pvm.Height, pvm.View, false)))
		go s.node.PlainBroadcast(SMVBAVoteTag, vm, nil)
	}
}

func (s *SMVBA) HandleVoteMsg(vm *SMVBAVoteMessage) {
	s.logger.Debug("HandleVoteMsg is called", "replica", s.node.Name,
		"s.Height", s.height, "msg.view", vm.View, "sender", vm.Sender)

	if s.output != nil {
		s.logger.Debug("HandleVoteMsg is called, but output has been decided", "replica", s.node.Name,
			"output", string(s.output))
		return
	}

	s.Lock()
	defer s.Unlock()

	// No need to deal with an obsolete Vote message, just return
	// The reason is that if it is obsolete, the replica must have entered the next view successfully
	if vm.View < s.view {
		return
	}
	if _, ok := s.voteMessageMap[vm.View]; !ok {
		s.voteMessageMap[vm.View] = make(map[string]*SMVBAVoteMessage)
	}
	s.voteMessageMap[vm.View][vm.Sender] = vm
	usePrevData := false
	if len(s.voteMessageMap[vm.View]) == 2*s.node.F+1 {
		trueCount := 0
		falseCount := 0
		partialSigsTrue := make([][]byte, 2*s.node.F+1)
		partialSigsFalse := make([][]byte, 2*s.node.F+1)
		var someVoteMsgTrue *SMVBAVoteMessage
		for _, m := range s.voteMessageMap[vm.View] {
			if m.Flag {
				partialSigsTrue[trueCount] = m.Pho
				trueCount++
				someVoteMsgTrue = m
			} else {
				partialSigsFalse[falseCount] = m.Pho
				falseCount++
			}
		}

		// 2f+1 true votes
		if trueCount == 2*s.node.F+1 && s.output == nil {
			// decide and output it
			s.output = vm.Hash
			s.logger.Debug("Data is output after consensus", "replica", s.node.Name, "Height", vm.Height, "view", vm.View,
				"dealer", vm.Dealer, "data", string(s.output))

			data := []byte(fmt.Sprintf("%s%v%v%T", msgTagNameMap[SMVBAVoteTag], vm.Height, vm.View, true))
			intactTS := sign_tools.AssembleIntactTSPartial(partialSigsTrue, s.node.PubKeyTS,
				data, s.node.N-s.node.F, s.node.N)

			// broadcast halt messages
			hm := SMVBAHaltMessage{
				Height: vm.Height,
				Data:   vm.Data,
				Hash:   vm.Hash,
				Proof:  intactTS,
				View:   vm.View,
				Dealer: vm.Dealer,
			}

			go s.node.PlainBroadcast(SMVBAHaltTag, hm, nil)

			payLoadHashes, ok := s.payLoadHashesMap[vm.Dealer]
			if !ok {
				s.logger.Error("payLoadHashes has not received by the node")
			}

			s.logger.Info("Return from SMVBA", "replica", s.node.Name, "s.Height", s.height,
				"msg.View", vm.View, "dealer", vm.Dealer, "data", string(s.output))
			go s.node.abaMap[hm.Height].inputValue(hm.Height, payLoadHashes, 1)

			// send a signal to suspend this round of MVBA
			s.logger.Debug("Before sending a signal to outputCh or receiving a signal from stopCh in HandleVoteMsg",
				"replica", s.node.Name, "s.Height", s.height, "msg.View", vm.View)
			select {
			case s.outputCh <- vm.View:
				s.logger.Debug("Successfully send a signal to outputCh in HandleVoteMsg",
					"replica", s.node.Name, "s.Height", s.height, "msg.View", vm.View)
			case sn := <-s.stopCh:
				s.logger.Debug("To send signal to outputCh in HandleVoteMsg, while finding MVBA is stopped",
					"replica", s.node.Name, "sn", sn)
				return
			}
			return
		}

		var proofForNewView []byte
		var dataForNewView [][HASHSIZE]byte
		var txCountForNewView int
		if falseCount == 2*s.node.F+1 {
			// 2f+1 false votes
			data := []byte(fmt.Sprintf("%s%v%v%T", msgTagNameMap[SMVBAVoteTag], vm.Height, vm.View, false))
			intactTS := sign_tools.AssembleIntactTSPartial(partialSigsFalse, s.node.PubKeyTS,
				data, s.node.N-s.node.F, s.node.N)
			usePrevData = true
			// dataForNewView = s.inProcessData
			// TODO: proofForNewView is different from the paper description
			proofForNewView = intactTS
		} else {
			dataForNewView = s.payLoadHashesMap[someVoteMsgTrue.Dealer]
			proofForNewView = someVoteMsgTrue.Proof
			txCountForNewView = len(dataForNewView) * s.node.maxNumInPayLoad
		}

		go s.RunOneMVBAView(usePrevData, dataForNewView, proofForNewView, txCountForNewView, vm.View)

	}

}

func (s *SMVBA) HandleHaltMsg(hm *SMVBAHaltMessage) {
	// TODO: check if HandleHaltMsg will be called twice

	s.logger.Debug("HandleHaltMsg is called", "replica", s.node.Name,
		"s.Height", s.height, "msg.View", hm.View, "dealer", hm.Dealer)
	s.Lock()
	defer s.Unlock()
	//TODO: check the proof in HaltMessage

	if s.output == nil {
		s.output = hm.Hash
		s.logger.Debug("Data is output after consensus by receiving a halt message ", "replica", s.node.Name,
			"Height", hm.Height, "node-view", s.view, "dealer", hm.Dealer, "data", string(s.output))
		go s.node.PlainBroadcast(SMVBAHaltTag, *hm, nil)

		payLoadHashes, ok := s.payLoadHashesMap[hm.Dealer]
		if !ok {
			s.logger.Error("payLoadHashes has not received by the node")
		}

		s.logger.Info("Return from SMVBA", "replica", s.node.Name, "s.Height", s.height, "View", s.view,
			"dealer", hm.Dealer, "data", string(s.output))
		go s.node.abaMap[hm.Height].inputValue(hm.Height, payLoadHashes, 1)

		// send a signal to suspend this round of MVBA
		s.logger.Debug("Before sending a signal to outputCh or receiving a signal from stopCh in HandleHaltMsg",
			"replica", s.node.Name, "s.Height", s.height, "msg.View", hm.View)
		select {
		case s.outputCh <- s.view:
			s.logger.Debug("Successfully send a signal to outputCh in HandleHaltMsg",
				"replica", s.node.Name, "s.Height", s.height, "msg.View", hm.View)
		case sn := <-s.stopCh:
			s.logger.Debug("To send signal to outputCh in HandleHaltMsg, while finding MVBA is stopped",
				"replica", s.node.Name, "sn", sn)
			return
		}

	}

}
