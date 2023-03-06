package core

import "reflect"

// Message tags to indicate the type of message.
const (
	HSProposalMsgTag uint8 = iota
	HSVoteMsgTag
	ABABvalRequestMsgTag
	ABAExitMsgTag
	ABAAuxRequestMsgTag
	PaceSyncMsgTag
	SMVBAPBValTag
	SMVBAPBVoteTag
	SMVBAFinishTag
	SMVBADoneShareTag
	SMVBAPreVoteTag
	SMVBAVoteTag
	SMVBAHaltTag
	PayLoadMsgTag
)

var msgTagNameMap = map[uint8]string{
	SMVBAPBValTag:     "VAL",
	SMVBAPBVoteTag:    "PBV",
	SMVBAFinishTag:    "FSH",
	SMVBADoneShareTag: "DOS",
	SMVBAPreVoteTag:   "PVT",
	SMVBAVoteTag:      "VOT",
}

type Block struct {
	TxNum    int
	Reqs     []byte
	Height   int
	Proposer int
}

type PayLoadMsg struct {
	Reqs [][]byte
}

type HSProposalMsg struct {
	Block
	Proof []byte
}

type ProofData struct {
	Proof  []byte
	Height int
}

type HSVoteMsg struct {
	Share  []byte
	Height int
	Voter  int
}

type ReadyData struct {
	ComponentId uint8 // 0 represents data being sent from the optimistic path, while 1 represents one from ABA
	TxCount     int
	Height      int
}

// ABABvalRequestMsg holds the input value of the binary input.
type ABABvalRequestMsg struct {
	Height  int
	Sender  int
	TxCount int
	Round   uint32
	BValue  uint8
}

// ABAAuxRequestMsg holds the output value.
type ABAAuxRequestMsg struct {
	Height  int
	TxCount int
	Sender  int
	Round   uint32
	BValue  uint8
	TSPar   []byte
}

// ABAExitMsg indicates that a replica has decided
type ABAExitMsg struct {
	Height  int
	Sender  int
	TxCount int
	Value   int
}

// PaceSyncMsg
type PaceSyncMsg struct {
	SN     int
	Sender int
	Epoch  int
	Proof  []byte
}

type SMVBAViewPhase struct {
	View  int
	Phase uint8 // phase can only be 1 or 2
}

type SMVBAPBVALMessage struct {
	Height  int
	TxCount int
	Data    []byte
	Proof   []byte
	Dealer  string // dealer and sender are the same
	SMVBAViewPhase
}

type SMVBAPBVOTMessage struct {
	Height     int
	TxCount    int
	Data       []byte
	PartialSig []byte
	Dealer     string
	Sender     string
	SMVBAViewPhase
}

type SMVBAQCedData struct {
	Height  int
	TxCount int
	Data    []byte
	QC      []byte
	SMVBAViewPhase
}

type SMVBAFinishMessage struct {
	Height  int
	Data    []byte
	QC      []byte
	Dealer  string
	TxCount int
	View    int
}

type SMVBADoneShareMessage struct {
	Height  int
	TSShare []byte
	Sender  string
	TxCount int
	View    int
}

// SMVBAPreVoteMessage must contain SNView
type SMVBAPreVoteMessage struct {
	Height            int
	TxCount           int
	Flag              bool
	Dealer            string
	Value             []byte
	ProofOrPartialSig []byte // lock proof (sigma_1) or rho_{pn}
	Sender            string

	View int
}

// SMVBAVoteMessage must contain SNView
type SMVBAVoteMessage struct {
	Height  int
	TxCount int
	Flag    bool
	Dealer  string
	Value   []byte
	Proof   []byte // sigma_1 or sigma_{PN}
	Pho     []byte // pho_{2,i} or pho_{vn, i}

	Sender string

	View int
}

type SMVBAHaltMessage struct {
	Height  int
	TxCount int
	Value   []byte
	Proof   []byte
	Dealer  string
	View    int
}

type SMVBAReadyViewData struct {
	Height      int
	txCount     int
	usePrevData bool // indicate if using the previous data
	data        []byte
	proof       []byte
}

type StatusChangeSignal struct {
	Height      int
	Data        []byte
	ComponentID uint8 // 0 indicates the statuschangesignal is sent by HS, 1 indicates sMVBA
	Status      uint8
}

var hpMsg HSProposalMsg
var hvMsg HSVoteMsg
var ababrMsg ABABvalRequestMsg
var abaarMsg ABAAuxRequestMsg
var abaexMsg ABAExitMsg
var psMsg PaceSyncMsg
var smvbaPbValMsg SMVBAPBVALMessage
var smvbaPbVoteMsg SMVBAPBVOTMessage
var smvbaFinishMsg SMVBAFinishMessage
var smvbaDoneShareMsg SMVBADoneShareMessage
var smvbaPreVoteMsg SMVBAPreVoteMessage
var smvbaVoteMsg SMVBAVoteMessage
var smvbaHaltMsg SMVBAHaltMessage
var payLoadMsg PayLoadMsg

var reflectedTypesMap = map[uint8]reflect.Type{
	HSProposalMsgTag:     reflect.TypeOf(hpMsg),
	HSVoteMsgTag:         reflect.TypeOf(hvMsg),
	ABABvalRequestMsgTag: reflect.TypeOf(ababrMsg),
	ABAAuxRequestMsgTag:  reflect.TypeOf(abaarMsg),
	ABAExitMsgTag:        reflect.TypeOf(abaexMsg),
	SMVBAPBValTag:        reflect.TypeOf(smvbaPbValMsg),
	SMVBAPBVoteTag:       reflect.TypeOf(smvbaPbVoteMsg),
	SMVBAFinishTag:       reflect.TypeOf(smvbaFinishMsg),
	SMVBADoneShareTag:    reflect.TypeOf(smvbaDoneShareMsg),
	SMVBAPreVoteTag:      reflect.TypeOf(smvbaPreVoteMsg),
	SMVBAVoteTag:         reflect.TypeOf(smvbaVoteMsg),
	SMVBAHaltTag:         reflect.TypeOf(smvbaHaltMsg),
	PayLoadMsgTag:        reflect.TypeOf(payLoadMsg),
	PaceSyncMsgTag:       reflect.TypeOf(psMsg),
}
