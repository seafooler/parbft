package core

import (
	"bytes"
	"testing"
)

func TestSimpleSinglePB(t *testing.T) {
	nodes := Setup(4, 9006, 3)

	originalData := []byte("seafooler")

	for _, node := range nodes {
		node.smvbaMap[0] = NewSMVBA(node, 0)
	}

	if err := nodes[0].smvbaMap[0].spb.pb1.PBBroadcastData(originalData, nil, 1, 1); err != nil {
		t.Fatal(err)
	}

	data := <-nodes[0].smvbaMap[0].spb.pb1.pbOutputCh

	if !bytes.Equal(originalData, data.Data) {
		t.Fatalf("The QCed data does not equal the original one, original: %s, qced: %s",
			originalData, data.Data[:len(data.Data)-1])
	}

	if ok, err := nodes[0].smvbaMap[0].VerifyTS(data.Data, data.QC); !ok {
		t.Fatal(err)
	}
}
