package core

import (
	"testing"
	"time"
)

// Testing ABA should cover all of the following specifications.
//
// 1. If a correct node outputs the value (b), then every good node outputs (b).
// 2. If all good nodes receive input, then every good node outputs a value.
// 3. If any good node outputs value (b), then at least one good node receives (b)
// as input.

func TestFaultyAgreement(t *testing.T) {
	testAgreement(t, []uint8{1, 0, 0, 0}, 7776, true, 0)
}

// Test ABA with 2 false and 2 true nodes, cause binary agreement is not a
// majority vote it guarantees that all good nodes output a least the output of
// one good node. Hence the output should be true for all the nodes.
func TestAgreement2FalseNodes(t *testing.T) {
	testAgreement(t, []uint8{1, 0, 0, 1}, 7876, false, 0)
}

func TestAgreement1FalseNode(t *testing.T) {
	testAgreement(t, []uint8{1, 0, 1, 1}, 7976, true, 1)
}

func TestAgreementGoodNodes(t *testing.T) {
	testAgreement(t, []uint8{1, 1, 1, 1}, 8076, true, 1)
}

// @expected indicates if there is an expected decided value
// @expectValue indicates the expected value
func testAgreement(t *testing.T, inputs []uint8, startPort int, expected bool, expectValue int) {
	num_nodes := len(inputs)
	nodes := Setup(num_nodes, startPort, 3)

	for _, node := range nodes {
		node.smvbaMap[0] = NewSMVBA(node, 0)
		node.abaMap[0] = NewABA(node, 0)
	}

	for i, node := range nodes {
		if err := node.abaMap[0].inputValue(0, 1000, inputs[i]); err != nil {
			t.Fatal(err)
		}

	}

	time.Sleep(time.Second * 5)

}
