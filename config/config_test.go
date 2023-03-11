package config

import (
	"github.com/seafooler/sign_tools"
	"strconv"
	"testing"
)

func checkEqualInt(a, b int, logInfo string, t *testing.T) {
	if a != b {
		t.Fatal(logInfo + " is loaded incorrectly")
	}
}

func checkEqualString(a, b string, logInfo string, t *testing.T) {
	if a != b {
		t.Fatal(logInfo + " is loaded incorrectly")
	}
}

func TestConfigLoad(t *testing.T) {
	conf, err := LoadConfig("", "node0")
	if err != nil {
		t.Fatal(err)
	}

	checkEqualInt(conf.N, 4, "N", t)
	checkEqualInt(conf.F, 1, "F", t)
	checkEqualInt(conf.Id, 0, "Id", t)
	checkEqualInt(conf.LogLevel, 3, "LogLevel", t)

	checkEqualString(conf.Addr, "54.221.157.253", "Addr", t)
	checkEqualString(conf.Name, "node0", "Name", t)
	checkEqualString(conf.P2pPort, "8000", "P2pPort", t)

	for id, addr := range conf.Id2AddrMap {
		switch id {
		case 0:
			if addr != "54.221.157.253" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 1:
			if addr != "13.49.0.28" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 2:
			if addr != "54.250.243.96" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 3:
			if addr != "52.65.226.163" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		default:
			t.Fatal("Id2AddrMap is loaded incorrectly")
		}
	}

	for id, na := range conf.Id2NameMap {
		if na != "node"+strconv.Itoa(id) {
			t.Fatal("Id2NameMap is loaded incorrectly")
		}
	}

	for na, id := range conf.Name2IdMap {
		if na != "node"+strconv.Itoa(id) {
			t.Fatal("Name2IdMap is created incorrectly")
		}
	}

	for _, po := range conf.Id2PortMap {
		if po != "8000" {
			t.Fatal("Id2PortMap is loaded incorrectly")
		}
	}

	dataToSign := []byte("seafooler")
	sig := sign_tools.SignEd25519(conf.PriKeyED, dataToSign)
	if ok, err := sign_tools.VerifySignEd25519(conf.PubKeyED[0], dataToSign, sig); !ok || err != nil {
		t.Fatal("ed25519 is set wrong")
	}

}
