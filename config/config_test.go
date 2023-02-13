package config

import (
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
	conf, err := LoadConfig("", "config")
	if err != nil {
		t.Fatal(err)
	}

	checkEqualInt(conf.N, 4, "N", t)
	checkEqualInt(conf.F, 1, "F", t)
	checkEqualInt(conf.Id, 0, "Id", t)
	checkEqualInt(conf.LogLevel, 3, "LogLevel", t)

	checkEqualString(conf.Addr, "8.213.130.208", "Addr", t)
	checkEqualString(conf.Name, "node0", "Name", t)
	checkEqualString(conf.P2pPort, "9000", "P2pPort", t)

	for id, addr := range conf.Id2AddrMap {
		switch id {
		case 0:
			if addr != "8.213.130.208" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 1:
			if addr != "47.74.91.50" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 2:
			if addr != "47.254.33.104" {
				t.Fatal("Id2AddrMap is loaded incorrectly")
			}
		case 3:
			if addr != "47.245.56.147" {
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
		if po != "9000" {
			t.Fatal("Id2PortMap is loaded incorrectly")
		}
	}
}
