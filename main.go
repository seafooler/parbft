package main

import (
	"github.com/seafooler/parbft/config"
	"github.com/seafooler/parbft/core"
	"time"
)

var conf *config.Config
var err error

func init() {
	conf, err = config.LoadConfig("", "config")
	if err != nil {
		panic(err)
	}
}

func main() {
	//logger := hclog.New(&hclog.LoggerOptions{
	//	Name:   "bdt-main",
	//	Output: hclog.DefaultOutput,
	//	Level:  hclog.Level(conf.LogLevel),
	//})

	node := core.NewNode(conf)
	if err = node.StartP2PListen(); err != nil {
		panic(err)
	}

	// wait for each node to start
	time.Sleep(time.Second * time.Duration(conf.WaitTime))

	if err = node.EstablishP2PConns(); err != nil {
		panic(err)
	}

	go node.HandleMsgsLoop()

	newBlock := &core.Block{
		Reqs:     nil,
		Height:   0,
		Proposer: node.Id,
	}

	if node.Id == node.Hs.LeaderId {
		go func() {
			node.Hs.ProofReady <- &core.ProofData{
				Proof:  nil,
				Height: -1,
			}
		}()
	}

	// launch the optimistic path
	node.LaunchOptimisticPath(newBlock)

	// launch the pessimistic path
	//node.LaunchPessimisticPath(newBlock)

	//node.BroadcastPayLoad()

	for {
		time.Sleep(time.Second)
	}
}
