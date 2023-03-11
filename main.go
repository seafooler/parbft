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
	//	Name:   "parbft-main",
	//	Output: hclog.DefaultOutput,
	//	Level:  hclog.Level(conf.LogLevel),
	//})

	node := core.NewNode(conf)
	if err = node.StartP2PListen(); err != nil {
		panic(err)
	}

	go node.StartListenRPC()

	// wait for each node to start
	time.Sleep(time.Second * time.Duration(conf.WaitTime))

	if err = node.EstablishP2PConns(); err != nil {
		panic(err)
	}

	node.EstablishRPCConns()

	// Help all the replicas to start simultaneously
	node.BroadcastSyncLaunchMsgs()
	node.WaitForEnoughSyncLaunchMsgs()

	go node.HandleMsgsLoop()

	if node.Id == 0 {
		go func() {
			node.Hs.ProofReady <- &core.ProofData{
				Proof:  nil,
				Height: -1,
			}
		}()
	}

	go func() {
		node.ReadyData <- core.ReadyData{
			ComponentId:   0,
			PayLoadHashes: nil,
			Height:        -1,
		}
	}()

	// launch the optimistic path
	//node.LaunchOptimisticPath(0)

	// launch the pessimistic path
	//node.LaunchPessimisticPath(newBlock)

	for {
		node.BroadcastPayLoadLoop()
	}
}
