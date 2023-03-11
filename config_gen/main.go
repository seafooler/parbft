package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"github.com/seafooler/sign_tools"
	"github.com/spf13/viper"
	"strconv"
	"strings"
)

func main() {
	viperRead := viper.New()

	// for environment variables
	viperRead.SetEnvPrefix("")
	viperRead.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viperRead.SetEnvKeyReplacer(replacer)

	viperRead.SetConfigName("config_temp")
	viperRead.AddConfigPath("./")

	err := viperRead.ReadInConfig()
	if err != nil {
		panic(err)
	}

	// Deal with id_name as a string map
	idNameMapInterface := viperRead.GetStringMap("id_name")
	nodeNumber := len(idNameMapInterface)
	idNameMap := make(map[int]string, nodeNumber)

	i := 0
	for idAsString, addrAsInterface := range idNameMapInterface {
		if addrAsString, ok := addrAsInterface.(string); ok {
			id, err := strconv.Atoi(idAsString)
			if err != nil {
				panic(err)
			}
			idNameMap[id] = addrAsString
			i++
		} else {
			panic("id_name in the config file cannot be decoded correctly")
		}
	}

	// Deal with id_p2p_port as a string map
	idP2PPortMapInterface := viperRead.GetStringMap("id_p2p_port")
	if nodeNumber != len(idP2PPortMapInterface) {
		panic("id_p2p_port does not match with id_name")
	}
	idP2PPortMap := make(map[int]int, nodeNumber)
	for idAsString, portAsInterface := range idP2PPortMapInterface {
		id, err := strconv.Atoi(idAsString)
		if err != nil {
			panic(err)
		}
		if port, ok := portAsInterface.(int); ok {
			idP2PPortMap[id] = port
		} else {
			panic("id_p2p_port in the config file cannot be decoded correctly")
		}
	}

	// Deal with id_p2p_port as a string map
	idP2PPortPayloadMapInterface := viperRead.GetStringMap("id_p2p_port_payload")
	if nodeNumber != len(idP2PPortPayloadMapInterface) {
		panic("id_p2p_port does not match with id_name")
	}
	idP2PPortPayloadMap := make(map[int]int, nodeNumber)
	for idAsString, portAsInterface := range idP2PPortPayloadMapInterface {
		id, err := strconv.Atoi(idAsString)
		if err != nil {
			panic(err)
		}
		if port, ok := portAsInterface.(int); ok {
			idP2PPortPayloadMap[id] = port
		} else {
			panic("id_p2p_port in the config file cannot be decoded correctly")
		}
	}

	// Deal with id_ips as a string map
	idIPMapInterface := viperRead.GetStringMap("id_ip")
	if nodeNumber != len(idIPMapInterface) {
		panic("id_ips does not match with id_name")
	}
	idIPMap := make(map[int]string, nodeNumber)
	for idAsString, ipAsInterface := range idIPMapInterface {
		if addrAsString, ok := ipAsInterface.(string); ok {
			id, err := strconv.Atoi(idAsString)
			if err != nil {
				panic(err)
			}
			idIPMap[id] = addrAsString
			i++
		} else {
			panic("id_ips in the config file cannot be decoded correctly")
		}
	}

	// create the threshold signature keys
	numT := nodeNumber - nodeNumber/3
	shares, pubPoly := sign_tools.GenTSKeys(numT, nodeNumber)
	logLevel := viperRead.GetInt("log_level")
	maxPool := viperRead.GetInt("max_pool")
	timeOut := viperRead.GetInt("timeout")
	mockLatency := viperRead.GetInt("mock_latency")
	ddos := viperRead.GetBool("ddos")
	ddosDelay := viperRead.GetInt("ddos_delay")
	max_payload_size := viperRead.GetInt("max_payload_size")
	max_payload_count := viperRead.GetInt("max_payload_count")
	tx_size := viperRead.GetInt("tx_size")
	rate := viperRead.GetInt("rate")
	wait_time := viperRead.GetInt("wait_time")

	sks := make([]ed25519.PrivateKey, nodeNumber)
	pksMap := make(map[int]string)
	for i := 0; i < nodeNumber; i++ {
		sk, pk := sign_tools.GenED25519Keys()
		sks[i] = sk
		pksMap[i] = hex.EncodeToString(pk)
	}

	// write to configure files
	for i, name := range idNameMap {
		viperWrite := viper.New()
		viperWrite.SetConfigFile(fmt.Sprintf("%s.yaml", name))
		share := shares[i]
		shareAsBytes, err := sign_tools.EncodeTSPartialKey(share)
		if err != nil {
			panic("encode the share")
		}
		tsPubKeyAsBytes, err := sign_tools.EncodeTSPublicKey(pubPoly)
		if err != nil {
			panic("encode the share")
		}
		viperWrite.Set("id", i)
		viperWrite.Set("name", name)
		viperWrite.Set("address", idIPMap[i])
		viperWrite.Set("p2p_port", idP2PPortMap[i])
		viperWrite.Set("p2p_port_payload", idP2PPortPayloadMap[i])
		viperWrite.Set("id_p2p_port", idP2PPortMap)
		viperWrite.Set("id_p2p_port_payload", idP2PPortPayloadMap)
		//viperWrite.Set("rpc_listen_port", rpcListenPort)
		viperWrite.Set("TSShare", hex.EncodeToString(shareAsBytes))
		viperWrite.Set("TSPubKey", hex.EncodeToString(tsPubKeyAsBytes))
		viperWrite.Set("log_level", logLevel)
		viperWrite.Set("max_pool", maxPool)
		viperWrite.Set("timeout", timeOut)
		viperWrite.Set("id_name", idNameMap)
		viperWrite.Set("id_ip", idIPMap)
		viperWrite.Set("mock_latency", mockLatency)
		viperWrite.Set("ddos", ddos)
		viperWrite.Set("ddos_delay", ddosDelay)
		viperWrite.Set("max_payload_size", max_payload_size)
		viperWrite.Set("max_payload_count", max_payload_count)
		viperWrite.Set("tx_size", tx_size)
		viperWrite.Set("rate", rate)
		viperWrite.Set("wait_time", wait_time)
		viperWrite.Set("pri_key", hex.EncodeToString(sks[i]))
		viperWrite.Set("pub_key_map", pksMap)
		viperWrite.WriteConfig()
	}
}
