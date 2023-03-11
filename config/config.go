package config

import (
	"crypto/ed25519"
	"encoding/hex"
	"github.com/seafooler/sign_tools"
	"github.com/spf13/viper"
	"go.dedis.ch/kyber/v3/share"
	"strconv"
	"strings"
)

// Config defines a type to describe the configuration.
type Config struct {
	N              int
	F              int
	Id             int
	Name           string
	Id2NameMap     map[int]string // map from id to name
	Name2IdMap     map[string]int // map from name to id
	Addr           string
	P2pPort        string
	P2pPortPayload string

	// own private key of threshold signature
	PriKeyTS *share.PriShare
	// public key of threshold signature
	PubKeyTS *share.PubPoly

	PriKeyED ed25519.PrivateKey
	PubKeyED map[int]ed25519.PublicKey

	Id2AddrMap        map[int]string // map from id to address
	Id2PortMap        map[int]string // map from id to p2pPort
	Id2PortPayLoadMap map[int]string // map from id to p2pPortPayload

	MaxPool         int
	LogLevel        int
	Timeout         int
	MockLatency     int
	DDoS            bool
	DDoSDelay       int
	MaxPayloadSize  int
	MaxPayloadCount int
	Rate            int
	TxSize          int
	WaitTime        int
}

// New creates a new variable of type Config from some arguments.
func New(id int, name string, id2NameMap map[int]string, name2IdMap map[string]int, addr, p2pPort, p2pPortPayLoad string,
	priKeyTS *share.PriShare, pubKeyTS *share.PubPoly, priKeyED ed25519.PrivateKey, pubKeyED map[int]ed25519.PublicKey,
	id2AddrMap, id2PortMap, id2PortPayloadMap map[int]string, maxPool, logLevel, timeOut int, mockLatency int,
	ddos bool, ddosDelay, maxPayloadSize, maxPayloadCount, rate, txSize, waitTime int) *Config {
	conf := &Config{
		Id:                id,
		Name:              name,
		Id2NameMap:        id2NameMap,
		Name2IdMap:        name2IdMap,
		Addr:              addr,
		P2pPort:           p2pPort,
		P2pPortPayload:    p2pPortPayLoad,
		PriKeyTS:          priKeyTS,
		PubKeyTS:          pubKeyTS,
		PriKeyED:          priKeyED,
		PubKeyED:          pubKeyED,
		Id2AddrMap:        id2AddrMap,
		Id2PortMap:        id2PortMap,
		Id2PortPayLoadMap: id2PortPayloadMap,
		MaxPool:           maxPool,
		LogLevel:          logLevel,
		Timeout:           timeOut,
		MockLatency:       mockLatency,
		DDoS:              ddos,
		DDoSDelay:         ddosDelay,
		MaxPayloadCount:   maxPayloadCount,
		MaxPayloadSize:    maxPayloadSize,
		Rate:              rate,
		TxSize:            txSize,
		WaitTime:          waitTime,
	}

	conf.N = len(id2NameMap)
	conf.F = conf.N / 3

	return conf
}

// LoadConfig loads configuration files by package viper.
func LoadConfig(configPrefix, configName string) (*Config, error) {
	viperConfig := viper.New()

	// for environment variables
	viperConfig.SetEnvPrefix(configPrefix)
	viperConfig.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viperConfig.SetEnvKeyReplacer(replacer)

	viperConfig.SetConfigName(configName)
	viperConfig.AddConfigPath("./")

	err := viperConfig.ReadInConfig()
	if err != nil {
		return nil, err
	}

	addr := viperConfig.GetString("address")
	id := viperConfig.GetInt("id")
	name := viperConfig.GetString("name")
	p2pPort := viperConfig.GetString("p2p_port")
	p2pPortPayload := viperConfig.GetString("p2p_port_payload")
	logLevel := viperConfig.GetInt("log_level")
	maxPool := viperConfig.GetInt("max_pool")
	timeOut := viperConfig.GetInt("timeout")
	mockLatency := viperConfig.GetInt("mock_latency")
	ddos := viperConfig.GetBool("ddos")
	ddosDelay := viperConfig.GetInt("ddos_delay")
	maxPayloadSize := viperConfig.GetInt("max_payload_size")
	maxPayloadCount := viperConfig.GetInt("max_payload_count")
	rate := viperConfig.GetInt("rate")
	txSize := viperConfig.GetInt("tx_size")
	waitTime := viperConfig.GetInt("wait_time")

	idStringNameMap := viperConfig.GetStringMapString("id_name")
	id2NameMap := make(map[int]string)
	name2IdMap := make(map[string]int)

	for idAsString, na := range idStringNameMap {
		if idAsInt, err := strconv.Atoi(idAsString); err == nil {
			id2NameMap[idAsInt] = na
			name2IdMap[na] = idAsInt
		} else {
			panic(err)
		}
	}

	idStringAddrMap := viperConfig.GetStringMapString("id_ip")
	id2AddrMap := make(map[int]string)

	for idAsString, ad := range idStringAddrMap {
		if idAsInt, err := strconv.Atoi(idAsString); err == nil {
			id2AddrMap[idAsInt] = ad
		} else {
			panic(err)
		}
	}

	idStringP2PPortMap := viperConfig.GetStringMapString("id_p2p_port")
	id2P2PPortMap := make(map[int]string)

	for idAsString, po := range idStringP2PPortMap {
		if idAsInt, err := strconv.Atoi(idAsString); err == nil {
			id2P2PPortMap[idAsInt] = po
		} else {
			panic(err)
		}
	}

	idStringP2PPortPayloadMap := viperConfig.GetStringMapString("id_p2p_port_payload")
	id2P2PPortPayloadMap := make(map[int]string)

	for idAsString, po := range idStringP2PPortPayloadMap {
		if idAsInt, err := strconv.Atoi(idAsString); err == nil {
			id2P2PPortPayloadMap[idAsInt] = po
		} else {
			panic(err)
		}
	}

	tsPubKeyAsString := viperConfig.GetString("tspubkey")
	tsPubKeyAsBytes, err := hex.DecodeString(tsPubKeyAsString)
	if err != nil {
		return nil, err
	}
	tsPubKey, err := sign_tools.DecodeTSPublicKey(tsPubKeyAsBytes)
	if err != nil {
		return nil, err
	}

	tsShareAsString := viperConfig.GetString("tsshare")
	tsShareAsBytes, err := hex.DecodeString(tsShareAsString)
	if err != nil {
		return nil, err
	}
	tsShareKey, err := sign_tools.DecodeTSPartialKey(tsShareAsBytes)
	if err != nil {
		return nil, err
	}

	edPriKeyAsString := viperConfig.GetString("pri_key")
	edPriKey, err := hex.DecodeString(edPriKeyAsString)
	if err != nil {
		return nil, err
	}

	edPubKeyMapString := viperConfig.GetStringMapString("pub_key_map")
	edPubKeyMap := make(map[int]ed25519.PublicKey, len(edPubKeyMapString))

	for idStr, pkAsStr := range edPubKeyMapString {
		id, _ := strconv.Atoi(idStr)
		pk, err := hex.DecodeString(pkAsStr)
		if err != nil {
			panic(err)
		}
		edPubKeyMap[id] = pk
	}

	return New(id, name, id2NameMap, name2IdMap, addr, p2pPort, p2pPortPayload, tsShareKey, tsPubKey, edPriKey,
		edPubKeyMap, id2AddrMap, id2P2PPortMap, id2P2PPortPayloadMap, maxPool, logLevel, timeOut, mockLatency,
		ddos, ddosDelay, maxPayloadSize, maxPayloadCount, rate, txSize, waitTime), nil
}
