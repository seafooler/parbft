package config

import (
	"encoding/hex"
	"github.com/seafooler/sign_tools"
	"github.com/spf13/viper"
	"go.dedis.ch/kyber/v3/share"
	"strconv"
	"strings"
)

// Config defines a type to describe the configuration.
type Config struct {
	N          int
	F          int
	Id         int
	Name       string
	Id2NameMap map[int]string // map from id to name
	Name2IdMap map[string]int // map from name to id
	Addr       string
	P2pPort    string
	// own private key of threshold signature
	PriKeyTS *share.PriShare
	// public key of threshold signature
	PubKeyTS *share.PubPoly

	Id2AddrMap map[int]string // map from id to address
	Id2PortMap map[int]string // map from id to p2pPort

	MaxPool  int
	LogLevel int
	Timeout  int
}

// New creates a new variable of type Config from some arguments.
func New(id int, name string, id2NameMap map[int]string, name2IdMap map[string]int, addr, p2pPort string,
	priKeyTS *share.PriShare, pubKeyTS *share.PubPoly, id2AddrMap, id2PortMap map[int]string, maxPool, logLevel, timeOut int) *Config {
	conf := &Config{
		Id:         id,
		Name:       name,
		Id2NameMap: id2NameMap,
		Name2IdMap: name2IdMap,
		Addr:       addr,
		P2pPort:    p2pPort,
		PriKeyTS:   priKeyTS,
		PubKeyTS:   pubKeyTS,
		Id2AddrMap: id2AddrMap,
		Id2PortMap: id2PortMap,
		MaxPool:    maxPool,
		LogLevel:   logLevel,
		Timeout:    timeOut,
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
	logLevel := viperConfig.GetInt("log_level")
	maxPool := viperConfig.GetInt("max_pool")
	timeOut := viperConfig.GetInt("timeout")

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

	return New(id, name, id2NameMap, name2IdMap, addr, p2pPort, tsShareKey, tsPubKey, id2AddrMap, id2P2PPortMap,
		maxPool, logLevel, timeOut), nil
}
