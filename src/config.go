package main

import (
	"encoding/json"
	"io/ioutil"
)

type DBConfig struct {
	DBDir string `json:"dbDir"`
}

type CacheConfig struct {
	ObjectCacheWeightMax uint32 `json:"objectCacheWeightMax"`
}

type GatherConfig struct {
	StoreRawTrx bool `json:"storeRawTrx"`
}

type BtcWalletConfig struct {
	RpcReqUrl string `json:"rpcReqUrl"`
}

type RawBlockConfig struct {
	RpcReqUrl string `json:"rpcReqUrl"`
}

type RpcClientConfig struct {
	DataSource string          `json:"dataSource"`
	BtcWallet  BtcWalletConfig `json:"btcWallet"`
	RawBlock   RawBlockConfig  `json:"rawBlock"`
}

type RpcServerConfig struct {
	RpcListenEndPoint string `json:"rpcListenEndPoint"`
}

type Config struct {
	DBConfig        DBConfig        `json:"dbConfig"`
	CacheConfig     CacheConfig     `json:"cacheConfig"`
	GatherConfig    GatherConfig    `json:"gatherConfig"`
	RpcClientConfig RpcClientConfig `json:"rpcClientConfig"`
	RpcServerConfig RpcServerConfig `json:"rpcServerConfig"`
}

type JsonStruct struct {
}

func (j *JsonStruct) Load(configFile string, config interface{}) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}
	return nil
}
