package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/block"
	"github.com/mutalisk999/bitcoin-lib/src/script"
	"github.com/mutalisk999/bitcoin-lib/src/transaction"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"github.com/ybbus/jsonrpc"
	"io"
	"strconv"
	"strings"
	"time"
)

func doHttpJsonRpcCallType1(method string, args ...interface{}) (*jsonrpc.RPCResponse, error) {
	rpcClient := jsonrpc.NewClient(config.RpcClientConfig.BtcWallet.RpcReqUrl)
	rpcResponse, err := rpcClient.Call(method, args)
	if err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func getBlockCountRpcType1() (uint32, error) {
	rpcResponse, err := doHttpJsonRpcCallType1("getblockcount")
	if err != nil {
		fmt.Println("getBlockCountRpcType1 Failed: ", err)
		return 0, err
	}
	blockCount, err := rpcResponse.GetInt()
	if err != nil {
		fmt.Println("Get blockCount from rpcResponse Failed: ", err)
		return 0, err
	}
	return uint32(blockCount), nil
}

func getBlockHashRpcType1(blockHeight uint32) (string, error) {
	rpcResponse, err := doHttpJsonRpcCallType1("getblockhash", blockHeight)
	if err != nil {
		fmt.Println("getBlockCountRpcType1 Failed: ", err)
		return "", err
	}
	blockHash, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get blockHash from rpcResponse Failed: ", err)
		return "", err
	}
	return blockHash, nil
}

func getRawBlockType1(blockHash string) (string, error) {
	rpcResponse, err := doHttpJsonRpcCallType1("getblock", blockHash, 0)
	if err != nil {
		fmt.Println("getBlockCountRpcType1 Failed: ", err)
		return "", err
	}
	rawBlockHex, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get rawBlockHex from rpcResponse Failed: ", err)
		return "", err
	}
	return rawBlockHex, nil
}

func doHttpJsonRpcCallType2(method string, args ...interface{}) (*jsonrpc.RPCResponse, error) {
	rpcClient := jsonrpc.NewClient(config.RpcClientConfig.RawBlock.RpcReqUrl)
	rpcResponse, err := rpcClient.Call(method, args)
	if err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func getBlockCountRpcType2() (uint32, error) {
	rpcResponse, err := doHttpJsonRpcCallType2("Service.GetBlockCount", nil)
	if err != nil {
		fmt.Println("doHttpJsonRpcCallType2 Failed: ", err)
		return 0, err
	}
	blockCount, err := rpcResponse.GetInt()
	if err != nil {
		fmt.Println("Get blockCount from rpcResponse Failed: ", err)
		return 0, err
	}
	return uint32(blockCount), nil
}

func getBlockHashRpcType2(blockHeight uint32) (string, error) {
	rpcResponse, err := doHttpJsonRpcCallType2("Service.GetBlockHash", blockHeight)
	if err != nil {
		fmt.Println("doHttpJsonRpcCallType2 Failed: ", err)
		return "", err
	}
	blockHash, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get blockHash from rpcResponse Failed: ", err)
		return "", err
	}
	return blockHash, nil
}

func getRawBlockType2(blockHash string) (string, error) {
	rpcResponse, err := doHttpJsonRpcCallType2("Service.GetRawBlock", blockHash)
	if err != nil {
		fmt.Println("doHttpJsonRpcCallType2 Failed: ", err)
		return "", err
	}
	rawBlockHex, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get rawBlockHex from rpcResponse Failed: ", err)
		return "", err
	}
	return rawBlockHex, nil
}

func getStartBlockHeight() (uint32, error) {
	var startBlockHeight uint32
	blockHeightStr, err := globalConfigDBMgr.DBGet("blockHeight")
	if err != nil && err.Error() == LevelDBNotFound {
		startBlockHeight = 0
	} else {
		ui64, err := strconv.ParseUint(blockHeightStr, 10, 32)
		if err != nil {
			return 0, err
		}
		startBlockHeight = uint32(ui64)
	}
	return startBlockHeight, nil
}

func storeBlockCache(blockCache *BlockCache) error {
	err := addressTrxDBMgr.DBBatch(blockCache.AddressTrxs)
	if err != nil {
		return err
	}
	err = trxUtxoDBMgr.DBBatch(blockCache.TrxUtxos)
	if err != nil {
		return err
	}
	err = rawTrxDBMgr.DBBatch(blockCache.RawTrxs)
	if err != nil {
		return err
	}
	return nil
}

func storeStartBlockHeight(blockHeight uint32) error {
	err := globalConfigDBMgr.DBPut("blockHeight", strconv.Itoa(int(blockHeight)))
	if err != nil {
		return err
	}
	return nil
}

func dealWithVinToCache(vin transaction.TxIn, trxId bigint.Uint256) error {
	// deal trx utxo pair
	// query from memory cache
	utxoSource := UtxoSource{vin.PrevOut.Hash, vin.PrevOut.N}
	utxoDetail, ok := utxoMemCache.Get(utxoSource)
	if !ok {
		return errors.New("can not find prevout trxid: " + vin.PrevOut.Hash.GetHex() + ", vout: " + strconv.Itoa(int(vin.PrevOut.N)))
	}
	utxoMemCache.Remove(utxoSource)

	scriptPubKey := utxoDetail.ScriptPubKey
	trxUtxoPair := new(TrxUtxoPair)
	trxUtxoPair.TrxUtxoKey = utxoSource
	trxUtxoPair.TrxUtxoOp = 1
	blockCache.AddTrxUtxoPair(*trxUtxoPair)

	// deal address trx pair
	isSucc, scriptType, addresses := script.ExtractDestination(scriptPubKey)
	if isSucc {
		addrStr := ""
		if script.IsSingleAddress(scriptType) {
			addrStr = addresses[0]
		} else if script.IsMultiAddress(scriptType) {
			addrStr = strings.Join(addresses, ",")
		}
		if addrStr != "" {
			addressInCache := false
			for i := 0; i < len(blockCache.AddressTrxs); i++ {
				if blockCache.AddressTrxs[i].AddressTrxKey == addrStr {
					blockCache.AddressTrxs[i].AddressTrxValue[trxId.GetHex()] = 0
					addressInCache = true
					break
				}
			}
			if !addressInCache {
				addrTrxPair := AddressTrxPair{}
				trxIds, err := addressTrxDBMgr.DBGet(addrStr)
				if err != nil && err.Error() == LevelDBNotFound {
					fmt.Println("err != nil:", err)
					addrTrxPair = AddressTrxPair{addrStr, map[string]int{trxId.GetHex(): 0}, 0}
				} else {
					trxIds[trxId.GetHex()] = 0
					addrTrxPair = AddressTrxPair{addrStr, trxIds, 0}
				}
				blockCache.AddAddressTrxPair(addrTrxPair)
			}
		}
	}
	return nil
}

func dealWithVoutToCache(blockHeight uint32, vout transaction.TxOut, trxId bigint.Uint256, index uint32) error {
	var scriptPubKey script.Script
	var addrStr string

	scriptPubKey = vout.ScriptPubKey
	// deal address trx pair
	isSucc, scriptType, addresses := script.ExtractDestination(scriptPubKey)
	if isSucc {
		if script.IsSingleAddress(scriptType) {
			addrStr = addresses[0]
		} else if script.IsMultiAddress(scriptType) {
			addrStr = strings.Join(addresses, ",")
		}
		if addrStr != "" {
			addressInCache := false
			for i := 0; i < len(blockCache.AddressTrxs); i++ {
				if blockCache.AddressTrxs[i].AddressTrxKey == addrStr {
					blockCache.AddressTrxs[i].AddressTrxValue[trxId.GetHex()] = 0
					addressInCache = true
					break
				}
			}
			if !addressInCache {
				addrTrxPair := AddressTrxPair{}
				trxIds, err := addressTrxDBMgr.DBGet(addrStr)
				if err != nil && err.Error() == LevelDBNotFound {
					addrTrxPair = AddressTrxPair{addrStr, map[string]int{trxId.GetHex(): 0}, 0}
				} else {
					trxIds[trxId.GetHex()] = 0
					addrTrxPair = AddressTrxPair{addrStr, trxIds, 0}
				}
				blockCache.AddAddressTrxPair(addrTrxPair)
			}
		}
	}
	// deal trx utxo pair
	utxoSource := UtxoSource{trxId, index}
	utxoDetail := UtxoDetail{vout.Value, blockHeight, addrStr, scriptPubKey}
	trxUtxoPair := new(TrxUtxoPair)
	trxUtxoPair.TrxUtxoKey = utxoSource
	trxUtxoPair.TrxUtxoValue = utxoDetail
	trxUtxoPair.TrxUtxoOp = 0
	blockCache.AddTrxUtxoPair(*trxUtxoPair)

	// add to memory cache
	utxoMemCache.Add(utxoSource, utxoDetail)

	return nil
}

func dealWithRawTrxToCache(trxId bigint.Uint256, trx *transaction.Transaction) error {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := trx.Pack(bufWriter)
	if err != nil {
		return err
	}
	rawTrxPair := RawTrxPair{trxId.GetHex(), bytesBuf.Bytes(), 0}
	blockCache.AddRawTrxPair(rawTrxPair)
	return nil
}

func dealWithTrxToCache(blockHeight uint32, trx *transaction.Transaction, isCoinBase bool) error {
	trxId, err := trx.CalcTrxId()
	if err != nil {
		return err
	}
	if !isCoinBase {
		for _, vin := range trx.Vin {
			err := dealWithVinToCache(vin, trxId)
			if err != nil {
				return err
			}
		}
	}
	for index, vout := range trx.Vout {
		err := dealWithVoutToCache(blockHeight, vout, trxId, uint32(index))
		if err != nil {
			return err
		}
	}
	err = dealWithRawTrxToCache(trxId, trx)
	if err != nil {
		return err
	}

	return nil
}

func dealWithRawBlock(blockHeight uint32, rawBlockData *string) error {
	blockBytes, err := hex.DecodeString(*rawBlockData)
	if err != nil {
		return err
	}
	bytesBuf := bytes.NewBuffer(blockBytes)
	bufReader := io.Reader(bytesBuf)
	blockNew := new(block.Block)
	blockNew.UnPack(bufReader)
	for i := 0; i < len(blockNew.Vtx); i++ {
		isCoinBase := false
		if i == 0 {
			isCoinBase = true
		}
		err = dealWithTrxToCache(blockHeight, &blockNew.Vtx[i], isCoinBase)
		if err != nil {
			return err
		}
	}
	return nil
}

func doGatherUtxoType1(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	var err error

	startBlockHeight, err = getStartBlockHeight()
	if err != nil {
		return
	}

	for {
		if quitFlag {
			break
		}

		blockCount, err := getBlockCountRpcType1()
		if err != nil {
			break
		}

		if startBlockHeight >= blockCount {
			time.Sleep(5 * 1000 * 1000 * 1000)
		} else {
			for {
				if quitFlag {
					break
				}

				if startBlockHeight >= blockCount {
					break
				}
				NewBlockHeight := startBlockHeight + 1

				blockHash, err := getBlockHashRpcType1(NewBlockHeight)
				if err != nil {
					quitFlag = true
					break
				}
				rawBlockData, err := getRawBlockType1(blockHash)
				if err != nil {
					quitFlag = true
					break
				}
				err = dealWithRawBlock(NewBlockHeight, &rawBlockData)
				if err != nil {
					quitFlag = true
					break
				}
				if NewBlockHeight%config.CacheConfig.BlockCacheCount == 0 {
					err = storeBlockCache(blockCache)
					if err != nil {
						quitFlag = true
						break
					}
					err = storeStartBlockHeight(NewBlockHeight)
					if err != nil {
						quitFlag = true
						break
					}
					blockCache = new(BlockCache)
				}
				startBlockHeight += 1
			}
			// need to flush block cache
			err = storeBlockCache(blockCache)
			if err != nil {
				quitFlag = true
				break
			}
			err = storeStartBlockHeight(startBlockHeight)
			if err != nil {
				quitFlag = true
				break
			}
			blockCache = new(BlockCache)

			// if break from the inside loop for, break from the outside loop for
			if quitFlag == true {
				break
			}
		}
	}
	quitChan <- 0x0
}

func doGatherUtxoType2(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	var err error

	startBlockHeight, err = getStartBlockHeight()
	if err != nil {
		return
	}

	for {
		if quitFlag {
			break
		}

		blockCount, err := getBlockCountRpcType2()
		if err != nil {
			break
		}

		if startBlockHeight >= blockCount {
			time.Sleep(5 * 1000 * 1000 * 1000)
		} else {
			for {
				if quitFlag {
					break
				}

				if startBlockHeight >= blockCount {
					break
				}
				NewBlockHeight := startBlockHeight + 1

				blockHash, err := getBlockHashRpcType2(NewBlockHeight)
				if err != nil {
					quitFlag = true
					break
				}
				rawBlockData, err := getRawBlockType2(blockHash)
				if err != nil {
					quitFlag = true
					break
				}
				err = dealWithRawBlock(NewBlockHeight, &rawBlockData)
				if err != nil {
					quitFlag = true
					break
				}
				if NewBlockHeight%config.CacheConfig.BlockCacheCount == 0 {
					err = storeBlockCache(blockCache)
					if err != nil {
						quitFlag = true
						break
					}
					err = storeStartBlockHeight(NewBlockHeight)
					if err != nil {
						quitFlag = true
						break
					}
					blockCache = new(BlockCache)
				}
				startBlockHeight += 1
			}
			// need to flush block cache
			err = storeBlockCache(blockCache)
			if err != nil {
				quitFlag = true
				break
			}
			err = storeStartBlockHeight(startBlockHeight)
			if err != nil {
				quitFlag = true
				break
			}
			blockCache = new(BlockCache)

			// if break from the inside loop for, break from the outside loop for
			if quitFlag == true {
				break
			}
		}
	}
	quitChan <- 0x0
}

func startGatherUtxoType1() uint64 {
	return goroutineMgr.GoroutineCreatePn("gatherutxotype1", doGatherUtxoType1, nil)
}

func startGatherUtxoType2() uint64 {
	return goroutineMgr.GoroutineCreatePn("gatherutxotype2", doGatherUtxoType2, nil)
}
