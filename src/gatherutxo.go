package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	block "github.com/mutalisk999/bitcoin-lib/src/block"
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
	rpcClient := jsonrpc.NewClient("http://test:test@192.168.1.107:30011")
	rpcResponse, err := rpcClient.Call(method, args)
	if err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func getBlockCountRpcType1() (uint32, error) {
	rpcResponse, err := doHttpJsonRpcCallType1("getblockcount")
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
	rpcClient := jsonrpc.NewClient("")
	rpcResponse, err := rpcClient.Call(method, args)
	if err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func getBlockCountRpcType2() (uint32, error) {
	rpcResponse, err := doHttpJsonRpcCallType2("GetBlockCount")
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
	rpcResponse, err := doHttpJsonRpcCallType2("GetBlockHash", blockHeight)
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
	rpcResponse, err := doHttpJsonRpcCallType2("GetRawBlock", blockHash)
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
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
	if err != nil {
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

func storeStartBlockHeight(blockHeight uint32) error {
	err := globalConfigDBMgr.DBPut("blockHeight", strconv.Itoa(int(blockHeight)))
	if err != nil {
		return err
	}
	return nil
}

func dealWithVinToCache(blockCache *BlockCache, vin transaction.TxIn, trxId bigint.Uint256) error {
	lastVoutInCache := false
	var scriptPubKey script.Script
	// deal trx utxo pair
	for i := 0; i < len(blockCache.TrxUtxos); i++ {
		if bigint.IsUint256Equal(&vin.PrevOut.Hash, &blockCache.TrxUtxos[i].TrxUtxoKey.TrxId) && vin.PrevOut.N == blockCache.TrxUtxos[i].TrxUtxoKey.Vout {
			if blockCache.TrxUtxos[i].TrxUtxoValue.Status != 0 {
				return errors.New("invalid utxo status, and utxo status must be unspent")
			}
			blockCache.TrxUtxos[i].TrxUtxoValue.Status = 1
			scriptPubKey = blockCache.TrxUtxos[i].TrxUtxoValue.ScriptPubKey
			lastVoutInCache = true
			break
		}
	}
	if !lastVoutInCache {
		utxoSource := UtxoSource{vin.PrevOut.Hash, vin.PrevOut.N}
		utxoDetail, err := trxUtxoDBMgr.DBGet(utxoSource)
		if err != nil {
			return errors.New("can not find prevout trxid: " + vin.PrevOut.Hash.GetHex() + ", vout: " + strconv.Itoa(int(vin.PrevOut.N)))
		}
		scriptPubKey = utxoDetail.ScriptPubKey
		trxUtxoPair := new(TrxUtxoPair)
		trxUtxoPair.TrxUtxoKey = utxoSource
		utxoDetail.Status = 1
		trxUtxoPair.TrxUtxoValue = utxoDetail
		trxUtxoPair.TrxUtxoOp = 0
		blockCache.AddTrxUtxoPair(*trxUtxoPair)
	}
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
					blockCache.AddressTrxs[i].AddressTrxValue[trxId] = 0
					addressInCache = true
					break
				}
			}
			if !addressInCache {
				addrTrxPair := AddressTrxPair{}
				trxIds, err := addressTrxDBMgr.DBGet(addrStr)
				if err != nil {
					addrTrxPair = AddressTrxPair{addrStr, map[bigint.Uint256]int{trxId: 0}, 0}
				} else {
					trxIds[trxId] = 0
					addrTrxPair = AddressTrxPair{addrStr, trxIds, 0}
				}
				blockCache.AddressTrxs = append(blockCache.AddressTrxs, addrTrxPair)
			}
		}
	}
	return nil
}

func dealWithVoutToCache(blockCache *BlockCache, vout transaction.TxOut, trxId bigint.Uint256, index uint32) error {
	return nil
}

func dealWithTrxToCache(blockCache *BlockCache, trx *transaction.Transaction, isCoinBase bool) error {
	trxId, err := trx.CalcTrxId()
	if err != nil {
		return err
	}
	if !isCoinBase {
		for _, vin := range trx.Vin {
			err := dealWithVinToCache(blockCache, vin, trxId)
			if err != nil {
				return err
			}
		}
	}
	for index, vout := range trx.Vout {
		if vout.Value != 0 {
			err := dealWithVoutToCache(blockCache, vout, trxId, uint32(index))
			if err != nil {
				return err
			}
		}
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
	blockCache := new(BlockCache)
	for i := 0; i < len(blockNew.Vtx); i++ {
		isCoinBase := false
		if i == 0 {
			isCoinBase = true
		}
		err = dealWithTrxToCache(blockCache, &blockNew.Vtx[i], isCoinBase)
		if err != nil {
			return err
		}
	}
	return nil
}

func doGatherUtxoType1(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	for {
		if quitFlag {
			break
		}
		startBlockHeight, err := getStartBlockHeight()
		if err != nil {
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
				err = storeStartBlockHeight(NewBlockHeight)
				if err != nil {
					quitFlag = true
					break
				}
				startBlockHeight += 1
			}
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
	for {
		if quitFlag {
			break
		}
		startBlockHeight, err := getStartBlockHeight()
		if err != nil {
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
			}
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
