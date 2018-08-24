package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"github.com/tecbot/gorocksdb"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var globalConfigDBMgr *GlobalConfigDBMgr
var addrTrxsDBMgr *AddrTrxsDBMgr
var utxoDBMgr *UtxoDBMgr
var trxSeqDBMgr *TrxSeqDBMgr
var rawTrxDBMgr *RawTrxDBMgr

var quitFlag = false
var quitChan chan byte
var config Config

var startBlockHeight uint32
var startTrxSequence uint32

func appInit() error {
	var err error

	// init quit channel
	quitChan = make(chan byte)

	// init slot cache
	slotCache = new(SlotCache)
	slotCache.Initialize()

	// init goroutine manager
	goroutineMgr = new(goroutine_mgr.GoroutineManager)
	goroutineMgr.Initialise("MainGoroutineManager")

	// init db config
	if config.DBConfig.DbType == "leveldb" {
		NotFoundError = NotFoundErrorLevelDB
	} else if config.DBConfig.DbType == "rocksdb" {
		NotFoundError = NotFoundErrorRocksDB
		RocksDBCreateOpt = gorocksdb.NewDefaultOptions()
		RocksDBCreateOpt.SetCreateIfMissing(true)
		RocksDBReadOpt = gorocksdb.NewDefaultReadOptions()
		RocksDBWriteOpt = gorocksdb.NewDefaultWriteOptions()
	} else {
		return errors.New("invalid db type")
	}

	// init global config db manager
	globalConfigDBMgr = new(GlobalConfigDBMgr)
	err = globalConfigDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "global_config_db")
	if err != nil {
		return err
	}

	// init address trx db manager
	addrTrxsDBMgr = new(AddrTrxsDBMgr)
	err = addrTrxsDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "addr_trx_db")
	if err != nil {
		return err
	}

	// init trx utxo db manager
	utxoDBMgr = new(UtxoDBMgr)
	err = utxoDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "utxo_db")
	if err != nil {
		return err
	}

	// init trx seq db manager
	trxSeqDBMgr = new(TrxSeqDBMgr)
	err = trxSeqDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "trx_seq_db")
	if err != nil {
		return err
	}

	// init raw trx db manager
	rawTrxDBMgr = new(RawTrxDBMgr)
	err = rawTrxDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "raw_trx_db")
	if err != nil {
		return err
	}

	// get chain index state
	state, err := getChainIndexState()
	if err != nil {
		if err.Error() != NotFoundError {
			return err
		}
	} else {
		if state != "1" {
			return errors.New("incorrect chain index state")
		}
	}

	return nil
}

func appRun() error {
	startSignalHandler()
	startRpcServer()

	if config.RpcClientConfig.DataSource == "btcWallet" {
		// collect from the wallet node
		startGatherUtxoType1()
	} else if config.RpcClientConfig.DataSource == "rawBlock" {
		// collect from the raw block collector
		startGatherUtxoType2()
	} else {
		return errors.New("invalid gather type")
	}
	return nil
}

func appCmd() error {
	var stdinReader *bufio.Reader
	stdinReader = bufio.NewReader(os.Stdin)
	var stdoutWriter *bufio.Writer
	stdoutWriter = bufio.NewWriter(os.Stdout)
	for {
		if quitFlag {
			break
		}
		_, err := stdoutWriter.WriteString(">>>")
		if err != nil {
			quitFlag = true
			break
		}
		stdoutWriter.Flush()
		strLine, err := stdinReader.ReadString('\n')
		if err != nil {
			quitFlag = true
			break
		}
		strLine = strings.Trim(strLine, "\x0a")
		strLine = strings.Trim(strLine, "\x0d")
		strLine = strings.TrimLeft(strLine, " ")
		strLine = strings.TrimRight(strLine, " ")

		if strLine == "" {
		} else if strLine == "stop" || strLine == "quit" || strLine == "exit" {
			quitFlag = true
			break
		} else if strLine == "getblockcount" {
			fmt.Println(startBlockHeight)
		} else if strLine == "gettrxcount" {
			fmt.Println(startTrxSequence)
		} else if strLine == "getslotweight" {
			fmt.Println(slotCache.CalcObjectCacheWeight())
		} else if strLine == "goroutinestatus" {
			goroutineMgr.GoroutineDump()
		} else if strLine == "heapinfo" {
			var mStat runtime.MemStats
			runtime.ReadMemStats(&mStat)
			fmt.Println("HeapAlloc:", mStat.HeapAlloc)
			fmt.Println("HeapIdle:", mStat.HeapIdle)
		} else if strLine == "memoryfree" {
			runtime.GC()
			debug.FreeOSMemory()
		} else {
			fmt.Println("not support command: ", strLine)
		}
	}
	<-quitChan

	// sync and close
	globalConfigDBMgr.DBClose()
	addrTrxsDBMgr.DBClose()
	utxoDBMgr.DBClose()
	trxSeqDBMgr.DBClose()
	rawTrxDBMgr.DBClose()

	return nil
}

func main() {
	var err error

	// init config
	jsonParser := new(JsonStruct)
	err = jsonParser.Load("config.json", &config)
	if err != nil {
		fmt.Println("Load config.json", err)
		return
	}

	err = appInit()
	if err != nil {
		fmt.Println("appInit", err)
		return
	}
	err = appRun()
	if err != nil {
		fmt.Println("appRun", err)
		return
	}
	err = appCmd()
	if err != nil {
		fmt.Println("appCmd", err)
		return
	}
	return
}
