package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"github.com/stackimpact/stackimpact-go"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var globalConfigDBMgr *GlobalConfigDBMgr
var addrTrxsDBMgr *AddrTrxsDBMgr
var utxoDBMgr *UtxoDBMgr
var rawTrxDBMgr *RawTrxDBMgr

var quitFlag = false
var quitChan chan byte
var config Config

var startBlockHeight uint32

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

	// init global config db manager
	globalConfigDBMgr = new(GlobalConfigDBMgr)
	err = globalConfigDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "global_config_db")
	if err != nil {
		return err
	}

	// init address trx db manager
	addrTrxsDBMgr = new(AddrTrxsDBMgr)
	err = addrTrxsDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "address_trx_db")
	if err != nil {
		return err
	}

	// init trx utxo db manager
	utxoDBMgr = new(UtxoDBMgr)
	err = utxoDBMgr.DBOpen(config.DBConfig.DBDir + "/" + "trx_utxo_db")
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
	ok, err := getChainIndexState()
	if err != nil {
		if err.Error() != LevelDBNotFound {
			return err
		}
	} else {
		if !ok {
			return err
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
		} else if strLine == "getslotweight" {
			fmt.Println(slotCache.CalcObjectCacheWeight())
		} else if strLine == "goroutinestatus" {
			goroutineMgr.GoroutineDump()
		} else {
			fmt.Println("not support command: ", strLine)
		}
	}
	<-quitChan

	// sync and close
	globalConfigDBMgr.DBClose()
	addrTrxsDBMgr.DBClose()
	utxoDBMgr.DBClose()
	rawTrxDBMgr.DBClose()

	return nil
}

func main() {
	var err error

	go func() {
		http.ListenAndServe("0.0.0.0:8080", nil)
	}()

	agent := stackimpact.Start(stackimpact.Options{
		AgentKey: "0f6538f8e7589efb205d8dc44a4b9ba1ecfd0b11",
		AppName:  "MyGoApp",
	})
	agent.StartCPUProfiler()
	agent.StartBlockProfiler()
	agent.ReportAllocationProfile()

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
