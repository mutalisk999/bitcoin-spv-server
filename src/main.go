package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"os"
	"strings"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var globalConfigDBMgr *GlobalConfigDBMgr
var addressTrxDBMgr *AddressTrxDBMgr
var trxUtxoDBMgr *TrxUtxoDBMgr

var dbPath string = "F:/btc_spv_data/db"
var quitFlag = false
var quitChan chan byte
var gatherType = 2

var startBlockHeight uint32

func appInit() error {
	var err error
	// init quit channel
	quitChan = make(chan byte)

	// init goroutine manager
	goroutineMgr = new(goroutine_mgr.GoroutineManager)
	goroutineMgr.Initialise("MainGoroutineManager")

	// init global config db manager
	globalConfigDBMgr = new(GlobalConfigDBMgr)
	err = globalConfigDBMgr.DBOpen(dbPath + "/" + "global_config_db")
	if err != nil {
		return err
	}

	// init address trx db manager
	addressTrxDBMgr = new(AddressTrxDBMgr)
	err = addressTrxDBMgr.DBOpen(dbPath + "/" + "address_trx_db")
	if err != nil {
		return err
	}

	// init trx utxo db manager
	trxUtxoDBMgr = new(TrxUtxoDBMgr)
	err = trxUtxoDBMgr.DBOpen(dbPath + "/" + "trx_utxo_db")
	if err != nil {
		return err
	}

	return nil
}

func appRun() error {
	startSignalHandler()
	//startRpcServer()

	if gatherType == 1 {
		// collect from the wallet node
		startGatherUtxoType1()
	} else if gatherType == 2 {
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
		} else if strLine == "goroutinestatus" {
			goroutineMgr.GoroutineDump()
		} else {
			fmt.Println("not support command: ", strLine)
		}
	}
	<-quitChan

	// sync and close
	globalConfigDBMgr.DBClose()
	addressTrxDBMgr.DBClose()
	trxUtxoDBMgr.DBClose()

	return nil
}

func main() {
	var err error

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
