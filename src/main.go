package main

import (
	"errors"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var globalConfigDBMgr *GlobalConfigDBMgr
var addressTrxDBMgr *AddressTrxDBMgr
var trxUtxoDBMgr *TrxUtxoDBMgr

var dbPath string = "db/"
var quitFlag = false
var quitChan chan byte
var gatherType = 1

func appInit() error {
	var err error

	// init goroutine manager
	goroutineMgr = new(goroutine_mgr.GoroutineManager)
	goroutineMgr.Initialise("MainGoroutineManager")

	// init global config db manager
	globalConfigDBMgr = new(GlobalConfigDBMgr)
	err = globalConfigDBMgr.DBOpen(dbPath + "global_config_db")
	if err != nil {
		return err
	}

	// init address trx db manager
	addressTrxDBMgr = new(AddressTrxDBMgr)
	err = addressTrxDBMgr.DBOpen(dbPath + "address_trx_db")
	if err != nil {
		return err
	}

	// init trx utxo db manager
	trxUtxoDBMgr = new(TrxUtxoDBMgr)
	err = trxUtxoDBMgr.DBOpen(dbPath + "trx_utxo_db")
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

	return nil
}

func main() {
	var err error

	err = appInit()
	if err != nil {
		return
	}
	err = appRun()
	if err != nil {
		return
	}
	err = appCmd()
	if err != nil {
		return
	}
	return
}
