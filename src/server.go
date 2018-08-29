package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/transaction"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"io"
	"net/http"
)

type Service struct {
}

func (s *Service) GetBlockCount(r *http.Request, args *interface{}, reply *uint32) error {
	*reply = startBlockHeight
	return nil
}

func (s *Service) GetTrxCount(r *http.Request, args *interface{}, reply *uint32) error {
	*reply = startTrxSequence
	return nil
}

func (s *Service) GetTrxIdBySeq(r *http.Request, args *uint32, reply *string) error {
	trxId, err := trxSeqDBMgr.DBGet(*args)
	if err != nil {
		return errors.New("trx seq not found")
	}
	*reply = trxId.GetHex()
	return nil
}

func (s *Service) GetAddressTrxs(r *http.Request, args *string, reply *[]string) error {
	trxSeqs, err := addrTrxsDBMgr.DBGetPrefix(*args + ".")
	if err != nil {
		return errors.New("address not found")
	}
	for _, trxSeq := range trxSeqs {
		trxId, err := trxSeqDBMgr.DBGet(trxSeq)
		if err != nil {
			//return errors.New("trx sequence not found")
			continue
		}
		*reply = append(*reply, trxId.GetHex())
	}
	return nil
}

func (s *Service) GetRawTrx(r *http.Request, args *string, reply *string) error {
	var trxId bigint.Uint256
	err := trxId.SetHex(*args)
	if err != nil {
		return err
	}
	bytesRawTrx, err := rawTrxDBMgr.DBGet(trxId)
	if err != nil {
		return errors.New("transaction id not found")
	}
	*reply = hex.EncodeToString(bytesRawTrx)
	return nil
}

func (s *Service) GetTrx(r *http.Request, args *string, reply *transaction.TrxPrintAble) error {
	var trxId bigint.Uint256
	err := trxId.SetHex(*args)
	if err != nil {
		return err
	}
	bytesRawTrx, err := rawTrxDBMgr.DBGet(trxId)
	if err != nil {
		return errors.New("transaction id not found")
	}
	var trx transaction.Transaction
	bytesBuf := bytes.NewBuffer(bytesRawTrx)
	bytesReader := io.Reader(bytesBuf)
	err = trx.UnPack(bytesReader)
	if err != nil {
		return errors.New("unpack raw transaction fail")
	}
	trxPrintAble := trx.GetTrxPrintAble()
	*reply = trxPrintAble
	return nil
}

func (s *Service) GetUtxo(r *http.Request, args *UtxoSourcePrintAble, reply *UtxoDetailPrintAble) error {
	utxoSource := args.GetUtxoSource()
	utxoDetail, err := utxoDBMgr.DBGet(utxoSource)
	if err != nil {
		return errors.New("utxo source not found")
	}
	utxoDetailPrintAble := utxoDetail.GetUtxoDetailPrintAble()
	*reply = utxoDetailPrintAble
	return nil
}

func (s *Service) ListUnSpent(r *http.Request, args *string, reply *[]UtxoDetailPrintAble) error {
	trxSeqs, err := addrTrxsDBMgr.DBGetPrefix(*args + ".")
	if err != nil {
		return errors.New("address not found")
	}
	for _, trxSeq := range trxSeqs {
		trxId, err := trxSeqDBMgr.DBGet(trxSeq)
		if err != nil {
			//return errors.New("trx sequence not found")
			continue
		}
		bytesRawTrx, err := rawTrxDBMgr.DBGet(trxId)
		if err != nil {
			//return errors.New("transaction id not found")
			continue
		}
		var trx transaction.Transaction
		bytesBuf := bytes.NewBuffer(bytesRawTrx)
		bytesReader := io.Reader(bytesBuf)
		err = trx.UnPack(bytesReader)
		if err != nil {
			return errors.New("unpack raw transaction fail")
		}
		for i, _ := range trx.Vout {
			var utxoSource UtxoSource
			utxoSource.TrxId = trxId
			utxoSource.Vout = uint32(i)
			utxoDetail, err := utxoDBMgr.DBGet(utxoSource)
			if err != nil {
				continue
			}
			utxoDetailPrintAble := utxoDetail.GetUtxoDetailPrintAble()
			*reply = append(*reply, utxoDetailPrintAble)
		}
	}
	return nil
}

func rpcServer(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	rpcServer := rpc.NewServer()
	rpcServer.RegisterCodec(json.NewCodec(), "application/json")
	rpcServer.RegisterCodec(json.NewCodec(), "application/json;charset=UTF-8")

	rpcService := new(Service)
	rpcServer.RegisterService(rpcService, "")

	urlRouter := mux.NewRouter()
	urlRouter.Handle("/", rpcServer)
	http.ListenAndServe(config.RpcServerConfig.RpcListenEndPoint, urlRouter)
}

func startRpcServer() uint64 {
	return goroutineMgr.GoroutineCreatePn("rpcserver", rpcServer, nil)
}
