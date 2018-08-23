package main

import (
	"errors"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/syndtr/goleveldb/leveldb"
)

var LevelDBNotFound = "leveldb: not found"

type GlobalConfigDBMgr struct {
	db *leveldb.DB
}

type GlobalConfigPair struct {
	GlobalConfigKey   string
	GlobalConfigValue string
	GlobalConfigOp    byte // 0 put, 1 delete
}

type AddrTrxsDBMgr struct {
	db *leveldb.DB
}

type AddrTrxsPair struct {
	AddrTrxsKey   string
	AddrTrxsValue []uint32
	AddrTrxsOp    byte // 0 put, 1 delete
}

type UtxoDBMgr struct {
	db *leveldb.DB
}

type UtxoPair struct {
	UtxoKey   UtxoSource
	UtxoValue UtxoDetail
	UtxoOp    byte // 0 put, 1 delete
}

type TrxSeqDBMgr struct {
	db *leveldb.DB
}

type TrxSeqPair struct {
	TrxSeqKey  uint32
	TrxIdValue bigint.Uint256
	TrxSeqOp   byte // 0 put, 1 delete
}

type RawTrxDBMgr struct {
	db *leveldb.DB
}

type RawTrxPair struct {
	TrxIdKey        bigint.Uint256
	RawTrxDataValue []byte
	RawTrxOp        byte // 0 put, 1 delete
}

func (g *GlobalConfigDBMgr) DBOpen(dbFile string) error {
	var err error
	g.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (g *GlobalConfigDBMgr) DBClose() error {
	err := g.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (g GlobalConfigDBMgr) DBPut(key string, value string) error {
	err := g.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		return err
	}
	return nil
}

func (g GlobalConfigDBMgr) DBGet(key string) (string, error) {
	valueBytes, err := g.db.Get([]byte(key), nil)
	if err != nil {
		return "", err
	}
	return string(valueBytes), nil
}

func (g GlobalConfigDBMgr) DBDelete(key string) error {
	err := g.db.Delete([]byte(key), nil)
	if err != nil {
		return err
	}
	return nil
}

func (g GlobalConfigDBMgr) DBBatch(globalConfigs []GlobalConfigPair) error {
	batch := new(leveldb.Batch)
	for _, globalConfig := range globalConfigs {
		if globalConfig.GlobalConfigOp == 0 {
			batch.Put([]byte(globalConfig.GlobalConfigKey), []byte(globalConfig.GlobalConfigValue))
		} else if globalConfig.GlobalConfigOp == 1 {
			batch.Delete([]byte(globalConfig.GlobalConfigKey))
		} else {
			return errors.New("GlobalConfigOp type not support")
		}
	}
	err := g.db.Write(batch, nil)
	if err != nil && err.Error() != LevelDBNotFound {
		return err
	}
	return nil
}

func (a *AddrTrxsDBMgr) DBOpen(dbFile string) error {
	var err error
	a.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (a *AddrTrxsDBMgr) DBClose() error {
	err := a.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (a AddrTrxsDBMgr) DBPut(key string, value []uint32) error {
	bytesValue, err := trxSeqsToBytes(value)
	if err != nil {
		return err
	}
	err = a.db.Put([]byte(key), bytesValue, nil)
	if err != nil {
		return err
	}
	return nil
}

func (a AddrTrxsDBMgr) DBGet(key string) ([]uint32, error) {
	bytesValue, err := a.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	trxIds, err := trxSeqsFromBytes(bytesValue)
	return trxIds, nil
}

func (a AddrTrxsDBMgr) DBDelete(key string) error {
	err := a.db.Delete([]byte(key), nil)
	if err != nil {
		return err
	}
	return nil
}

func (a AddrTrxsDBMgr) DBBatch(addrTrxs []AddrTrxsPair) error {
	batch := new(leveldb.Batch)
	for _, addrTrx := range addrTrxs {
		if addrTrx.AddrTrxsOp == 0 {
			bytesValue, err := trxSeqsToBytes(addrTrx.AddrTrxsValue)
			if err != nil {
				return err
			}
			batch.Put([]byte(addrTrx.AddrTrxsKey), bytesValue)
		} else if addrTrx.AddrTrxsOp == 1 {
			batch.Delete([]byte(addrTrx.AddrTrxsKey))
		} else {
			return errors.New("AddressTrxOp type not support")
		}
	}
	err := a.db.Write(batch, nil)
	if err != nil && err.Error() != LevelDBNotFound {
		return err
	}
	return nil
}

func (t *UtxoDBMgr) DBOpen(dbFile string) error {
	var err error
	t.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t *UtxoDBMgr) DBClose() error {
	err := t.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (t UtxoDBMgr) DBPut(key UtxoSource, value UtxoDetail) error {
	bytesKey, err := utxoSrcToBytes(key)
	if err != nil {
		return err
	}
	bytesValue, err := utxoDetailToBytes(value)
	if err != nil {
		return err
	}
	err = t.db.Put(bytesKey, bytesValue, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t UtxoDBMgr) DBGet(key UtxoSource) (UtxoDetail, error) {
	bytesKey, err := utxoSrcToBytes(key)
	if err != nil {
		return UtxoDetail{}, err
	}
	bytesValue, err := t.db.Get(bytesKey, nil)
	if err != nil {
		return UtxoDetail{}, err
	}
	utxoDetail, err := utxoDetailFromBytes(bytesValue)
	return utxoDetail, nil
}

func (t UtxoDBMgr) DBDelete(key UtxoSource) error {
	bytesKey, err := utxoSrcToBytes(key)
	if err != nil {
		return err
	}
	err = t.db.Delete(bytesKey, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t UtxoDBMgr) DBBatch(trxUtxos []UtxoPair) error {
	batch := new(leveldb.Batch)
	for _, trxUtxo := range trxUtxos {
		if trxUtxo.UtxoOp == 0 {
			bytesKey, err := utxoSrcToBytes(trxUtxo.UtxoKey)
			if err != nil {
				return err
			}
			bytesValue, err := utxoDetailToBytes(trxUtxo.UtxoValue)
			if err != nil {
				return err
			}
			batch.Put(bytesKey, bytesValue)
		} else if trxUtxo.UtxoOp == 1 {
			bytesKey, err := utxoSrcToBytes(trxUtxo.UtxoKey)
			if err != nil {
				return err
			}
			batch.Delete(bytesKey)
		} else {
			return errors.New("TrxUtxoOp type not support")
		}
	}
	err := t.db.Write(batch, nil)
	if err != nil && err.Error() != LevelDBNotFound {
		return err
	}
	return nil
}

func (t *TrxSeqDBMgr) DBOpen(dbFile string) error {
	var err error
	t.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t *TrxSeqDBMgr) DBClose() error {
	err := t.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (t TrxSeqDBMgr) DBPut(key uint32, value bigint.Uint256) error {
	bytesKey, err := uint32ToBytes(key)
	if err != nil {
		return err
	}
	bytesValue, err := uint256ToBytes(value)
	if err != nil {
		return err
	}
	err = t.db.Put(bytesKey, bytesValue, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t TrxSeqDBMgr) DBGet(key uint32) (bigint.Uint256, error) {
	bytesKey, err := uint32ToBytes(key)
	if err != nil {
		return bigint.Uint256{}, err
	}
	bytesValue, err := t.db.Get(bytesKey, nil)
	if err != nil {
		return bigint.Uint256{}, err
	}
	ui256, err := uint256FromBytes(bytesValue)
	if err != nil {
		return bigint.Uint256{}, err
	}
	return ui256, nil
}

func (t TrxSeqDBMgr) DBDelete(key uint32) error {
	bytesKey, err := uint32ToBytes(key)
	if err != nil {
		return err
	}
	err = t.db.Delete(bytesKey, nil)
	if err != nil {
		return err
	}
	return nil
}

func (r TrxSeqDBMgr) DBBatch(trxSeqs []TrxSeqPair) error {
	batch := new(leveldb.Batch)
	for _, trxSeq := range trxSeqs {
		bytesKey, err := uint32ToBytes(trxSeq.TrxSeqKey)
		if err != nil {
			return err
		}
		if trxSeq.TrxSeqOp == 0 {
			bytesValue, err := uint256ToBytes(trxSeq.TrxIdValue)
			if err != nil {
				return err
			}
			batch.Put(bytesKey, bytesValue)
		} else if trxSeq.TrxSeqOp == 1 {
			batch.Delete(bytesKey)
		} else {
			return errors.New("TrxSeqOp type not support")
		}
	}
	err := r.db.Write(batch, nil)
	if err != nil && err.Error() != LevelDBNotFound {
		return err
	}
	return nil
}

func (r *RawTrxDBMgr) DBOpen(dbFile string) error {
	var err error
	r.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (r *RawTrxDBMgr) DBClose() error {
	err := r.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (r RawTrxDBMgr) DBPut(key bigint.Uint256, value []byte) error {
	bytesKey, err := uint256ToBytes(key)
	if err != nil {
		return err
	}
	err = r.db.Put(bytesKey, value, nil)
	if err != nil {
		return err
	}
	return nil
}

func (r RawTrxDBMgr) DBGet(key bigint.Uint256) ([]byte, error) {
	bytesKey, err := uint256ToBytes(key)
	if err != nil {
		return []byte{}, err
	}
	bytesValue, err := r.db.Get(bytesKey, nil)
	if err != nil {
		return []byte{}, err
	}
	return bytesValue, nil
}

func (r RawTrxDBMgr) DBDelete(key bigint.Uint256) error {
	bytesKey, err := uint256ToBytes(key)
	if err != nil {
		return err
	}
	err = r.db.Delete(bytesKey, nil)
	if err != nil {
		return err
	}
	return nil
}

func (r RawTrxDBMgr) DBBatch(rawTrxs []RawTrxPair) error {
	batch := new(leveldb.Batch)
	for _, rawTrx := range rawTrxs {
		bytesKey, err := uint256ToBytes(rawTrx.TrxIdKey)
		if err != nil {
			return err
		}
		if rawTrx.RawTrxOp == 0 {
			batch.Put(bytesKey, rawTrx.RawTrxDataValue)
		} else if rawTrx.RawTrxOp == 1 {
			batch.Delete(bytesKey)
		} else {
			return errors.New("RawTrxOp type not support")
		}
	}
	err := r.db.Write(batch, nil)
	if err != nil && err.Error() != LevelDBNotFound {
		return err
	}
	return nil
}
