package main

import (
	"bytes"
	"errors"
	"github.com/mutalisk999/bitcoin-lib/src/serialize"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
)

type GlobalConfigDBMgr struct {
	db *leveldb.DB
}

type GlobalConfigPair struct {
	GlobalConfigKey   string
	GlobalConfigValue string
	GlobalConfigOp    byte // 0 put, 1 delete
}

type AddressTrxDBMgr struct {
	db *leveldb.DB
}

type AddressTrxPair struct {
	AddressTrxKey   string
	AddressTrxValue []UtxoSource
	AddressTrxOp    byte // 0 put, 1 delete
}

type TrxUtxoDBMgr struct {
	db *leveldb.DB
}

type TrxUtxoPair struct {
	TrxUtxoKey   UtxoSource
	TrxUtxoValue UtxoDetail
	TrxUtxoOp    byte // 0 put, 1 delete
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
	if err != nil {
		return err
	}
	return nil
}

func (a *AddressTrxDBMgr) DBOpen(dbFile string) error {
	var err error
	a.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (a *AddressTrxDBMgr) DBClose() error {
	err := a.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func utxoSrcsToBytes(utxoSrcs []UtxoSource) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := serialize.PackCompactSize(bufWriter, uint64(len(utxoSrcs)))
	if err != nil {
		return []byte{}, err
	}
	for _, utxoSrc := range utxoSrcs {
		err = utxoSrc.Pack(bufWriter)
		if err != nil {
			return []byte{}, err
		}
	}
	return bytesBuf.Bytes(), nil
}

func utxoSrcsFromBytes(bytesUtxoSrcs []byte) ([]UtxoSource, error) {
	var utxoSrcs []UtxoSource
	bufReader := io.Reader(bytes.NewBuffer(bytesUtxoSrcs))
	ui64, err := serialize.UnPackCompactSize(bufReader)
	if err != nil {
		return []UtxoSource{}, err
	}
	for i := 0; i < int(ui64); i++ {
		var utxoSrc UtxoSource
		err = utxoSrc.UnPack(bufReader)
		if err != nil {
			return []UtxoSource{}, err
		}
		utxoSrcs = append(utxoSrcs, utxoSrc)
	}
	return utxoSrcs, nil
}

func (a AddressTrxDBMgr) DBPut(key string, value []UtxoSource) error {
	bytesValue, err := utxoSrcsToBytes(value)
	if err != nil {
		return err
	}
	err = a.db.Put([]byte(key), bytesValue, nil)
	if err != nil {
		return err
	}
	return nil
}

func (a AddressTrxDBMgr) DBGet(key string) ([]UtxoSource, error) {
	bytesValue, err := a.db.Get([]byte(key), nil)
	if err != nil {
		return []UtxoSource{}, err
	}
	utxoSrcs, err := utxoSrcsFromBytes(bytesValue)
	return utxoSrcs, nil
}

func (a AddressTrxDBMgr) DBDelete(key string) error {
	err := a.db.Delete([]byte(key), nil)
	if err != nil {
		return err
	}
	return nil
}

func (a AddressTrxDBMgr) DBBatch(addressTrxs []AddressTrxPair) error {
	batch := new(leveldb.Batch)
	for _, addressTrx := range addressTrxs {
		if addressTrx.AddressTrxOp == 0 {
			bytesValue, err := utxoSrcsToBytes(addressTrx.AddressTrxValue)
			if err != nil {
				return err
			}
			batch.Put([]byte(addressTrx.AddressTrxKey), bytesValue)
		} else if addressTrx.AddressTrxOp == 1 {
			batch.Delete([]byte(addressTrx.AddressTrxKey))
		} else {
			return errors.New("AddressTrxOp type not support")
		}
	}
	err := a.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t *TrxUtxoDBMgr) DBOpen(dbFile string) error {
	var err error
	t.db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}
	return nil
}

func (t *TrxUtxoDBMgr) DBClose() error {
	err := t.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func utxoSrcToBytes(utxoSrc UtxoSource) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := utxoSrc.Pack(bufWriter)
	if err != nil {
		return []byte{}, err
	}
	return bytesBuf.Bytes(), nil
}

func utxoSrcFromBytes(bytesUtxoSrc []byte) (UtxoSource, error) {
	var utxoSrc UtxoSource
	bufReader := io.Reader(bytes.NewBuffer(bytesUtxoSrc))
	err := utxoSrc.UnPack(bufReader)
	if err != nil {
		return UtxoSource{}, err
	}
	return utxoSrc, nil
}

func utxoDetailToBytes(utxoDetail UtxoDetail) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := utxoDetail.Pack(bufWriter)
	if err != nil {
		return []byte{}, err
	}
	return bytesBuf.Bytes(), nil
}

func utxoDetailFromBytes(bytesUtxoDetail []byte) (UtxoDetail, error) {
	var utxoDetail UtxoDetail
	bufReader := io.Reader(bytes.NewBuffer(bytesUtxoDetail))
	err := utxoDetail.UnPack(bufReader)
	if err != nil {
		return UtxoDetail{}, err
	}
	return utxoDetail, nil
}

func (t TrxUtxoDBMgr) DBPut(key UtxoSource, value UtxoDetail) error {
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

func (t TrxUtxoDBMgr) DBGet(key UtxoSource) (UtxoDetail, error) {
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

func (t TrxUtxoDBMgr) DBDelete(key UtxoSource) error {
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

func (t TrxUtxoDBMgr) DBBatch(trxUtxos []TrxUtxoPair) error {
	batch := new(leveldb.Batch)
	for _, trxUtxo := range trxUtxos {
		if trxUtxo.TrxUtxoOp == 0 {
			bytesKey, err := utxoSrcToBytes(trxUtxo.TrxUtxoKey)
			if err != nil {
				return err
			}
			bytesValue, err := utxoDetailToBytes(trxUtxo.TrxUtxoValue)
			if err != nil {
				return err
			}
			batch.Put(bytesKey, bytesValue)
		} else if trxUtxo.TrxUtxoOp == 1 {
			bytesKey, err := utxoSrcToBytes(trxUtxo.TrxUtxoKey)
			if err != nil {
				return err
			}
			batch.Delete(bytesKey)
		} else {
			return errors.New("TrxUtxoOp type not support")
		}
	}
	err := t.db.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}