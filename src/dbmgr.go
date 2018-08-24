package main

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
)

type GlobalConfigDBMgr struct {
	db *DBCommon
}

type AddrTrxsDBMgr struct {
	db *DBCommon
}

type UtxoDBMgr struct {
	db *DBCommon
}

type TrxSeqDBMgr struct {
	db *DBCommon
}

type RawTrxDBMgr struct {
	db *DBCommon
}

func (g *GlobalConfigDBMgr) DBOpen(dbFile string) error {
	g.db = new(DBCommon)
	err := g.db.DBOpen(dbFile)
	if err != nil {
		return err
	}
	return nil
}

func (g *GlobalConfigDBMgr) DBClose() error {
	err := g.db.DBClose()
	if err != nil {
		return err
	}
	return nil
}

func (g GlobalConfigDBMgr) DBPut(key string, value string) error {
	err := g.db.DBPut([]byte(key), []byte(value))
	if err != nil {
		return err
	}
	return nil
}

func (g GlobalConfigDBMgr) DBGet(key string) (string, error) {
	valueBytes, err := g.db.DBGet([]byte(key))
	if err != nil {
		return "", err
	}
	return string(valueBytes), nil
}

func (g GlobalConfigDBMgr) DBDelete(key string) error {
	err := g.db.DBDelete([]byte(key))
	if err != nil {
		return err
	}
	return nil
}

func (a *AddrTrxsDBMgr) DBOpen(dbFile string) error {
	a.db = new(DBCommon)
	err := a.db.DBOpen(dbFile)
	if err != nil {
		return err
	}
	return nil
}

func (a *AddrTrxsDBMgr) DBClose() error {
	err := a.db.DBClose()
	if err != nil {
		return err
	}
	return nil
}

func (a AddrTrxsDBMgr) DBPut(key string, value []uint32) error {
	valueBytes, err := trxSeqsToBytes(value)
	if err != nil {
		return err
	}
	err = a.db.DBPut([]byte(key), valueBytes)
	if err != nil {
		return err
	}
	return nil
}

func (a AddrTrxsDBMgr) DBGet(key string) ([]uint32, error) {
	valueBytes, err := a.db.DBGet([]byte(key))
	if err != nil {
		return nil, err
	}
	trxIds, err := trxSeqsFromBytes(valueBytes)
	return trxIds, nil
}

func (a AddrTrxsDBMgr) DBDelete(key string) error {
	err := a.db.DBDelete([]byte(key))
	if err != nil {
		return err
	}
	return nil
}

func (u *UtxoDBMgr) DBOpen(dbFile string) error {
	u.db = new(DBCommon)
	err := u.db.DBOpen(dbFile)
	if err != nil {
		return err
	}
	return nil
}

func (u *UtxoDBMgr) DBClose() error {
	err := u.db.DBClose()
	if err != nil {
		return err
	}
	return nil
}

func (u UtxoDBMgr) DBPut(key UtxoSource, value UtxoDetail) error {
	keyBytes, err := utxoSrcToBytes(key)
	if err != nil {
		return err
	}
	valueBytes, err := utxoDetailToBytes(value)
	if err != nil {
		return err
	}
	err = u.db.DBPut(keyBytes, valueBytes)
	if err != nil {
		return err
	}
	return nil
}

func (u UtxoDBMgr) DBGet(key UtxoSource) (UtxoDetail, error) {
	keyBytes, err := utxoSrcToBytes(key)
	if err != nil {
		return UtxoDetail{}, err
	}
	valueBytes, err := u.db.DBGet(keyBytes)
	if err != nil {
		return UtxoDetail{}, err
	}
	utxoDetail, err := utxoDetailFromBytes(valueBytes)
	return utxoDetail, nil
}

func (u UtxoDBMgr) DBDelete(key UtxoSource) error {
	keyBytes, err := utxoSrcToBytes(key)
	if err != nil {
		return err
	}
	err = u.db.DBDelete(keyBytes)
	if err != nil {
		return err
	}
	return nil
}

func (t *TrxSeqDBMgr) DBOpen(dbFile string) error {
	t.db = new(DBCommon)
	err := t.db.DBOpen(dbFile)
	if err != nil {
		return err
	}
	return nil
}

func (t *TrxSeqDBMgr) DBClose() error {
	err := t.db.DBClose()
	if err != nil {
		return err
	}
	return nil
}

func (t TrxSeqDBMgr) DBPut(key uint32, value bigint.Uint256) error {
	keyBytes, err := uint32ToBytes(key)
	if err != nil {
		return err
	}
	valueBytes, err := uint256ToBytes(value)
	if err != nil {
		return err
	}
	err = t.db.DBPut(keyBytes, valueBytes)
	if err != nil {
		return err
	}
	return nil
}

func (t TrxSeqDBMgr) DBGet(key uint32) (bigint.Uint256, error) {
	keyBytes, err := uint32ToBytes(key)
	if err != nil {
		return bigint.Uint256{}, err
	}
	valueBytes, err := t.db.DBGet(keyBytes)
	if err != nil {
		return bigint.Uint256{}, err
	}
	ui256, err := uint256FromBytes(valueBytes)
	if err != nil {
		return bigint.Uint256{}, err
	}
	return ui256, nil
}

func (t TrxSeqDBMgr) DBDelete(key uint32) error {
	keyBytes, err := uint32ToBytes(key)
	if err != nil {
		return err
	}
	err = t.db.DBDelete(keyBytes)
	if err != nil {
		return err
	}
	return nil
}

func (r *RawTrxDBMgr) DBOpen(dbFile string) error {
	r.db = new(DBCommon)
	err := r.db.DBOpen(dbFile)
	if err != nil {
		return err
	}
	return nil
}

func (r *RawTrxDBMgr) DBClose() error {
	err := r.db.DBClose()
	if err != nil {
		return err
	}
	return nil
}

func (r RawTrxDBMgr) DBPut(key bigint.Uint256, value []byte) error {
	keyBytes, err := uint256ToBytes(key)
	if err != nil {
		return err
	}
	err = r.db.DBPut(keyBytes, value)
	if err != nil {
		return err
	}
	return nil
}

func (r RawTrxDBMgr) DBGet(key bigint.Uint256) ([]byte, error) {
	keyBytes, err := uint256ToBytes(key)
	if err != nil {
		return nil, err
	}
	valueBytes, err := r.db.DBGet(keyBytes)
	if err != nil {
		return nil, err
	}
	return valueBytes, nil
}

func (r RawTrxDBMgr) DBDelete(key bigint.Uint256) error {
	keyBytes, err := uint256ToBytes(key)
	if err != nil {
		return err
	}
	err = r.db.DBDelete(keyBytes)
	if err != nil {
		return err
	}
	return nil
}
