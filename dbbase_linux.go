package main

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tecbot/gorocksdb"
)

const (
	NotFoundErrorLevelDB = "leveldb: not found"
	NotFoundErrorRocksDB = "rocksdb: not found"
)

var NotFoundError string
var RocksDBCreateOpt *gorocksdb.Options
var RocksDBReadOpt *gorocksdb.ReadOptions
var RocksDBWriteOpt *gorocksdb.WriteOptions

type DBCommon struct {
	ldb *leveldb.DB
	rdb *gorocksdb.DB
}

func (d *DBCommon) DBOpen(dbFile string) error {
	var err error
	if config.DBConfig.DbType == "leveldb" {
		d.ldb, err = leveldb.OpenFile(dbFile, nil)
		if err != nil {
			return err
		}
		return nil
	} else if config.DBConfig.DbType == "rocksdb" {
		d.rdb, err = gorocksdb.OpenDb(RocksDBCreateOpt, dbFile)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("invalid db type")
}

func (d *DBCommon) DBClose() error {
	if config.DBConfig.DbType == "leveldb" {
		err := d.ldb.Close()
		if err != nil {
			return err
		}
		return nil
	} else if config.DBConfig.DbType == "rocksdb" {
		d.rdb.Close()
		return nil
	}
	return errors.New("invalid db type")
}

func (d DBCommon) DBPut(key []byte, value []byte) error {
	if config.DBConfig.DbType == "leveldb" {
		err := d.ldb.Put(key, value, nil)
		if err != nil {
			return err
		}
		return nil
	} else if config.DBConfig.DbType == "rocksdb" {
		err := d.rdb.Put(RocksDBWriteOpt, key, value)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("invalid db type")
}

func (d DBCommon) DBGet(key []byte) ([]byte, error) {
	if config.DBConfig.DbType == "leveldb" {
		value, err := d.ldb.Get(key, nil)
		if err != nil {
			return nil, err
		}
		valueBytes := make([]byte, len(value))
		copy(valueBytes[0:], value)
		return valueBytes, nil
	} else if config.DBConfig.DbType == "rocksdb" {
		value, err := d.rdb.Get(RocksDBReadOpt, key)
		if err != nil {
			return nil, err
		}
		defer value.Free()
		if value.Data() == nil {
			return nil, errors.New(NotFoundErrorRocksDB)
		}
		valueBytes := make([]byte, len(value.Data()))
		copy(valueBytes[0:], value.Data())
		return valueBytes, nil
	}
	return nil, errors.New("invalid db type")
}

func (d DBCommon) DBGetPrefix(key []byte) ([][]byte, error) {
	var valuesBytes [][]byte
	if config.DBConfig.DbType == "leveldb" {
		iter := d.ldb.NewIterator(util.BytesPrefix(key), nil)
		for iter.Next() {
			valueBytes := make([]byte, len(iter.Value()))
			copy(valueBytes[0:], iter.Value())
			valuesBytes = append(valuesBytes, valueBytes)
		}
		iter.Release()
		err := iter.Error()
		if err != nil {
			return nil, err
		}
		return valuesBytes, nil
	} else if config.DBConfig.DbType == "rocksdb" {
		iter := d.rdb.NewIterator(RocksDBReadOpt)
		for iter.Seek(key); iter.Valid() && bytes.HasPrefix(iter.Key().Data(), key); iter.Next() {
			k, v := iter.Key(), iter.Value()
			valueBytes := make([]byte, len(iter.Value().Data()))
			copy(valueBytes[0:], iter.Value().Data())
			valuesBytes = append(valuesBytes, valueBytes)
			k.Free()
			v.Free()
		}
		err := iter.Err()
		if err != nil {
			return nil, err
		}
		return valuesBytes, nil
	}
	return nil, errors.New("invalid db type")
}

func (d DBCommon) DBDelete(key []byte) error {
	if config.DBConfig.DbType == "leveldb" {
		err := d.ldb.Delete(key, nil)
		if err != nil {
			return err
		}
		return nil
	} else if config.DBConfig.DbType == "rocksdb" {
		err := d.rdb.Delete(RocksDBWriteOpt, key)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("invalid db type")
}
