package main

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
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
		valueBytes, err := d.ldb.Get(key, nil)
		if err != nil {
			return nil, err
		}
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
		return value.Data(), nil
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
