package main

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	NotFoundErrorLevelDB = "leveldb: not found"
)

var NotFoundError string

type DBCommon struct {
	ldb *leveldb.DB
}

func (d *DBCommon) DBOpen(dbFile string) error {
	var err error
	if config.DBConfig.DbType == "leveldb" {
		d.ldb, err = leveldb.OpenFile(dbFile, nil)
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
	}
	return errors.New("invalid db type")
}
