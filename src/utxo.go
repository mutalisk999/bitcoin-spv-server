package main

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/blob"
	"github.com/mutalisk999/bitcoin-lib/src/script"
	"github.com/mutalisk999/bitcoin-lib/src/serialize"
	"io"
)

type UtxoSource struct {
	TrxId bigint.Uint256
	Vout  uint32
}

func (u UtxoSource) Pack(writer io.Writer) error {
	err := u.TrxId.Pack(writer)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, u.Vout)
	if err != nil {
		return err
	}
	return nil
}

func (u *UtxoSource) UnPack(reader io.Reader) error {
	err := u.TrxId.UnPack(reader)
	if err != nil {
		return err
	}
	u.Vout, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	return nil
}

type UtxoDetail struct {
	Amount       int64
	Height       uint32
	Address      string
	ScriptPubKey script.Script
	Status       byte // 0 valid     1 has spent
}

func (u UtxoDetail) Pack(writer io.Writer) error {
	err := serialize.PackInt64(writer, u.Amount)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, u.Height)
	if err != nil {
		return err
	}
	var bytesAddr blob.Byteblob
	bytesAddr.SetData([]byte(u.Address))
	err = bytesAddr.Pack(writer)
	if err != nil {
		return err
	}
	err = u.ScriptPubKey.Pack(writer)
	if err != nil {
		return err
	}
	err = serialize.PackByte(writer, u.Status)
	if err != nil {
		return err
	}
	return nil
}

func (u *UtxoDetail) UnPack(reader io.Reader) error {
	var err error
	u.Amount, err = serialize.UnPackInt64(reader)
	if err != nil {
		return err
	}
	u.Height, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	var bytesAddr blob.Byteblob
	err = bytesAddr.UnPack(reader)
	if err != nil {
		return err
	}
	u.Address = string(bytesAddr.GetData())
	err = u.ScriptPubKey.UnPack(reader)
	if err != nil {
		return err
	}
	u.Status, err = serialize.UnPackByte(reader)
	if err != nil {
		return err
	}
	return nil
}
