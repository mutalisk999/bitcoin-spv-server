package main

import (
	"encoding/hex"
	"errors"
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

func (u UtxoSource) ToStreamString() (string, error) {
	bytesUint32, err := uint32ToBytes(u.Vout)
	if err != nil {
		return "", err
	}
	streamStr := string(u.TrxId.GetData()) + string(bytesUint32)
	if len(streamStr) != 36 {
		return "", errors.New("invalid stream string with wrong length")
	}
	return streamStr, nil
}

func (u *UtxoSource) FromStreamString(streamStr string) error {
	if len(streamStr) != 36 {
		return errors.New("invalid stream string with wrong length")
	}
	var err error
	u.TrxId.SetData([]byte(streamStr[0:32]))
	u.Vout, err = uint32FromBytes([]byte(streamStr[32:36]))
	if err != nil {
		return err
	}
	return nil
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

type UtxoSourcePrintAble struct {
	TrxId string
	Vout  uint32
}

func (u *UtxoSource) GetUtxoSourcePrintAble() UtxoSourcePrintAble {
	var utxoSourcePrintAble UtxoSourcePrintAble
	utxoSourcePrintAble.TrxId = u.TrxId.GetHex()
	utxoSourcePrintAble.Vout = u.Vout
	return utxoSourcePrintAble
}

func (u *UtxoSourcePrintAble) GetUtxoSource() UtxoSource {
	var utxoSource UtxoSource
	utxoSource.TrxId.SetHex(u.TrxId)
	utxoSource.Vout = u.Vout
	return utxoSource
}

type UtxoDetail struct {
	Amount       int64
	BlockHeight  uint32
	Address      string
	ScriptPubKey script.Script
}

func (u UtxoDetail) Pack(writer io.Writer) error {
	err := serialize.PackInt64(writer, u.Amount)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, u.BlockHeight)
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
	return nil
}

func (u *UtxoDetail) UnPack(reader io.Reader) error {
	var err error
	u.Amount, err = serialize.UnPackInt64(reader)
	if err != nil {
		return err
	}
	u.BlockHeight, err = serialize.UnPackUint32(reader)
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
	return nil
}

type UtxoDetailPrintAble struct {
	Amount       int64
	BlockHeight  uint32
	Address      string
	ScriptPubKey string
}

func (u *UtxoDetail) GetUtxoDetailPrintAble() UtxoDetailPrintAble {
	var utxoDetailPrintAble UtxoDetailPrintAble
	utxoDetailPrintAble.Amount = u.Amount
	utxoDetailPrintAble.BlockHeight = u.BlockHeight
	utxoDetailPrintAble.Address = u.Address
	utxoDetailPrintAble.ScriptPubKey = hex.EncodeToString(u.ScriptPubKey.GetScriptBytes())
	return utxoDetailPrintAble
}

func (u *UtxoDetailPrintAble) GetUtxoDetail() (UtxoDetail, error) {
	var utxoDetail UtxoDetail
	utxoDetail.Amount = u.Amount
	utxoDetail.BlockHeight = u.BlockHeight
	utxoDetail.Address = u.Address
	bytesScript, err := hex.DecodeString(u.ScriptPubKey)
	if err != nil {
		return UtxoDetail{}, err
	}
	utxoDetail.ScriptPubKey.SetScriptBytes(bytesScript)
	return utxoDetail, nil
}
