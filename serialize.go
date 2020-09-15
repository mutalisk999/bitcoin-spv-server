package main

import (
	"bytes"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/serialize"
	"io"
)

func trxIdsToBytes(trxIds []bigint.Uint256) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := serialize.PackCompactSize(bufWriter, uint64(len(trxIds)))
	if err != nil {
		return []byte{}, err
	}
	for _, trxId := range trxIds {
		err = trxId.Pack(bufWriter)
		if err != nil {
			return []byte{}, err
		}
	}
	return bytesBuf.Bytes(), nil
}

func trxIdsFromBytes(bytesTrxIds []byte) ([]bigint.Uint256, error) {
	bufReader := io.Reader(bytes.NewBuffer(bytesTrxIds))
	ui64, err := serialize.UnPackCompactSize(bufReader)
	if err != nil {
		return []bigint.Uint256{}, err
	}
	trxIds := make([]bigint.Uint256, ui64, ui64)
	for i := 0; i < int(ui64); i++ {
		var trxId bigint.Uint256
		err = trxId.UnPack(bufReader)
		if err != nil {
			return []bigint.Uint256{}, err
		}
		trxIds[i] = trxId
	}
	return trxIds, nil
}

func trxSeqsToBytes(trxSeqs []uint32) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := serialize.PackCompactSize(bufWriter, uint64(len(trxSeqs)))
	if err != nil {
		return []byte{}, err
	}
	for _, trxSeq := range trxSeqs {
		err = serialize.PackUint32(bufWriter, trxSeq)
		if err != nil {
			return []byte{}, err
		}
	}
	return bytesBuf.Bytes(), nil
}

func trxSeqsFromBytes(bytesTrxSeqs []byte) ([]uint32, error) {
	bufReader := io.Reader(bytes.NewBuffer(bytesTrxSeqs))
	ui64, err := serialize.UnPackCompactSize(bufReader)
	if err != nil {
		return []uint32{}, err
	}
	trxSeqs := make([]uint32, ui64, ui64)
	for i := 0; i < int(ui64); i++ {
		var ui32 uint32
		ui32, err = serialize.UnPackUint32(bufReader)
		if err != nil {
			return []uint32{}, err
		}
		trxSeqs[i] = ui32
	}
	return trxSeqs, nil
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

func uint256ToBytes(uint256 bigint.Uint256) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := uint256.Pack(bufWriter)
	if err != nil {
		return []byte{}, err
	}
	return bytesBuf.Bytes(), nil
}

func uint256FromBytes(bytesUint256 []byte) (bigint.Uint256, error) {
	var ui256 bigint.Uint256
	bufReader := io.Reader(bytes.NewBuffer(bytesUint256))
	err := ui256.UnPack(bufReader)
	if err != nil {
		return bigint.Uint256{}, err
	}
	return ui256, nil
}

func uint32ToBytes(ui32 uint32) ([]byte, error) {
	bytesBuf := bytes.NewBuffer([]byte{})
	bufWriter := io.Writer(bytesBuf)
	err := serialize.PackUint32(bufWriter, ui32)
	if err != nil {
		return []byte{}, err
	}
	return bytesBuf.Bytes(), nil
}

func uint32FromBytes(bytesUint32 []byte) (uint32, error) {
	var ui32 uint32
	bufReader := io.Reader(bytes.NewBuffer(bytesUint32))
	ui32, err := serialize.UnPackUint32(bufReader)
	if err != nil {
		return 0, err
	}
	return ui32, nil
}
