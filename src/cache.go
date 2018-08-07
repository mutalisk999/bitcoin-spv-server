package main

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"strconv"
)

type BlockCache struct {
	AddressTrxs []AddressTrxPair
	TrxUtxos    []TrxUtxoPair
	RawTrxs     []RawTrxPair
	AddrChanged []string
}

func (b *BlockCache) AddAddressTrxPair(addressTrxPair AddressTrxPair) {
	b.AddressTrxs = append(b.AddressTrxs, addressTrxPair)
}

func (b *BlockCache) AddTrxUtxoPair(trxUtxoPair TrxUtxoPair) {
	b.TrxUtxos = append(b.TrxUtxos, trxUtxoPair)
}

func (b *BlockCache) AddRawTrxPair(rawTrxPair RawTrxPair) {
	b.RawTrxs = append(b.RawTrxs, rawTrxPair)
}

func (b *BlockCache) AddAddrChanged(addrStr string) {
	isInAddressCache := false
	for _, addr := range b.AddrChanged {
		if addrStr == addr {
			isInAddressCache = true
			break
		}
	}
	if !isInAddressCache {
		b.AddrChanged = append(b.AddrChanged, addrStr)
	}
}

var blockCache *BlockCache

type UtxoMemCache struct {
	UtxoDetailMemMap map[string]UtxoDetail
}

func (u UtxoMemCache) Get(utxoSrc UtxoSource) (UtxoDetail, bool) {
	memCacheKey := utxoSrc.TrxId.GetHex() + "," + strconv.Itoa(int(utxoSrc.Vout))
	utxoDetail, ok := u.UtxoDetailMemMap[memCacheKey]
	return utxoDetail, ok
}

func (u *UtxoMemCache) Add(utxoSrc UtxoSource, utxoDetail UtxoDetail) {
	memCacheKey := utxoSrc.TrxId.GetHex() + "," + strconv.Itoa(int(utxoSrc.Vout))
	u.UtxoDetailMemMap[memCacheKey] = utxoDetail
}

func (u *UtxoMemCache) Remove(utxoSrc UtxoSource) {
	memCacheKey := utxoSrc.TrxId.GetHex() + "," + strconv.Itoa(int(utxoSrc.Vout))
	delete(u.UtxoDetailMemMap, memCacheKey)
}

var utxoMemCache *UtxoMemCache

type AddressTrxsMemCache struct {
	AddressTrxsMap map[string][]bigint.Uint256
}

func (a *AddressTrxsMemCache) Set(addrStr string, trxIds []bigint.Uint256) {
	a.AddressTrxsMap[addrStr] = trxIds
}

func (a *AddressTrxsMemCache) Get(addrStr string) ([]bigint.Uint256, bool) {
	trxIds, ok := a.AddressTrxsMap[addrStr]
	return trxIds, ok
}

func (a *AddressTrxsMemCache) Add(addrStr string, trxId bigint.Uint256) {
	trxIdsByAddr, ok := a.AddressTrxsMap[addrStr]
	isNewAddr := false
	if !ok {
		var err error
		trxIdsByAddr, err = addressTrxDBMgr.DBGet(addrStr)
		if err != nil && err.Error() == LevelDBNotFound{
			a.AddressTrxsMap[addrStr] = []bigint.Uint256{trxId}
			isNewAddr = true
		}
	}
	if !isNewAddr {
		isInTrxIds := false
		for _, trxIdAddr := range trxIdsByAddr {
			if bigint.IsUint256Equal(&trxIdAddr, &trxId) {
				isInTrxIds = true
				break
			}
		}
		// duplicated trxid
		if !isInTrxIds {
			trxIdsByAddr = append(trxIdsByAddr, trxId)
			a.AddressTrxsMap[addrStr] = trxIdsByAddr
		}
	}
}

var addressTrxsMemCache *AddressTrxsMemCache
