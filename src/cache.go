package main

import "strconv"

type BlockCache struct {
	AddressTrxs []AddressTrxPair
	TrxUtxos    []TrxUtxoPair
	RawTrxs     []RawTrxPair
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
