package main

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
