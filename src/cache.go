package main

import "github.com/mutalisk999/bitcoin-lib/src/bigint"

type SlotCache struct {
	AddrTrxsAdd map[string]map[string]int
	UtxosAdd    map[string]UtxoDetail
	UtxosDel    map[string]int
	RawTrxsAdd  map[string][]byte
}

func (s *SlotCache) Initialize() {
	s.AddrTrxsAdd = make(map[string]map[string]int)
	s.UtxosAdd = make(map[string]UtxoDetail)
	s.UtxosDel = make(map[string]int)
	s.RawTrxsAdd = make(map[string][]byte)
}

func (s *SlotCache) Clear() {
	s.AddrTrxsAdd = make(map[string]map[string]int)
	s.UtxosAdd = make(map[string]UtxoDetail)
	s.UtxosDel = make(map[string]int)
	s.RawTrxsAdd = make(map[string][]byte)
}

func (s *SlotCache) AddAddrTrx(addrStr string, trxId bigint.Uint256) {
	trxIdsMapByAddr, ok := s.AddrTrxsAdd[addrStr]
	if !ok {
		trxIdsMapByAddr = make(map[string]int)
	}
	trxIdsMapByAddr[trxId.GetHex()] = 0
	s.AddrTrxsAdd[addrStr] = trxIdsMapByAddr
}

func (s *SlotCache) GetUtxo(utxoSrc UtxoSource) (UtxoDetail, bool) {
	utxoDetail, ok := s.UtxosAdd[utxoSrc.ToString()]
	return utxoDetail, ok
}

func (s *SlotCache) AddUtxo(utxoSrc UtxoSource, utxoDetail UtxoDetail) {
	s.UtxosAdd[utxoSrc.ToString()] = utxoDetail
}

func (s *SlotCache) DelUtxo(utxoSrc UtxoSource) {
	utxoSrcStr := utxoSrc.ToString()
	_, ok := s.UtxosAdd[utxoSrcStr]
	if ok {
		delete(s.UtxosAdd, utxoSrcStr)
	} else {
		s.UtxosDel[utxoSrcStr] = 0
	}
}

func (s *SlotCache) AddRawTrx(trxIdStr string, rawTrxData []byte) {
	s.RawTrxsAdd[trxIdStr] = rawTrxData
}

func (s *SlotCache) CalcObjectCacheWeight() uint32 {
	return uint32(len(s.AddrTrxsAdd)*20 + len(s.UtxosAdd) + len(s.UtxosDel) + len(s.RawTrxsAdd)*100)
}

type PendingCache struct {
	AddrTrxs []AddrTrxsPair
	Utxos    []UtxoPair
	RawTrxs  []RawTrxPair
}

func (p *PendingCache) Initialize() {
	p.AddrTrxs = make([]AddrTrxsPair, 0, 50000)
	p.Utxos = make([]UtxoPair, 0, 50000)
	p.RawTrxs = make([]RawTrxPair, 0, 50000)
}

func (p *PendingCache) Clear() {
	p.AddrTrxs = p.AddrTrxs[:0]
	p.Utxos = p.Utxos[:0]
	p.RawTrxs = p.RawTrxs[:0]
}

func (p *PendingCache) AddAddrTrxsPair(addrTrxsPair AddrTrxsPair) {
	p.AddrTrxs = append(p.AddrTrxs, addrTrxsPair)
}

func (p *PendingCache) AddUtxoPair(utxoPair UtxoPair) {
	p.Utxos = append(p.Utxos, utxoPair)
}

func (p *PendingCache) AddRawTrxPair(rawTrxPair RawTrxPair) {
	p.RawTrxs = append(p.RawTrxs, rawTrxPair)
}

var slotCache *SlotCache
var pendingCache *PendingCache
