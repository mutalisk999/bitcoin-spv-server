package main

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"sync"
)

type SlotCache struct {
	AddrTrxsAdd map[string]map[string]int
	UtxosAdd    map[string]UtxoDetail
	UtxosDel    map[string]int
	RawTrxsAdd  map[string][]byte
	Mutex       *sync.Mutex
}

func (s *SlotCache) Initialize() {
	s.AddrTrxsAdd = make(map[string]map[string]int)
	s.UtxosAdd = make(map[string]UtxoDetail)
	s.UtxosDel = make(map[string]int)
	s.RawTrxsAdd = make(map[string][]byte)
	s.Mutex = new(sync.Mutex)
}

func (s *SlotCache) Clear() {
	s.AddrTrxsAdd = make(map[string]map[string]int)
	s.UtxosAdd = make(map[string]UtxoDetail)
	s.UtxosDel = make(map[string]int)
	s.RawTrxsAdd = make(map[string][]byte)
	s.Mutex = new(sync.Mutex)
}

func (s *SlotCache) AddAddrTrx(addrStr string, trxId bigint.Uint256) {
	s.Mutex.Lock()
	trxIdsMapByAddr, ok := s.AddrTrxsAdd[addrStr]
	if !ok {
		trxIdsMapByAddr = make(map[string]int)
	}
	trxIdsMapByAddr[string(trxId.GetData())] = 0
	s.AddrTrxsAdd[addrStr] = trxIdsMapByAddr
	s.Mutex.Unlock()
}

func (s *SlotCache) GetUtxo(utxoSrc UtxoSource) (UtxoDetail, bool) {
	utxoSrcStr, err := utxoSrc.ToStreamString()
	if err != nil {
		return UtxoDetail{}, false
	}
	s.Mutex.Lock()
	utxoDetail, ok := s.UtxosAdd[utxoSrcStr]
	s.Mutex.Unlock()
	return utxoDetail, ok
}

func (s *SlotCache) AddUtxo(utxoSrc UtxoSource, utxoDetail UtxoDetail) error {
	utxoSrcStr, err := utxoSrc.ToStreamString()
	if err != nil {
		return err
	}
	s.Mutex.Lock()
	s.UtxosAdd[utxoSrcStr] = utxoDetail
	s.Mutex.Unlock()
	return nil
}

func (s *SlotCache) DelUtxo(utxoSrc UtxoSource) error {
	utxoSrcStr, err := utxoSrc.ToStreamString()
	if err != nil {
		return err
	}
	s.Mutex.Lock()
	_, ok := s.UtxosAdd[utxoSrcStr]
	if ok {
		delete(s.UtxosAdd, utxoSrcStr)
	} else {
		s.UtxosDel[utxoSrcStr] = 0
	}
	s.Mutex.Unlock()
	return nil
}

func (s *SlotCache) AddRawTrx(trxIdStr string, rawTrxData []byte) {
	s.Mutex.Lock()
	s.RawTrxsAdd[trxIdStr] = rawTrxData
	s.Mutex.Unlock()
}

func (s *SlotCache) CalcObjectCacheWeight() int64 {
	var addrTrxsWeight int64 = 0
	var utxosWeight int64 = 0
	var rawTrxsWeight int64 = 0
	var totalWeight int64 = 0

	s.Mutex.Lock()
	for _, v := range s.AddrTrxsAdd {
		addrTrxsWeight = addrTrxsWeight + int64(30) + int64(32)*int64(len(v))
	}
	utxosWeight = int64(108)*int64(len(s.UtxosAdd)) + int64(36)*int64(len(s.UtxosDel))
	for _, v := range s.RawTrxsAdd {
		rawTrxsWeight = rawTrxsWeight + int64(32) + int64(len(v))
	}
	totalWeight = addrTrxsWeight + utxosWeight + rawTrxsWeight
	s.Mutex.Unlock()
	return totalWeight
}

var slotCache *SlotCache
