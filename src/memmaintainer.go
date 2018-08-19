package main

import (
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"runtime"
	"runtime/debug"
	"time"
)

func doMemMaintain(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	for {
		var mStat runtime.MemStats
		runtime.ReadMemStats(&mStat)
		if mStat.HeapIdle > config.MemMaintainConfig.HeapIdleSizeMax {
			runtime.GC()
			debug.FreeOSMemory()
		}
		time.Sleep(5 * time.Second)
	}
}

func startMemMaintainer() uint64 {
	return goroutineMgr.GoroutineCreatePn("memorymaintainer", doMemMaintain, nil)
}
