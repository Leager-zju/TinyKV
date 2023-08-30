package raftstore

import "github.com/pingcap-incubator/tinykv/log"

const DEBUG bool = false

func DPrintf(format string, a ...interface{}) {
	if DEBUG {
		log.Infof(format, a...)
	}
}
