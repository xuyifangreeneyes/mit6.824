package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = 1

var mu sync.Mutex

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		mu.Lock()
		log.Printf(format + "\n", a...)
		mu.Unlock()
	}
	return
}
