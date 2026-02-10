//go:build !windows

package main

import (
	"syscall"
	"time"
)

func processCPUTime() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	user := time.Duration(usage.Utime.Nano())
	sys := time.Duration(usage.Stime.Nano())
	return user + sys
}
