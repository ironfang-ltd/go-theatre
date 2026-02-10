package main

import (
	"syscall"
	"time"
)

func processCPUTime() time.Duration {
	var creation, exit, kernel, user syscall.Filetime
	h, _ := syscall.GetCurrentProcess()
	syscall.GetProcessTimes(h, &creation, &exit, &kernel, &user)
	k := filetimeToDuration(kernel)
	u := filetimeToDuration(user)
	return k + u
}

func filetimeToDuration(ft syscall.Filetime) time.Duration {
	// FILETIME is in 100-nanosecond intervals.
	return time.Duration(int64(ft.HighDateTime)<<32|int64(ft.LowDateTime)) * 100
}
