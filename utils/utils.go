package utils

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// 获取当前程序的进程id
func GetCurrPid() string {
	return strconv.Itoa(os.Getpid())
}

// 获取当前go协程id
func GetCurrGid() string {
	buf := make([]byte, 128)
	buf = buf[:runtime.Stack(buf, false)]
	stackInfo := string(buf)
	return strings.TrimSpace(strings.Split(strings.Split(stackInfo, "[running]")[0], "goroutine")[1])
}

func GetPidAndGidStr() string {
	return fmt.Sprintf("%s_%s", GetCurrPid(), GetCurrGid())
}
