package log

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
)

// Debugging
var Debug = flag.Bool("debug", false, "whether to print debug logs")

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

var debugEnabled bool

func SetDebug(enabled bool) {
	debugEnabled = enabled
}

func DPrintf(format string, a ...interface{}) {
	if debugEnabled || *Debug {
		_, path, line, _ := runtime.Caller(1 /*skip*/)
		file := filepath.Base(path)
		msg := fmt.Sprintf(format, a...)
		log.Printf("%s (%v:%v)", msg, file, line)
	}
}
