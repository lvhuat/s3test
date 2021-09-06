package main

import (
	"flag"
	"runtime"

	"github.com/lvhuat/s3test/s3file"
)

var (
	files    = flag.Int("files", 1000, "total file number")
	routines = flag.Int("routines", 1000, "total worker routines")
	cores    = flag.Int("cores", 0, "max procs")
	name     = flag.String("name", "", "test name")
)

func main() {
	if *cores != 0 {
		runtime.GOMAXPROCS(*cores)
	}

	flag.Parse()
	s3file.Loop(*files, *routines, *name)
}
