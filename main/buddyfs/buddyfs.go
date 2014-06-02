package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/buddyfs/gobuddyfs"
	"github.com/buddyfs/go-chord"
)

var PORT uint = 9000
var TIMEOUT time.Duration = time.Duration(20 * time.Millisecond)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	f, err := os.Create("buddyfs.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// memStore := gobuddyfs.NewMemStore()

	var listen string = fmt.Sprintf("localhost:%d", PORT)
	trans, _ := chord.InitTCPTransport(listen, TIMEOUT)
	var conf *chord.Config = chord.DefaultConfig(listen)
	r, _ := chord.Create(conf, trans)
	kvStore := chord.NewKVStoreClient(r)

	err = fs.Serve(c, gobuddyfs.NewBuddyFS(kvStore))
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
