package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/user"
	"runtime/pprof"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/buddyfs/buddystore"
	"github.com/buddyfs/gobuddyfs"
	"github.com/golang/glog"
)

var PORT uint = 9000
var TIMEOUT time.Duration = time.Duration(20 * time.Millisecond)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
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

	// kvStore := gobuddyfs.NewMemStore()

	/*
		var listen string = fmt.Sprintf("localhost:%d", PORT)
		trans, _ := buddystore.InitTCPTransport(listen, TIMEOUT)
		var conf *buddystore.Config = buddystore.DefaultConfig(listen)
		r, _ := buddystore.Create(conf, trans)
		kvStore := buddystore.NewKVStoreClient(r)
	*/

	// TODO: Replace OS username with PGP key
	currentUser, _ := user.Current()
	glog.Infof("Logging in as user: %s", currentUser.Name)
	config := &buddystore.BuddyStoreConfig{MyID: currentUser.Name}
	bStore := buddystore.NewBuddyStore(config)
	kvStore, errno := bStore.GetMyKVClient()

	if errno != buddystore.OK {
		glog.Fatalf("Error getting KVClient instance from Buddystore. %d", errno)
	}

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
