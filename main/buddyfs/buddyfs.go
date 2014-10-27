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

	"bytes"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/buddyfs/buddystore"
	"github.com/buddyfs/gobuddyfs"
	"github.com/golang/glog"
	"github.com/steveyen/gkvlite"
)

var storeType = flag.String("store", "p2p",
	"Type of backing store for filesystem. Options: mem|gkv|p2p")

var PORT uint = 9000
var TIMEOUT time.Duration = time.Duration(20 * time.Millisecond)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func getBuddyStoreClient() gobuddyfs.KVStore {
	// TODO: Replace OS username with PGP key
	currentUser, _ := user.Current()
	glog.Infof("Logging in as user: %s", currentUser.Name)
	config := &buddystore.BuddyStoreConfig{MyID: currentUser.Name}
	bStore := buddystore.NewBuddyStore(config)
	kvStore, errno := bStore.GetMyKVClient()

	if errno != buddystore.OK {
		// If there is an error instantiating the KV client, not much to do.
		// Spit out an error and die.
		glog.Fatalf("Error getting KVClient instance from Buddystore. %d", errno)
		os.Exit(1)
	}

	return kvStore
}

func getInMemoryKVStoreClient() gobuddyfs.KVStore {
	return gobuddyfs.NewMemStore()
}

func getGKVStoreClient() (gobuddyfs.KVStore, func()) {
	const buddyfs string = "BuddyFS"
	f, err := os.OpenFile("/tmp/test.gkvlite", os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		glog.Fatal(err)
	}

	s, err := gkvlite.NewStore(f)
	if err != nil {
		glog.Fatal(err)
	}

	c := s.GetCollection(buddyfs)
	if c == nil {
		c = s.SetCollection(buddyfs, bytes.Compare)
	}

	return gobuddyfs.NewGKVStore(c, s), func() {
		c.Write()
		s.Close()
		s.Flush()
		f.Sync()
	}
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

	var kvStore gobuddyfs.KVStore
	var cleanup func()

	switch *storeType {
	case "mem":
		kvStore = getInMemoryKVStoreClient()
	case "gkv":
		kvStore, cleanup = getGKVStoreClient()
	case "p2p":
		kvStore = getBuddyStoreClient()
	default:
		log.Fatal("Unknown store type", storeType)
		os.Exit(2)
	}

	if cleanup != nil {
		defer cleanup()
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
