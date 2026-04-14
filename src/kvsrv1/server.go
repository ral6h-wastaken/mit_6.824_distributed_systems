package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type versionedData struct {
	data    string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]versionedData
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]versionedData),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := (*args).Key
	
	vd, present := kv.store[key]
	if !present {
		reply.Err = rpc.ErrNoKey
		return 
	}

	reply.Err = rpc.OK
	reply.Version = vd.version
	reply.Value = vd.data

	/* return */
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	version := args.Version

	vd, present := kv.store[key]
	if !present {
		if version == 0 { 
			kv.store[key] = versionedData{
				data:    value,
				version: 1,
			}
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}

	curVer := vd.version
	if curVer != version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.store[key] = versionedData{
		data:    value,
		version: curVer + 1,
	}

	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
