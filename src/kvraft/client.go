package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader     int32
	id         int64
	identifier int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	DPrintf("!!!!in client: make new clerk %s", fmt.Sprintf("%p", ck))
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	ck.identifier++
	identifier := fmt.Sprintf("%d", ck.id) + " " + strconv.Itoa(ck.identifier)
	args := GetArgs{Key: key, Identifier: identifier, ClientId: ck.id, OpIndex: ck.identifier}
	DPrintf("in client Get: identifier is %s",identifier)
	leader := atomic.LoadInt32(&ck.leader)
	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrTimeout {
				continue
			} else if reply.Err == ErrNoKey {
				break
			}
		}
		leader = ck.nextLeader(leader)
		time.Sleep(100 * time.Millisecond)
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.identifier++
	identifier := fmt.Sprintf("%d", ck.id) + " " + strconv.Itoa(ck.identifier)
	args := PutAppendArgs{Key: key, Value: value, Op: op, Identifier: identifier, ClientId: ck.id, OpIndex: ck.identifier}
	DPrintf("in client PutAppend: identifier is %s",identifier)
	leader := atomic.LoadInt32(&ck.leader)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.PutAppendErr == OK {
				return
			} else if reply.PutAppendErr == ErrTimeout {
				continue
			}
		}
		leader = ck.nextLeader(leader)
		time.Sleep(100 * time.Millisecond)
	}

}
func (ck *Clerk) nextLeader(current int32) int32 {
	next := (current + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leader, next)
	return next
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}