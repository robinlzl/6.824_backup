package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpRetBundle struct {
	RetOp Op
	Ret   map[string]string
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string

	Identifier string
	ClientId   int64
	OpIndex    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// record put or append result
	kv map[string]string
	// leader return result to client through channel
	retCh map[int]chan OpRetBundle
	// avoid applying multiply times
	lastApplied int
	deDup       map[int64]interface{}
	get         map[int]chan string
	done        map[int]chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		Value:      "",
		Type:       "GET",
		Identifier: args.Identifier,
		ClientId:   args.ClientId,
		OpIndex:    args.OpIndex,
	}
	DPrintf("in server %d Get: start to send op key %s, Op GET", kv.me, args.Key)
	idx, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("in server %d Get: is LEADER!!!!!!!!!! idx:  %d", kv.me, idx)
	kv.mu.Lock()
	if _, ok := kv.retCh[idx]; !ok {
		kv.retCh[idx] = make(chan OpRetBundle)
	}
	ch := kv.retCh[idx]
	kv.mu.Unlock()
	var retBundle OpRetBundle
	select {
	case retBundle = <-ch:
		if retBundle.RetOp.Identifier != op.Identifier {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Value = retBundle.Ret["value"]
		reply.Err = retBundle.Ret["err"]
		DPrintf("in server %d Get  idx:  %d, reply = %+v", kv.me, idx, reply)
		return
	case <-time.After(400 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.retCh, idx)
		reply.Err = ErrTimeout
		kv.mu.Unlock()
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		Type:       args.Op,
		Identifier: args.Identifier,
		ClientId:   args.ClientId,
		OpIndex:    args.OpIndex,
	}
	DPrintf("in server %d PutAppend: start to send op key %s, value %s, Op %s", kv.me, args.Key, args.Value, args.Op)
	idx, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.PutAppendErr = ErrWrongLeader
		return
	}
	DPrintf("in server %d PutAppend:  is LEADER!!!!!!!!!! idx:  %d", kv.me, idx)
	kv.mu.Lock()
	if _, ok := kv.retCh[idx]; !ok {
		kv.retCh[idx] = make(chan OpRetBundle)
	}
	ch := kv.retCh[idx]
	kv.mu.Unlock()
	var retBundle OpRetBundle
	select {
	case retBundle = <-ch:
		if retBundle.RetOp.Identifier != op.Identifier {
			reply.PutAppendErr = ErrWrongLeader
			return
		}
		reply.PutAppendErr = OK
		DPrintf("in server %d PutAppend idx:  %d, reply = %+v", kv.me, idx, reply)
		return
	case <-time.After(400 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.retCh, idx)
		reply.PutAppendErr = ErrTimeout
		kv.mu.Unlock()
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.readSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	//kv.kv = make(map[string]string)
	kv.retCh = make(map[int]chan OpRetBundle)
	kv.get = make(map[int]chan string)
	kv.done = make(map[int]chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//go kv.createSnapshotAndTruncateLog(persister, maxraftstate)
	go kv.doApply()
	//go func() {
	//	for {
	//		//		DPrintf("in server %d StartKVServer: ready to receive appych, kv.applyIndex %d", kv.me, kv.applyIndex)
	//		if kv.killed() {
	//			return
	//		}
	//		select {
	//		case msg := <- kv.applyCh:
	//			kv.mu.Lock()
	//			if msg.CommandValid == false {
	//				DPrintf("in server %d StartKVServer: install snapshot offset %d, term %d\n", kv.me, msg.CommandIndex, msg.CommandTerm)
	//				kv.readSnapshot(msg.Command.([]byte))
	//				kv.mu.Unlock()
	//				continue
	//			}
	//			op := msg.Command.(Op)
	//			kv.lastApplied = kv.max(kv.lastApplied, msg.CommandIndex)
	//
	//			DPrintf("in server %d StartKVServer: received appych, msg.CommandIndex %d", kv.me, msg.CommandIndex)
	//
	//
	//			DPrintf("in server %d StartKVServer: about to process request, command.identifier %s", kv.me, op.Identifier)
	//			ret := kv.processRequest(op)
	//			//kv.seenIdentifier[op.Identifier] = ret
	//			kv.sentMegToRPCHandle(&msg, ret, op)
	//
	//			DPrintf("in server %d StartKVServer: received appych, set msg.CommandIndex %d", kv.me, msg.CommandIndex)
	//			kv.createSnapshotAndTruncateLog(persister, maxraftstate)
	//			kv.mu.Unlock()
	//		}
	//
	//
	//	}
	//}()
	// You may need initialization code here.
	return kv
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if msg.CommandIndex <= kv.lastApplied {
		DPrintf("reject ApplyMsg due to smaller Index. lastApplied=%d v=%+v", kv.lastApplied, msg)
		return
	}
	op := msg.Command.(Op)

	if op.Type == "GET" {
		kv.lastApplied = msg.CommandIndex
	} else if op.Type == "Append" {
		kv.kv[op.Key] += op.Value
	} else {
		kv.kv[op.Key] = op.Value
	}
	kv.deDup[op.ClientId] = msg.Command

}

//func (kv *KVServer) sentMegToRPCHandle(msg *raft.ApplyMsg, ret map[string]string, command Op) {
//
//	if ch, ok := kv.retCh[msg.CommandIndex]; ok {
//
//		bundle := OpRetBundle{
//			Ret: ret,
//			RetOp: command,
//		}
//		select {
//		case ch <- bundle:
//
//		case <-time.After(500 * time.Millisecond):
//
//		}
//	}
//}

//func (kv *KVServer) createSnapshotAndTruncateLog(persister *raft.Persister) {
//	if kv.maxraftstate == -1 {
//		return
//	}
//	if kv.maxraftstate <= persister.RaftStateSize() {
//		DPrintf("in server %d createSnapshotAndTruncateLog: about to truncate log persister.RaftStateSize %d", kv.me, persister.RaftStateSize())
//		retDictCopy := make(map[string]string)
//		clientAppliedIndexCopy := make(map[int64]int)
//		for k, v := range kv.kv {
//			retDictCopy[k] = v
//		}
//		for k, v := range kv.deDup {
//			clientAppliedIndexCopy[k] = v
//		}
//		appliedIndex := kv.lastApplied
//		go kv.rf.Snapshot(appliedIndex, )
//	}
//}
func (kv *KVServer) max(a int, b int) int {

	if a >= b {
		return a
	}
	return b
}

func (kv *KVServer) readSnapshot(data []byte) {

	if kv.maxraftstate == -1 || data == nil || len(data) < 1 { // bootstrap without any state?
		kv.kv = make(map[string]string)
		kv.lastApplied = 0
		kv.deDup = make(map[int64]interface{})
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var retDict map[string]string
	var lastApplied int
	var deDup map[int64]interface{}
	if d.Decode(&retDict) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&deDup) != nil {
		panic("##################Decode failed")
	}

	if kv.lastApplied > lastApplied {
		return
	}
	kv.kv = retDict
	kv.lastApplied = lastApplied
	kv.deDup = deDup

}

func (kv *KVServer) doApply() {

	for msg := range kv.applyCh {
		//		DPrintf("in server %d StartKVServer: ready to receive appych, kv.applyIndex %d", kv.me, kv.applyIndex)
		if kv.killed() {
			return
		}

		if msg.CommandValid == false {
			DPrintf("in server %d StartKVServer: install snapshot offset %d, term %d\n", kv.me, msg.CommandIndex, msg.CommandTerm)
			kv.readSnapshot(msg.Command.([]byte))
			continue
		}

		DPrintf("in server %d StartKVServer: received appych, msg.CommandIndex %d", kv.me, msg.CommandIndex)

		kv.apply(msg)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		op := msg.Command.(Op)
		switch op.Type {
		case "GET":
			getCh := kv.get[msg.CommandIndex]
			val := ""
			if s, ok := kv.kv[op.Key]; ok {
				val = s
			}
			kv.mu.Unlock()
			go func() {
				getCh <- val
			}()
		case "Put", "Append":
			putCh := kv.done[msg.CommandIndex]
			kv.mu.Unlock()
			go func() {
				putCh <- struct{}{}
			}()
		}
		//kv.sentMegToRPCHandle(&msg, ret, op)
		//kv.createSnapshotAndTruncateLog(persister, maxraftstate)
		kv.mu.Unlock()
	}
}

