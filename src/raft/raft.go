package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	state       string
	timer       time.Time
	timeout     int
	aeTimeout   int
	logs        []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	offset      int
	applyCond   *sync.Cond
}

type Log struct {
	Index int
	Term  int
	Cmd   interface{}
}

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.serializeState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.offset)
	return w.Bytes()
}

func (rf *Raft) serializeSnapshot(kvPairs map[string]string, newOffset int, clientAppliedIndex map[int64]int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kvPairs)
	e.Encode(newOffset)
	e.Encode(clientAppliedIndex)
	return w.Bytes()

}

func (rf *Raft) TruncateLog(kvPairs map[string]string, newOffset int, clientAppliedIndex map[int64]int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if newOffset <= rf.offset {
		return
	}
	DPrintf("in TruncateLog: server %d term %d about to truncate log in offset %d\n", rf.me, rf.currentTerm, newOffset)
	//DPrintf("in TruncateLog: kvPairs %d\n", kvPairs)
	//DPrintf("in TruncateLog: seenIdentifier %d\n", seenIdentifier)

	rf.logs = rf.logs[newOffset-rf.offset:]
	DPrintf("in TruncateLog: server %d, newOffset %d, offset %d, len(rf.logs) %d\n", rf.me, newOffset, rf.offset, len(rf.logs))
	rf.offset = newOffset
	rf.lastApplied = rf.offset
	rf.commitIndex = rf.offset
	state := rf.serializeState()
	snapShot := rf.serializeSnapshot(kvPairs, newOffset, clientAppliedIndex)
	rf.persister.SaveStateAndSnapshot(state, snapShot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.logs = make([]Log, 0)
		rf.logs = append(rf.logs, Log{Cmd: -1, Term: 0})
		rf.offset = 0
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	var offset int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&offset) != nil {
		panic("##################Decode failed")
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	rf.offset = offset
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("in Snapshot: before server %d rf.lastApplied = %d, term %d, len(logs) = %d, log =  %+v， index = %d\n", rf.me, rf.lastApplied, rf.currentTerm, len(rf.logs), rf.logs, index)
	if index <= rf.offset {
		return
	}
	tempLogs := make([]Log, 0)
	if index+1-rf.offset <= len(rf.logs)-1 {
		tempLogs = rf.logs[index+1-rf.offset:]
	}
	term := rf.logs[index-rf.offset].Term
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Cmd: -1, Term: term})
	rf.logs = append(rf.logs, tempLogs...)
	rf.offset = index
	data := rf.serializeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	DPrintf("in Snapshot: after server %d rf.lastApplied = %d, term %d, len(logs) = %d, index = %d, rf.logs = %+v\n", rf.me, rf.lastApplied, rf.currentTerm, len(rf.logs), index, rf.logs)

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//
	rf.mu.Lock()

	if args.Term <= rf.currentTerm {
		DPrintf("in RequestVote: follower %d term %d reject vote for %d term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {

		DPrintf("in RequestVote: server %d become follower in term %d, reply term %d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.persist()
		if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term ||
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term &&
				//	len(rf.logs) - 1 > args.LastLogIndex) {
				len(rf.logs)-1+rf.offset > args.LastLogIndex) {
			DPrintf("in RequestVote: follower %d term %d not grant vote for %d term %d\n", rf.me, rf.logs[len(rf.logs)-1].Term, args.CandidateId, args.LastLogTerm)
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
		DPrintf("in RequestVote: %s %d term %d grant vote for %d term %d\n", rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		rf.timer = time.Now()
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer = time.Now()
	if args.Term < rf.currentTerm {
		DPrintf("in AppendEntries: follower %d reject appendEntries from leader %d\n", rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("in AppendEntries: follower %d get appendEntries from leader %d\n", rf.me, args.LeaderId)
	reply.Success = true
	rf.currentTerm = args.Term
	rf.persist()
	rf.timer = time.Now()
	rf.state = FOLLOWER
	reply.Term = rf.currentTerm
	if args.PrevLogIndex >= len(rf.logs)+rf.offset {
		DPrintf("in AppendEntries: follower %d don't have prev log index %d\n", rf.me, args.PrevLogIndex)
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.offset {
		diff := rf.offset - args.PrevLogIndex
		if diff >= len(args.Entries) {
			reply.Success = true
			return
		}
		args.PrevLogIndex = rf.offset
		args.Entries = args.Entries[diff:]
	}
	if rf.logs[args.PrevLogIndex-rf.offset].Term != args.PrevLogTerm {
		DPrintf("in AppendEntries: follower %d prev log index's term %d doesn't equal to leader's term %d, rf.logs = %+v\n", rf.me, rf.logs[args.PrevLogIndex-rf.offset].Term, args.PrevLogTerm, rf.logs)
		reply.Success = false
		return
	}

	if len(args.Entries) > 0 { // have log to copy from leader to local

		DPrintf("in AppendEntries: follower %d about to append log\n", rf.me)
		DPrintf("in AppendEntries: follower %d, length of the args %d,  prev logIndexTerm %d, args pervLogTerm %d\n",
			rf.me,
			len(args.Entries),
			rf.logs[args.PrevLogIndex-rf.offset].Term,
			args.PrevLogTerm,
		)
		idx := args.PrevLogIndex + 1
		for i := 0; i < len(args.Entries); i++ {
			if idx <= rf.offset {
				idx++
				continue
			}
			if idx == len(rf.logs)+rf.offset {
				DPrintf("in AppendEntries: follower %d about to append log index, %d cmd %d, term %d\n", rf.me, idx, args.Entries[i].Cmd, args.Entries[i].Term)
				idx++
				rf.logs = append(rf.logs, args.Entries[i])
				continue
			}
			if args.Entries[i].Cmd == rf.logs[idx-rf.offset].Cmd && args.Entries[i].Term == rf.logs[idx-rf.offset].Term {
				idx++
				continue
			}
			if args.Entries[i].Cmd != rf.logs[idx-rf.offset].Cmd || args.Entries[i].Term != rf.logs[idx-rf.offset].Term {
				rf.logs = rf.logs[:(idx - rf.offset)]
			}
			DPrintf("in AppendEntries: follower %d about to append log index %d, cmd %d, term %d\n", rf.me, idx, args.Entries[i].Cmd, args.Entries[i].Term)
			rf.logs = append(rf.logs, args.Entries[i])
			idx++

		}
		rf.persist()
	}

	// AE RPC receiver 5: if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex { // have log already stored need to apply
		DPrintf("in AppendEntries: leaderCommit log %d,  follower %d about to apply log, rf.commitIndex %d, rf.lastApplied = %d, rf.logs = %+v\n", args.LeaderCommit, rf.me, rf.commitIndex, rf.lastApplied, rf.logs)
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > len(rf.logs)-1+rf.offset {
			rf.commitIndex = len(rf.logs) - 1 + rf.offset
		}
		rf.applyCond.Broadcast()
	}
	// Rules for servers,  All servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	//for rf.commitIndex > rf.lastApplied {
	//	rf.lastApplied++
	//	command := rf.logs[rf.lastApplied-rf.offset].Cmd
	//	index := rf.lastApplied
	//	DPrintf("in AppendEntries:  server %d about to apply log %d, cmd %d, term %d\n", rf.me, rf.lastApplied, command, rf.logs[rf.lastApplied-rf.offset].Term)
	//	tmpTerm := rf.logs[rf.lastApplied-rf.offset].Term
	//	rf.ApplyLog(true, index, command, tmpTerm)
	//}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.offset {
		DPrintf("in InstallSnapshot:  follower %d fails to install snapshot because lastIncludedIndex %d, <= rf.offset %d\n", rf.me, args.LastIncludedIndex, rf.offset)
		rf.mu.Unlock()
		return
	}
	tmpLogs := []Log{{Cmd: -1, Term: args.LastIncludedTerm}}
	//if args.LastIncludedIndex >= len(rf.logs)+rf.offset {
	//	rf.logs = make([]Log, 0)
	//	rf.logs = append(rf.logs, Log{Cmd: -1, Term: args.LastIncludedTerm})
	//} else {
	//	rf.logs = rf.logs[args.LastIncludedIndex-rf.offset:]
	//}
	if args.LastIncludedIndex < len(rf.logs)+rf.offset {
		tmpLogs = append(tmpLogs, rf.logs[args.LastIncludedIndex-rf.offset+1:]...)
	}
	rf.logs = tmpLogs
	DPrintf("in InstallSnapshot:  follower %d about to install snapshot index %d, term %d, rf.logs = %+v\n", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.logs)
	rf.offset = args.LastIncludedIndex
	rf.commitIndex = rf.offset
	rf.lastApplied = rf.offset
	state := rf.serializeState()
	rf.persister.SaveStateAndSnapshot(state, args.Data)
	snapshotTerm := args.LastIncludedTerm
	snapshotIndex := args.LastIncludedIndex
	snapshot := args.Data
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
		Snapshot:      snapshot,
	}
	//rf.ApplyLog(false, args.LastIncludedIndex, args.Data, args.LastIncludedTerm)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		DPrintf("in sendRequestVote: candidate %d become follower in term %d, reply term %d\n", rf.me, rf.currentTerm, reply.Term)
		rf.persist()
	}
	return ok && reply.VoteGranted
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, bool, bool) {
	reachable := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	isFollower := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !reachable {
		return false, reachable, isFollower
	}
	if args.Term != rf.currentTerm {
		return false, false, isFollower
	}
	if !reply.Success {
		DPrintf("in sendAppendEntries: reply not successful, leader %d current term %d, reply term %d\n", rf.me, rf.currentTerm, reply.Term)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			DPrintf("in sendAppendEntries: leader %d become follower in term %d\n", rf.me, rf.currentTerm)
			isFollower = true
			rf.persist()
		}
	}
	return reply.Success, reachable, isFollower
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//DPrintf("in sendInstallSnapshot: leader %d send InstallSnapshot to %d in term %d, offset %d\n", rf.me, server, rf.currentTerm, args.LastIncludedIndex)
	reachable := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !reachable {
		DPrintf("in sendInstallSnapshot: leader %d send InstallSnapshot to server %d in term %d, offset %d, result not reachable\n", rf.me, server, rf.currentTerm, args.LastIncludedIndex)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		DPrintf("in sendInstallSnapshot: leader %d become follower in term %d\n", rf.me, rf.currentTerm)
		rf.persist()
	} else {
		rf.nextIndex[server] = rf.offset + 1
		rf.matchIndex[server] = rf.offset
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if !isLeader {
		return index, term, isLeader
	}
	rf.logs = append(rf.logs, Log{Cmd: command, Term: term, Index: rf.offset + len(rf.logs)})
	rf.persist()
	rf.timer = time.Now()
	index = len(rf.logs) - 1 + rf.offset
	DPrintf("!!!!!!!!!!! in start: leader %d starts to append log cmd %d term %d\n", rf.me, command, term)
	go rf.AttemptHeartBeat()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			rf.FollowerCheckTimeout()

		case CANDIDATE:
			// send VoteFor
			go rf.AttemptVote()
			rf.CandidateCheckTimeout()

		case LEADER:
			DPrintf("in Make:  in leader  %d\n", rf.me)
			rf.LeaderCheckTimeout()
		}
	}
}

func (rf *Raft) FollowerCheckTimeout() {
	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		now := time.Now()
		if int(now.Sub(rf.timer)/time.Millisecond) >= rf.timeout {
			rf.timeout = rand.Intn(500) + 300
			rf.timer = now
			DPrintf("in FollowerCheckTimeout: follower %d become candidate in term %d\n", rf.me, rf.currentTerm+1)
			rf.state = CANDIDATE
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CandidateCheckTimeout() {

	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			break
		}
		now := time.Now()
		if int(now.Sub(rf.timer)/time.Millisecond) >= rf.timeout {
			rf.timeout = rand.Intn(500) + 300
			rf.timer = now
			go rf.AttemptVote()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) LeaderCheckTimeout() {

	for {
		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.timer = time.Now()
			rf.mu.Unlock()
			break
		}
		now := time.Now()
		if int(now.Sub(rf.timer)/time.Millisecond) >= rf.aeTimeout {
			rf.aeTimeout = rand.Intn(50) + 100
			rf.timer = now
			go rf.AttemptHeartBeat()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) AttemptVote() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.voteFor = rf.me
	rf.timer = time.Now()
	rf.persist()
	vote := 1
	done := false
	currentTerm := rf.currentTerm
	me := rf.me
	lastLogIndex := len(rf.logs) - 1 + rf.offset
	lastLogTerm := rf.logs[lastLogIndex-rf.offset].Term
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {

		if i == me {
			continue
		}
		go func(server int) {

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			DPrintf("in AttemptVote: %d sent vote request to %d on term %d\n", me, server, currentTerm)
			voteGranted := rf.sendRequestVote(server, &args, &RequestVoteReply{})
			if !voteGranted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("in AttemptVote: in candidate, follower %d grant vote for %d\n", server, me)
			vote++
			if done || vote <= len(rf.peers)/2 {
				return
			}
			done = true
			DPrintf("in AttemptVote:  candidate %d become leader\n", me)
			DPrintf("in AttemptVote:  leader %d log length %d\n", me, len(rf.logs))
			rf.state = LEADER
			for i := 0; i < len(rf.peers); i++ {
				DPrintf("in AttemptVote:  assign nextIndex %d length %d\n", i, rf.offset+len(rf.logs))
				//rf.nextIndex[i] = len(rf.logs)
				//rf.matchIndex[i] = 0
				rf.nextIndex[i] = rf.offset + len(rf.logs)
				rf.matchIndex[i] = rf.offset + len(rf.logs) - 1
			}
		}(i)
	}

}

func (rf *Raft) AttemptHeartBeat() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		DPrintf("in sendAppendEntries: leader %d is already follower in term %d\n", rf.me, rf.currentTerm)
		return
	}
	term := rf.currentTerm
	leaderId := rf.me
	DPrintf("in AttemptHeartBeat:  %d about to sent appendEntries in term %d\n", leaderId, term)
	leaderCommit := rf.commitIndex
	logLength := len(rf.logs)
	offset := rf.offset
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		nextLogIndex := rf.nextIndex[i]
		prevLogIndex := nextLogIndex - 1
		DPrintf("in AttemptHeartBeat:   %d next log index %d, prev log index %d\n", i, nextLogIndex, prevLogIndex)
		//prevLogTerm := rf.logs[prevLogIndex].Term

		if prevLogIndex < offset {
			DPrintf("in AttemptHeartBeat:  leader %d about to sent InstallSnapshot to %d because prevLogIndex %d is less than offset %d\n", rf.me, i, prevLogIndex, offset)
			installSnapshotArgs := InstallSnapshotArgs{
				Term:              term,
				LastIncludedIndex: offset,
				LastIncludedTerm:  rf.logs[0].Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.sendInstallSnapshot(i, &installSnapshotArgs, &InstallSnapshotReply{})
			continue
		}
		DPrintf("in AttemptHeartBeat:  leader %d about to sent attemptHeartBeat to %d, prevLogIndex %d, offset %d, nextLogIndex %d, len(rf.logs) %d, logLength %d\n", rf.me, i, prevLogIndex, offset, nextLogIndex, len(rf.logs), logLength)
		prevLogTerm := rf.logs[prevLogIndex-offset].Term
		entries := make([]Log, 0)
		//if logLength - 1 >= nextLogIndex {
		//	entries = rf.logs[nextLogIndex:logLength]
		//}
		if logLength-1+offset >= nextLogIndex {
			entries = rf.logs[nextLogIndex-offset : logLength]
		}
		go func(server int, prevLogIndex int, pervLogTerm int, entries []Log, leaderCommit int, logLength int, offset int) {

			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			DPrintf("in AttemptHeartBeat:  %d about to sent appendEntries to %d with log length %d\n", leaderId, server, len(entries))
			logCopied, reachable, isFollower := rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
			DPrintf("in AttemptHeartBeat:  server %d response logCopied %t reachable %t, isFollower %t\n", server, logCopied, reachable, isFollower)
			if !reachable {
				DPrintf("in AttemptHeartBeat:  server %d is not reachable\n", server)
				return
			}
			if isFollower {
				DPrintf("in AttemptHeartBeat:  leader %d become follower because of server %d\n", leaderId, server)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.offset != offset {
				DPrintf("in AttemptHeartBeat:  %d cannot sent appendEntries to %d due to current offset %d not equals to old %d\n", leaderId, server, rf.offset, offset)
				return
			}
			if !logCopied {
				if prevLogIndex == offset {
					//sent InstallSnapshotRPC
					DPrintf("in AttemptHeartBeat: leader %d about to send to follower %d install snapshot because prevLogIndex %d = offset %d\n", rf.me, server, prevLogIndex, offset)
					installSnapshotArgs := InstallSnapshotArgs{
						Term:              term,
						LastIncludedIndex: offset,
						LastIncludedTerm:  rf.logs[0].Term,
						Data:              rf.persister.ReadSnapshot(),
					}
					go rf.sendInstallSnapshot(server, &installSnapshotArgs, &InstallSnapshotReply{})
					return
				}
				DPrintf("in AttemptHeartBeat: server %d decreases by one. leader %d is follower? %t\n", server, rf.me, isFollower)
				//rf.nextIndex[server] /= 2
				rf.nextIndex[server] = offset + (rf.nextIndex[server]-offset)/2
				//if rf.nextIndex[server] == 0 {
				//	rf.nextIndex[server] = 1
				//}
				if rf.nextIndex[server] == offset {
					rf.nextIndex[server] = offset + 1
				}
				return
			}
			//if len(entries) == 0 {
			//	DPrintf("in AttemptHeartBeat: server %d has not copied any log\n", server)
			//	return
			//}
			DPrintf("in AttemptHeartBeat:  server %d appended log\n", server)
			//if rf.nextIndex[server] < logLength {
			//	rf.nextIndex[server] = logLength
			//}
			//if rf.matchIndex[server] < logLength - 1 {
			//	rf.matchIndex[server] = logLength - 1
			//}

			if rf.nextIndex[server] < logLength+offset {
				rf.nextIndex[server] = logLength + offset
			}
			if rf.matchIndex[server] < logLength-1+offset {
				rf.matchIndex[server] = logLength - 1 + offset
			}
			//N := logLength - 1
			N := logLength - 1 + offset
			//for ; N >= 0; N-- {
			for ; N >= rf.offset; N-- {
				cnt := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					break
				}
			}
			DPrintf("in AttemptHeartBeat:  leader %d logLength = %d, N = %d, rf.offset = %d, offset = %d\n", rf.me, logLength, N, rf.offset, offset)
			if N < rf.offset {
				return
			}
			if rf.logs[N-rf.offset].Term == term && N > rf.commitIndex {
				DPrintf("in AttemptHeartBeat:  leader %d set commitIndex to %d\n", rf.me, N)
				rf.commitIndex = N
			}
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
			//for rf.commitIndex > rf.lastApplied {
			//
			//	rf.lastApplied++
			//	//	command := rf.logs[rf.lastApplied].Cmd
			//	command := rf.logs[rf.lastApplied-rf.offset].Cmd
			//	DPrintf("in AttemptHeartBeat:  leader %d about to apply log %d, cmd %d, term %d\n", leaderId, rf.lastApplied, command, rf.logs[rf.lastApplied-rf.offset].Term)
			//	index := rf.lastApplied
			//	tmpTerm := rf.logs[rf.lastApplied-rf.offset].Term
			//	rf.ApplyLog(true, index, command, tmpTerm)
			//
			//}
		}(i, prevLogIndex, prevLogTerm, entries, leaderCommit, logLength, offset)

	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex <= rf.lastApplied
}

func (rf *Raft) DoApply(applyCh chan ApplyMsg) {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		for rf.needApply() {
			//	DPrintf("in DoApply: server %d wait 1 rf.lastApplied = %d rf.commitIndex = %d", rf.me, rf.lastApplied, rf.commitIndex)
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}

		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			DPrintf("in DoApply: server %d wait 2 rf.lastApplied = %d rf.commitIndex = %d", rf.me, rf.lastApplied, rf.commitIndex)
			rf.mu.Unlock()
			continue
		}
		rf.lastApplied += 1
		DPrintf("in DoApply: server = %d rf.lastApplied = %d rf.commitIndex = %d, rf.offset = %d rf.logs = %+v", rf.me, rf.lastApplied, rf.commitIndex, rf.offset, rf.logs)
		if rf.lastApplied-rf.offset >= len(rf.logs) {
			panic("rf.lastApplied - rf.offset >= len(rf.logs)")
		}
		entry := rf.logs[rf.lastApplied-rf.offset]
		cmd := entry.Cmd
		index := entry.Index
		term := entry.Term

		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: index,
			CommandTerm:  term,
		}
	}
}
func (rf *Raft) GetStateSize() int {

	return rf.persister.RaftStateSize()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state = FOLLOWER
	rf.timer = time.Now()
	rf.timeout = rand.Intn(500) + 2500
	rf.aeTimeout = rand.Intn(50) + 200
	rf.commitIndex = rf.offset
	rf.lastApplied = rf.offset
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + rf.offset
		rf.matchIndex[i] = len(rf.logs) + rf.offset
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.DoApply(applyCh)
	return rf
}
