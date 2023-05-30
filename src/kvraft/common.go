package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)


// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identifier     string
	ClientId       int64
	OpIndex        int

}

type PutAppendReply struct {
	PutAppendErr string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Identifier  string
	ClientId    int64
	OpIndex     int

}

type GetReply struct {
	Err      string
	Value    string
}
