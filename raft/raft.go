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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab2raft/labrpc"
)

// Keep math/rand imported even before election timers are implemented
var _ = rand.Intn

// Server states
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	dead int32 // set by Kill()

	// Persistent state on all servers (Figure 2)
	currentTerm int
	votedFor    int
	log         []LogEntry // 1-indexed; log[0] is a dummy entry with Term: 0

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	state       ServerState

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Synchronization & channels
	applyCh           chan ApplyMsg
	applyCond         *sync.Cond
	lastElectionReset time.Time
	electionTimeout   time.Duration
}

// ============================================================================
// PART 1: STATE HELPERS & ELECTION TIMERS
// ============================================================================

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	// TODO (Part 1): Lock rf.mu, read currentTerm and whether state == Leader
	return term, isleader
}

// Helper: get last log index and term (must be called with rf.mu held)
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	// TODO (Part 1): Return last index in rf.log and its Term
	return 0, 0
}

// Helper: reset randomized election timeout (must be called with rf.mu held)
func (rf *Raft) resetElectionTimer() {
	// TODO (Part 1): Update rf.lastElectionReset to time.Now() and set randomized rf.electionTimeout
}

// Helper: transition to Follower (must be called with rf.mu held)
func (rf *Raft) becomeFollower(term int) {
	// TODO (Part 1): Set state = Follower; if term > currentTerm, update currentTerm and reset votedFor = -1
}

// Election timer loop
func (rf *Raft) electionTicker() {
	// TODO (Part 1): Loop while !rf.killed(), check if electionTimeout elapsed for non-Leader, and call rf.startElection()
}

// ============================================================================
// PART 2: LEADER ELECTION & RPC HANDLERS
// ============================================================================

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// TODO (Part 2): Implement RequestVote receiver rules (§5.1, §5.2, §5.4)
}

// Start a new election (must be called with rf.mu held)
func (rf *Raft) startElection() {
	// TODO (Part 2): Transition to Candidate, increment term, vote for self, reset timer, and send RequestVote RPCs to all peers
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// TODO (Part 2 / Part 3): Implement AppendEntries receiver rules + ConflictTerm/ConflictIndex fast backup
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ============================================================================
// PART 3: LOG REPLICATION, HEARTBEATS, COMMITMENT & APPLIER
// ============================================================================

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	// TODO (Part 3): If Leader, append entry to rf.log, update matchIndex/nextIndex, call broadcastAppendEntries(), and return immediately

	return index, term, isLeader
}

// Periodic Heartbeats
func (rf *Raft) heartbeatTicker() {
	// TODO (Part 3): Loop every ~100ms while !rf.killed(); if Leader, call rf.broadcastAppendEntries()
}

// Log Replication & Fast Backup
func (rf *Raft) broadcastAppendEntries() {
	// TODO (Part 3): Send AppendEntries RPCs in parallel to all peers, handle replies, update matchIndex/nextIndex, and fast backup on conflict
}

// Advance commitIndex (must be called with rf.mu held)
func (rf *Raft) updateCommitIndex() {
	// TODO (Part 3): Find N > commitIndex replicated on a majority where log[N].Term == currentTerm, update commitIndex, and Signal applyCond
}

// State Machine Applier Goroutine
func (rf *Raft) applier() {
	// TODO (Part 3): Wait on applyCond until commitIndex > lastApplied, copy newly committed entries, unlock, and send ApplyMsg to applyCh
}

// ============================================================================
// LIFECYCLE: KILL & MAKE (PRE-CONFIGURED FOR ALL PARTS)
// ============================================================================

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	if rf.applyCond != nil {
		rf.applyCond.Broadcast()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1
	// Dummy entry at index 0 so valid log entries are 1-indexed as specified in Figure 2
	rf.log = []LogEntry{{Command: nil, Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	// Start background goroutines
	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
