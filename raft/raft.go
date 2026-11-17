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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term) // updates term, clears votedFor
	}
	reply.Term = rf.currentTerm

	lastIdx, lastTerm := rf.lastLogIndexAndTerm()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer() // only reset when the vote is actually granted
		reply.VoteGranted = true
	}
}

// Start a new election (must be called with rf.mu held)
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	lastIdx, lastTerm := rf.lastLogIndexAndTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	votes := 1 // own vote; only touched while holding rf.mu

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// stale reply: no longer a candidate, or a newer election started
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.state = Leader
					last, _ := rf.lastLogIndexAndTerm()
					for p := range rf.peers {
						rf.nextIndex[p] = last + 1
						rf.matchIndex[p] = 0
					}
					rf.matchIndex[rf.me] = last
					go rf.broadcastAppendEntries() // assert leadership immediately
				}
			}
		}(i)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// valid leader for this term: step down if needed and reset the timer
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// log consistency check with fast-backup hints
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && rf.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = ci
		return
	}

	// append, truncating only on a real conflict (so delayed/duplicate RPCs
	// can't chop off entries that already match)
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	// commit update, bounded by the last entry this RPC verified
	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}

	reply.Success = true
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

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

	// Trigger an immediate broadcast without waiting for the next heartbeat tick
	rf.broadcastAppendEntries()

	return index, term, isLeader
}

// Periodic Heartbeats
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isLeader {
			rf.broadcastAppendEntries()
		}

		// Loop every 100ms to stay within the <= 10 heartbeats/sec rule
		time.Sleep(100 * time.Millisecond)
	}
}

// Log Replication & Fast Backup
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateToPeer(i)
		}
	}
}

// Send AppendEntries to a single peer and handle the reply
func (rf *Raft) replicateToPeer(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peer]
	if nextIdx < 1 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := append([]LogEntry{}, rf.log[nextIdx:]...)

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore delayed replies if we are no longer Leader or the term has changed
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	// Step down if peer has a higher term
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.updateCommitIndex()
	} else {
		rf.backupNextIndex(peer, reply)
	}
}

// Fast backup of nextIndex on AppendEntries rejection
func (rf *Raft) backupNextIndex(peer int, reply *AppendEntriesReply) {
	if reply.ConflictTerm == -1 {
		// Follower's log was shorter than PrevLogIndex
		rf.nextIndex[peer] = reply.ConflictIndex
	} else {
		// search leader's log from back to front for reply.ConflictTerm
		lastIndexWithTerm := -1
		for idx := len(rf.log) - 1; idx >= 1; idx-- {
			if rf.log[idx].Term == reply.ConflictTerm {
				lastIndexWithTerm = idx
				break
			}
		}
		if lastIndexWithTerm != -1 {
			rf.nextIndex[peer] = lastIndexWithTerm + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}

	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// Advance commitIndex
func (rf *Raft) updateCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Only commit entries from the leader's currentTerm by counting replicas
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 0
		for i := range rf.peers {
			// if the server have bigger term it hase already committed the entry, so we can count it as well
			if i == rf.me || rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// State Machine Applier Goroutine
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// Wait until there are new committed entries to apply
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Apply all committed but not yet applied entries
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.lastApplied = commitIndex
		rf.mu.Unlock()

		// Send ApplyMsg for each new entry to the service
		for i, entry := range entries {
			msg := ApplyMsg{
				Index:   lastApplied + 1 + i,
				Command: entry.Command,
			}
			rf.applyCh <- msg
		}
	}
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
