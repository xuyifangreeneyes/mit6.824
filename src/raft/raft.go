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

import "sync"
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"
import "time"

type Role int

const (
	Follower Role = iota
	Leader
	Candidate
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	role              Role
	lastResetTime     time.Time           // election timer
	winElection       chan struct{}
	newVote           chan struct{}
	appendSuccess     chan struct{}
	commitIndexUpdate chan struct{}
	raftState
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type raftState struct {
	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex   []int
	matchIndex  []int
	// volatile state on candidates
	votedForMe  []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

// term1(term2) is the term of the last entries of log1(log2).
// index1(index2) is the index of the last entries of log1(log2).
// The return value is -1 whether log1 is less up-to-date than log2.
// The return value is 0 whether log1 is even up-to-date than log2.
// The return value is 1 whether log1 is more up-to-date than log2.
func compareLog(term1, index1, term2, index2 int) int {
	if term1 == term2 {
		if index1 < index2 {
			return -1
		}
		if index1 == index2 {
			return 0
		}
		return 1
	}
	if term1 < term2 {
		return -1
	}
	return 1
}

// rf must hold lock when calling the function.
func (rf *Raft) resetElectionTimer() {
	rf.lastResetTime = time.Now()
}

// rf must hold lock when calling the function.
func (rf *Raft) updateTerm(newTerm int) {
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.votedForMe = nil
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && compareLog(args.LastLogTerm, args.LastLogIndex, rf.log[lastLogIndex].Term, lastLogIndex) >= 0 {
		reply.VoteGranted = true
		rf.resetElectionTimer()
		return
	}
	reply.VoteGranted = false
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
	return ok
}

func (rf *Raft) handleRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		// TODO: log the failure of call
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		return
	}
	if args.Term != rf.currentTerm {
		// When receiving an old RPC reply, just drop the reply and return.
		return
	}
	if reply.VoteGranted {
		rf.votedForMe[server] = true
		rf.newVote <- struct{}{}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRelpy struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRelpy) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.resetElectionTimer()
	if args.PrevLogIndex != -1 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}
	startIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if startIndex + i == len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if entry.Term != rf.log[startIndex + i].Term {
			rf.log = append(rf.log[:startIndex + i], args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		lastEntryIndex := startIndex + len(args.Entries) - 1
		if args.LeaderCommit < lastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}
		// TODO: check whether it is always true
		if rf.commitIndex > oldCommitIndex {
			rf.commitIndexUpdate <- struct{}{}
		}
	}
	reply.Success = true
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesRelpy{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		// TODO: log the failure of call
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		return
	}
	if args.Term != rf.currentTerm {
		// When receiving an old RPC reply, just drop the reply and return.
		return
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.appendSuccess <- struct{}{}
	} else {
		rf.nextIndex[server] = rf.nextIndex[server] - 1
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := -1
		if prevLogIndex != -1 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		newArgs := &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm: prevLogTerm,
			Entries: rf.log[prevLogIndex + 1:],
			LeaderCommit: rf.commitIndex,
		}
		go rf.handleAppendEntries(server, newArgs)
	}
}

func (rf *Raft) checkTimeout(maxTimeInterval int64, sleepTime int) {
	for {
		rf.mu.Lock()
		t := time.Since(rf.lastResetTime).Milliseconds()
		role := rf.role
		rf.mu.Unlock()
		if role != Leader && t > maxTimeInterval {
			rf.mu.Lock()
			// double check
			if role != Leader {
				rf.role = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.resetElectionTimer()
				rf.votedForMe = make([]bool, len(rf.peers))
				rf.votedForMe[rf.me] = true
				numServers := len(rf.peers)
				args := &RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm: rf.log[len(rf.log) - 1].Term,
				}
				for i := 0; i < numServers; i++ {
					if i == rf.me {
						continue
					}
					go rf.handleRequestVote(i, args)
				}
			}
			rf.mu.Unlock()
		} else {
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}
	}
}

func (rf *Raft) checkElection() {
	for {
		<- rf.newVote
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Candidate {
				return
			}
			numVotes := 0
			for _, vote := range rf.votedForMe {
				if vote {
					numVotes++
				}
			}
			if numVotes > len(rf.peers) / 2 {
				rf.role = Leader

			}
		}()
	}
}

func (rf *Raft) checkCommit() {
	for {
		<- rf.appendSuccess
		rf.mu.Lock()
		for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
			numReplicas := 0
			for _, matchIdx := range rf.matchIndex {
				if matchIdx >= i {
					numReplicas++
				}
			}
			if numReplicas > len(rf.peers) / 2 && rf.log[i].Term == rf.currentTerm {
				rf.commitIndex = i
				rf.commitIndexUpdate <- struct{}{}
				break
			}
		}
		rf.mu.Unlock()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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


	return rf
}
