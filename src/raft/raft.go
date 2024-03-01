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

/* rqi list
What should be locked?
Initialize correctly
Check Figure 2 and make sure you doing everything
Read all hints
Track the state of all variables
*/

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {

}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state. rqi: when is this needed?
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int

	role int //0 = follower, 1 = candidate, 2 = leader
	received_append_gave_vote bool //for follower: if it has received an append from the current leader in the current election timeout period, or gave a vote to someone
	received_vote_from []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// fmt.Printf("%d: GetState trying to lock...\n", rf.me)
	rf.mu.Lock()
	// fmt.Printf("%d: GetState locked!\n", rf.me)
	defer rf.mu.Unlock()
	// Your code here (3A).

	cur_term, is_leader := rf.currentTerm, rf.role == 2
	// fmt.Printf("%d: GetState. Term: %d, Leader: %t \n", rf.me, cur_term, is_leader)
	return cur_term, is_leader
}

func (rf *Raft) convertTo(new_role int) {
	// fmt.Printf("%d: convertTo. old_role: %d, new_role: %d\n", rf.me, rf.role, new_role)
	rf.role = new_role
	if(new_role == 0){
		rf.received_append_gave_vote = false
	} else if(new_role == 1){
		rf.received_vote_from = make([]bool, len(rf.peers))
	} else if(new_role == 2){
		//nothing yet
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	// fmt.Printf("%d: RequestVote args.Term: %d args.CandidateId: %d\n", rf.me, args.Term, args.CandidateId)

	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if(rf.role == 1 || rf.role == 2){
			rf.convertTo(0)
		}
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term != rf.currentTerm{
		panic("args.Term != rf.currentTerm")
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.received_append_gave_vote = true
		return
	}

	// Else, do not grant vote
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

type RequestVoteReplyAttempt struct {
	timed_out bool
	reply   *RequestVoteReply
}

func (rf *Raft) getRequestVoteChannel(server int, args *RequestVoteArgs, return_channel chan RequestVoteReplyAttempt) {
	reply := RequestVoteReply{}
	// fmt.Printf("%d: getRequestVoteChannel starting up to server %d....\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	// fmt.Printf("%d: getRequestVoteChannel. ok: %t server: %d\n", rf.me, ok, server)
	return_channel <- RequestVoteReplyAttempt{!ok, &reply}
}

func (rf *Raft) getRequestVoteChannelTimeout(return_channel chan RequestVoteReplyAttempt, timeout int) {
	time.Sleep(time.Duration(timeout) * time.Millisecond)
	return_channel <- RequestVoteReplyAttempt{true, nil}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%d: AppendEntries. Term: %d, Leader: %d\n", rf.me, args.Term, args.LeaderId)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if(rf.role == 1 || rf.role == 2){
			rf.convertTo(0)
		}
		rf.received_append_gave_vote = true
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	
	if rf.role == 2 {
		panic("Should NOT be the leader here!")
	} else if rf.role == 1 {
		rf.convertTo(0) //Candidate rule: If AppendEntries RPC received from new leader: convert to follower
	}
	rf.received_append_gave_vote = true // Received an append from the current leader

	// Reply with success!
	reply.Term = rf.currentTerm
	reply.Success = true
}

type AppendEntriesReplyAttempt struct {
	timed_out bool
	reply   *AppendEntriesReply
}

func (rf *Raft) getAppendEntriesChannel(server int, args *AppendEntriesArgs, return_channel chan AppendEntriesReplyAttempt) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return_channel <- AppendEntriesReplyAttempt{!ok, &reply}
}

func (rf *Raft) getAppendEntriesChannelTimeout(return_channel chan AppendEntriesReplyAttempt, timeout int) {
	time.Sleep(time.Duration(timeout) * time.Millisecond)
	return_channel <- AppendEntriesReplyAttempt{true, nil}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection(){
	if(rf.role != 1){
		panic("Not a candidate!")
	}
	// fmt.Printf("%d: Starting election\n", rf.me)
	//increase current term
	rf.currentTerm += 1
	//vote for self
	rf.votedFor = rf.me
	rf.received_vote_from = make([]bool, len(rf.peers))
	rf.received_vote_from[rf.me] = true

	//send RequestVote RPCs to all other servers
	channels := make([]chan RequestVoteReplyAttempt, len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		if(i == rf.me){
			continue
		}

		channels[i] = make(chan RequestVoteReplyAttempt)
		args := RequestVoteArgs{rf.currentTerm, rf.me}
		go rf.getRequestVoteChannel(i, &args, channels[i])
		timeout_val := 10
		go rf.getRequestVoteChannelTimeout(channels[i], timeout_val)
	}

	// fmt.Printf("%d: Sent RequestVote RPCs to all other servers\n", rf.me)

	vote_talley := 1 //vote for self
	for i := 0; i < len(rf.peers); i++{
		if(i == rf.me){
			continue
		}
		reply_attempt := <- channels[i]
		if(!reply_attempt.timed_out && reply_attempt.reply.Term > rf.currentTerm){
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			// fmt.Printf("%d: Received higher term %d\n", rf.me, reply_attempt.reply.Term)
			if(reply_attempt.reply.VoteGranted){
				panic("Should not have voted!")
			}
			rf.currentTerm = reply_attempt.reply.Term
			rf.votedFor = -1
			rf.convertTo(0)
			return
		} else if(!reply_attempt.timed_out && reply_attempt.reply.VoteGranted){
			vote_talley += 1
		}
	}

	// fmt.Printf("%d Finished receiving!\n", rf.me)

	// If votes received from majority of servers: become leader
	if vote_talley*2 > len(rf.peers){
		rf.convertTo(2)
	}
}

func (rf *Raft) sendHeartbeats(){
	append_entries_channels := make([]chan AppendEntriesReplyAttempt, len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		if(i == rf.me){
			continue
		}
		append_entries_channels[i] = make(chan AppendEntriesReplyAttempt)
		go rf.getAppendEntriesChannel(i, &AppendEntriesArgs{rf.currentTerm, rf.me}, append_entries_channels[i])
		timeout_val := 10
		go rf.getAppendEntriesChannelTimeout(append_entries_channels[i], timeout_val)
	}
	for i := 0; i < len(rf.peers); i++{
		if(i == rf.me){
			continue
		}
		reply_attempt := <- append_entries_channels[i]

		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if(!reply_attempt.timed_out && !reply_attempt.reply.Success && reply_attempt.reply.Term > rf.currentTerm){
			rf.currentTerm = reply_attempt.reply.Term
			rf.votedFor = -1
			rf.convertTo(0)
			return
		}
	}
}

func (rf *Raft) electionTimeout(){
	ms := 500 + (rand.Int63() % 500)
	time.Sleep(time.Duration(ms) * time.Millisecond)
}
func (rf *Raft) ticker() {
	rf.electionTimeout()

	for !rf.killed() {
		rf.mu.Lock()
		// fmt.Printf("%d: Top of the ticker. Current role: %d\n", rf.me, rf.role)
		// Your code here (3A)
		// Check if a leader election should be started.
		if(rf.role == 0){ //currently a follower
			//check if time since last_append_time 
			if(!rf.received_append_gave_vote){
				//convert to candidate
				// fmt.Printf("%d: Converted to candidate, starting election\n", rf.me)
				rf.convertTo(1)
			}
		}
		if (rf.role == 1){ //currently a candidate
			rf.startElection()
		}

		if(rf.role == 2){ //currently a leader, or just became one. 
			//Send Heartbeats
			
			rf.sendHeartbeats()
			
			// Restricted to 10 heartbeats per second
			ms := 100
			time.Sleep(time.Duration(ms) * time.Millisecond)

			// fmt.Printf("%d: Unlocking at Heartbeat. Current role: %d\n", rf.me, rf.role)
			rf.mu.Unlock()
			continue
		}

		// fmt.Printf("%d: Unlocking at Election Timeout. Current role: %d\n", rf.me, rf.role)
		rf.received_append_gave_vote = false //reset whether an append was received from the leader or gave a vote
		rf.mu.Unlock()
		// Reset randomized election timeout
		// rqi: changed this to wait between 500 and 1000 milliseconds
		rf.electionTimeout()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.role = 0 // initially a follower
	rf.received_append_gave_vote = false
	rf.received_vote_from = make([]bool, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
