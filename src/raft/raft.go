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
Consider state after unlocking

TODO:
Apply 3C optimization
More robust role changing checks
*/

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	Command interface{}
	Term int
	Index int
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
	received_append_or_given_vote bool //for follower: if it has received an append from the current leader in the current election timeout period
	received_vote_from []bool

	applyCh chan ApplyMsg
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
	LastLogIndex int
	LastLogTerm int
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
		rf.persist()
		
		// fmt.Printf("RequestVote conversion\n")
		rf.convertToFollower()
		go rf.followerTicker(rf.currentTerm)
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
	up_to_date := false
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		up_to_date = true
	} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		if args.LastLogIndex >= len(rf.log) - 1 {
			up_to_date = true
		}
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && up_to_date {
		if rf.role != 0 {
			panic("rf.role != 0")
		}

		rf.votedFor = args.CandidateId
		rf.persist()
		rf.received_append_or_given_vote = true

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	XTerm int
	XIndex int
	XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.XIndex = -1
	reply.XTerm = -1
	reply.XLen = -1

	// fmt.Printf("%d: AppendEntries. Term: %d, Leader: %d\n", rf.me, args.Term, args.LeaderId)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		// fmt.Printf("AppendEntries conversion")
		rf.convertToFollower()
		go rf.followerTicker(rf.currentTerm)
		if(rf.role != 0){
			panic("Should be a follower here!")
		}
		rf.received_append_or_given_vote = true
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.role == 1 {
		// fmt.Printf("AppendEntries conversion2")
		rf.convertToFollower() //Candidate rule: If AppendEntries RPC received from new leader: convert to follower
		go rf.followerTicker(rf.currentTerm)
	}

	if rf.role != 0 {
		panic("Should be a follower here!")
	}
	rf.received_append_or_given_vote = true // Received an append from the current leader
	// fmt.Printf("%d: AppendEntries. Term: %d, Leader: %d. Received an append\n", rf.me, args.Term, args.LeaderId)

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex >= len(rf.log) {
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			reply.XIndex = args.PrevLogIndex
			reply.XLen = len(rf.log)

			for i := 0; i <= reply.XIndex; i++ {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i
					break
				}
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that
	// follow it (§5.3)
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+1+i < len(rf.log) && rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			//delete from args.PrevLogIndex+1+i and onwards
			rf.log = rf.log[:args.PrevLogIndex+1+i]
			rf.persist()
			break
		}
	}
	// Append any new entries not already in the log
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+1+i >= len(rf.log) {
			if args.PrevLogIndex+1+i != len(rf.log){
				panic("Weird indexing")
			}
			rf.log = append(rf.log, args.Entries[i])
			rf.persist()
		}
	}

	// fmt.Printf("%d: AppendEntries. Term: %d, Leader: %d. Log: %v\n", rf.me, args.Term, args.LeaderId, rf.log)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	// Reply with success!
	reply.Term = rf.currentTerm
	reply.Success = true
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
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if(rf.role != 2){
		return -1, -1, false
	}

	

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true

	// fmt.Printf("%d: Start called. Index: %d, Term: %d, Leader: %t\n", rf.me, index, term, isLeader)

	// Your code here (3B).
	rf.log = append(rf.log, LogEntry{command, term, index})
	rf.persist()
	
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

func (rf *Raft) electionTimeout() {
	ms := 500 + (rand.Int63() % 500)
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) startRequestVotes(term int) {
	rf.mu.Lock()
	// fmt.Printf("%d: Starting election. Term: %d\n", rf.me, term)
	defer rf.mu.Unlock()
	if(rf.role != 1 || rf.currentTerm != term){
		return
	}
	// fmt.Printf("%d: Starting election\n", rf.me)
	//increase current term
	//vote for self

	//send RequestVote RPCs to all other servers
	channel := make(chan RequestVoteReplyAttempt, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++{
		// fmt.Printf("%d: RequestVote i=%d currentTerm=%d\n", rf.me, i, rf.currentTerm)
		if(i == rf.me){
			continue
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log)-1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		go rf.getRequestVoteChannel(i, &args, channel)
	}

	votes_needed := len(rf.peers)/2 + 1
	vote_talley := 1
	for i := 0; i < len(rf.peers)-1; i++{
		// fmt.Printf("%d: RequestVote i=%d\n", rf.me, i)
		rf.mu.Unlock()
		reply_attempt := <- channel
		rf.mu.Lock()

		// fmt.Printf("%d: Received reply %v %v\n", rf.me, reply_attempt, reply_attempt.reply)
		
		if(rf.currentTerm != term || rf.role != 1){
			// fmt.Printf("%d: Exiting election\n", rf.me)
			return
		}

		if(!reply_attempt.timed_out && reply_attempt.reply.Term > rf.currentTerm){
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			// fmt.Printf("%d: Received higher term %d\n", rf.me, reply_attempt.reply.Term)
			if(reply_attempt.reply.VoteGranted){
				panic("Should not have voted!")
			}
			rf.currentTerm = reply_attempt.reply.Term
			rf.persist()
			// fmt.Printf("StartElection conversion")
			rf.convertToFollower()
			go rf.followerTicker(rf.currentTerm)
			return
		} else if(!reply_attempt.timed_out && reply_attempt.reply.VoteGranted){
			vote_talley += 1

			if(vote_talley >= votes_needed){
				// fmt.Printf("%d: Received majority\n", rf.me)
				rf.convertToLeader()
				go rf.leaderTicker(rf.currentTerm)
				return
			}
		}
	}
}



func (rf *Raft) convertToFollower(){
	// fmt.Printf("%d: convertToFollower term = %d\n", rf.me, rf.currentTerm)
	rf.role = 0
	rf.votedFor = -1
	rf.persist()
	rf.received_append_or_given_vote = false
}

func (rf *Raft) followerTicker(term int){
	//election timeout --> start election
	rf.mu.Lock()

	// fmt.Printf("%d: followerTicker beginning\n", rf.me)
	defer rf.mu.Unlock()
	if(rf.role != 0 || term != rf.currentTerm){
		return
	}

	for {
		rf.mu.Unlock()

		
		rf.electionTimeout()
		

		rf.mu.Lock()
		if(term != rf.currentTerm || rf.role != 0){ //check state
			// fmt.Printf("%d: followerTicker term changed\n", rf.me)
			return
		}

		if(!rf.received_append_or_given_vote){ //convert to candidate
			// fmt.Printf("%d: followerTicker converting to candidate\n", rf.me)
			rf.convertToCandidate()
			go rf.candidateTicker(rf.currentTerm)
			return
		}
		// fmt.Printf("%d: followerTicker continuing\n", rf.me)
		rf.received_append_or_given_vote = false
	}
}

func (rf *Raft) convertToCandidate(){
	rf.role = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	// fmt.Printf("%d: convertToCandidate term = %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) candidateTicker(term int){
	rf.mu.Lock()
	// fmt.Printf("%d: candidateTicker beginning\n", rf.me)
	defer rf.mu.Unlock()

	if(rf.role != 1 || rf.currentTerm != term){ //should have already increased term when became candidate
		return
	}

	go rf.startRequestVotes(term)
	rf.mu.Unlock()
	rf.electionTimeout()
	rf.mu.Lock()
	if(rf.currentTerm != term || rf.role != 1){
		// fmt.Printf("%d: Exiting candidateTicker, term = %d, currentTerm = %d, role = %d\n", rf.me, term, rf.currentTerm, rf.role)
		return
	}
	rf.convertToCandidate()
	go rf.candidateTicker(rf.currentTerm)
}

func (rf *Raft) convertToLeader(){
	// fmt.Printf("%d: convertToLeader term = %d\n", rf.me, rf.currentTerm)
	rf.role = 2
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) updateLogs(term int, follower int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if(rf.currentTerm != term || rf.role != 2){
		return
	}

	// fmt.Printf("%d: Updating logs\n", rf.me)
	args := AppendEntriesArgs{
		Term: term,
		LeaderId: rf.me,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
		Entries: make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}

	if(len(rf.log)-1 >= rf.nextIndex[follower]){
		args.PrevLogIndex = rf.nextIndex[follower]-1 
	} else {
		//send empty appendEntries (heartbeat)
		args.PrevLogIndex = len(rf.log)-1
	}
	if(args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log)){
		panic("Out of bounds!")
	}
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	for i := args.PrevLogIndex+1; i < len(rf.log); i++{
		args.Entries = append(args.Entries, rf.log[i])
	}

	orig_nextIndex := rf.nextIndex[follower]
	orig_matchIndex := rf.matchIndex[follower]

	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[follower].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if rf.currentTerm != term || rf.role != 2{
		return
	}
	if rf.nextIndex[follower] != orig_nextIndex || rf.matchIndex[follower] != orig_matchIndex{
		//safety check, maybe too strict?
		return
	}
	if !ok {
		return
	}

	if reply.Success {
		// : update nextIndex and matchIndex for follower (§5.3)
		// fmt.Printf("%d: Updating logs success, updating nextIndex and matchIndex\n", rf.me)
		rf.nextIndex[follower] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[follower] = max(rf.matchIndex[follower], args.PrevLogIndex + len(args.Entries))
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.persist()
		rf.convertToFollower()
		go rf.followerTicker(rf.currentTerm)
		// fmt.Printf("%d: Updating logs failed, converting to follower\n", rf.me)
		return
	}

	// conflict: decrement nextIndex and retry (§5.3)
	// fmt.Printf("%d: Updating logs failed, decrementing nextIndex and retrying\n", rf.me)
	if reply.XLen == -1{
		panic("XLen is -1!")
	}
	if reply.XTerm != -1 {
		last_XTerm_index := -1
		for i := 0; i < len(rf.log); i++{
			if rf.log[i].Term == reply.XTerm{
				last_XTerm_index = i
			}
		}

		if last_XTerm_index == -1 {
			rf.nextIndex[follower] = reply.XIndex
		} else {
			rf.nextIndex[follower] = last_XTerm_index
		}
	} else {
		rf.nextIndex[follower] = reply.XLen
	}
	go rf.updateLogs(term, follower)
}

func (rf *Raft) updateLogsTickers(term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if(rf.currentTerm != term || rf.role != 2){
		return
	}

	for{
		// fmt.Printf("%d: updateLogsTickers term = %d\n", rf.me, term)
		for i := 0; i < len(rf.peers); i++{
			if(i == rf.me){
				continue
			}
			go rf.updateLogs(term, i)
		}
		rf.mu.Unlock()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if(rf.currentTerm != term || rf.role != 2){
			// fmt.Printf("%d: Exiting leaderTicker, term = %d, currentTerm = %d, role = %d\n", rf.me, term, rf.currentTerm, rf.role)
			return
		}
	}
}

func (rf *Raft) applyMsgTicker() {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	for{
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			rf.mu.Lock()
			continue
		}
		rf.lastApplied += 1
		// fmt.Printf("%d: increased lastApplied: %v\n", rf.me, rf.lastApplied)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
}
func (rf *Raft) updateCommitIndexTicker(term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("%d: updateCommitIndexTicker term = %d\n", rf.me, term)

	if(rf.currentTerm != term || rf.role != 2){
		return
	}

	for{
		for N := len(rf.log)-1; N > rf.commitIndex; N--{
			//check if a majority of matchIndex[i] >= N
			peers_have := 0
			for i := 0; i < len(rf.peers); i++{
				if(i == rf.me){
					peers_have += 1
					continue
				}

				if(rf.matchIndex[i] >= N){
					peers_have += 1
				}
			}
			if peers_have * 2 > len(rf.peers) && rf.log[N].Term == rf.currentTerm{
				rf.commitIndex = N
				break
			}
		}

		rf.mu.Unlock()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if(rf.currentTerm != term || rf.role != 2){
			return
		}
	}
}

func (rf *Raft) leaderTicker(term int){
	rf.mu.Lock()
	// fmt.Printf("%d: leaderTicker beginning\n", rf.me)
	defer rf.mu.Unlock()

	if(rf.currentTerm != term){
		return
	}

	go rf.updateLogsTickers(term)
	go rf.updateCommitIndexTicker(term)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{nil, -1, 0})

	rf.role = 0 // initially a follower
	rf.received_append_or_given_vote = false
	rf.received_vote_from = make([]bool, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.convertToFollower()

	go rf.followerTicker(rf.currentTerm)
	go rf.applyMsgTicker()


	return rf
}
