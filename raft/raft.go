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

/* FOLLOWER
 * CANDIDATE
 * LEADER
*/

import "sync"
import "labrpc"
import "fmt"

// tells servers when it is safe to apply log entries totheir state machines. 

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// * p6 - f6 logs are composed of entries
// * each entry contains the term in chich it was created
// * and a command for the state machine
type LogEntry struct {
	Term int
	Command interface{}
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state string

	// Persistant state on all servers
	currentTerm int   // latest term server has seen
	votedFor int      // candidateId that received vote in current term
	logs []LogEntry   // log entries

	// volatile state on all servers
	commitIndex int   // index of highest log entry know to be commited 
	lastApplied int   // index of highest log entry applied to state machine

	// volatile state on leaders 
	nextIndex  []int  // for each server, index of the next log entry to send to that server
    matchIndex []int  // for each server, index of highest log entry known to be replicated on server
	applyCh chan ApplyMsg // channel to send message of type ApplyMsg

    // Timer
    timer *time.Timer

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER )

	fmt.Printf("GetState: Peer Index: %d term: %d state: %s", rf.me, term, rf.state)
	return term, isleader
}

// * used for RequestVote RPC
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

// 
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term int   // for candidate to update itself
	VoteGranted bool
}

// * vote when received request for voting
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// fmt.Printf 
	rf.mu.Lock()
	defer rf.mu.Unlock

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm // update the term of candidate
		return
	}

	// If votedFor is null or candidateID, 
	// and candidate's log is at least as up-to-date as receiver's log, grant vote
	// * (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&

	if	args.LastLogTerm > rf.logs[len(rf.logs) - 1].Term || // in go array index starts from 0
		( ( args.LastLogTerm == rf.logs[len(rf.logs) - 1].Term ) && 
		( args.LastLogIndex >= len(rf.logs) - 1 ) ){

		reply.VoteGranted = true
		reply.Term = args.Term

	} else if { 
		reply.VoteGranted = false
		reply.Term = args.Term // args.Term == rf.currentTerm

	}
	
	// if term > currentTerm, become follower!
	if arg.Term > rf.currentTerm {
		rf.currentTerm = args.Term 
		rf.votedFor = args.candidateId // has not voted in this term
		rf.state = FOLLOWER // when granting vote, become follower
		// * reset timer
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ******** functions added

// * send RequestVote RPCs to all other servers
func (rf *Raft) sendRequestVoteAll() {
	rf.mu.Lock()

	tempArgs := RequestVoteArgs{}
	args := &tempArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[len(rf.logs) - 1].Term
	
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			tempReplyArgs := RequestVoteReply{}
			replyArgs := &tempReplyArgs
			ok := rf.sendRequestVote(i, args, replyArgs) // relpy set in requestvote

			if ok {
				go rf.replyHandler(replyArgs)
			}
		}
	}
} 
// requestvote reply handler
func (rf *Raft) replyHandler(reply RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		// rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.voteCount++

		if rf.voteCount > len(rf.peers) / 2 {
			rf.winElect = true
	}

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


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// * used to initialize the Raft struct 
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.currentTerm = 0
	rf.votedFor = -1  // * -1 means null (when it vote for no one)
	// * initialize log entry, at first len(logEntry) = 0
	// * make returns a slice to that array 
	// * term is 0 at first
	rf.logs = make([]LogEntry, 0) 

	rf.commitIndex = 0
	rf.lastApplied = 0

	// * initialized to leader last log index + 1
	rf.nextIndex = make(([]int, len(peers))
	// * initialized to 0
	rf.matchIndex = make([]int, len(peers))

	//
	rf.voteCount = 0
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	
	/*
	rf.electWin = make(chan bool)
	rf.heartbeat = make(chan bool)
	*/ 
	
	// rf. readPersist(persister.ReadRaftState)
	// rf.persist()
	// rf.resetTimer() 
	
	go startServer
	
	return rf
}

func (rf *Raft) startServer() {
	// omit the loop condition so it loops forever
	for {

		switch rf.state {

		case LEADER:
			rf.sendAppendEntries() //*** send append entries to all follower
			time.Sleep(time.Millisecond * 120) // time differs 
		}

		/*
		 * If election timeout elapses without receiving AppendEntries RPC from 
		 * the current leader/ granting votes to candidate:
		 * covert to candidate 
		 */
		case FOLLOWER:
			select {
				case <- rf.heartbeat: // remain follower
				case <- time.After(time.Millisecond * time.Duration(rand.Intn(200) + 300)):
					rf.state = CANDIDATE
			}

		
		/* On conversation to candidate, start election:
		 * 1. increment currentTerm
		 * 2. vote for self
		 * 3. reset election timer
		 * 4. send RequestVote RPCs to all other servers 
		 * If votes received from majority: become leader
		 * If AppendEntries RPC received: convert to follower
		 * If election timeout: start new election  
		 */
		case CANDIDATE: 
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			// save Rft's persistent state to stable storgae
			rf.voteCount = 1
			rf.mu.Unlock()
			rf.sendRequestVoteAll 

			select {
			// what does candidate do when election timeout
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200) + 300)):
			case <- rf.heartbeat:
				rf.state = FOLLOWER
			// if candidate win, do nothing (replyhandler 
			case <-rf.winElect:
				rf.mu.Lock()
				rf.state = LEADER

				// reinitialize volatile state on leaders after election 
				rf.nextIndex = make([]int, len(rf.peers)) 
				rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0
				nextIndexNumber := len(rf.logs)

				for i := range rf.peers {
					// initialized to leader last log + 1
					rf.nextIndex[i] = nexIndexNumber
				}

				for i := range rf.peers {
					// initialized to 0
					rf.matchIndex[i] = 0
				}

				rf.mu.Unlock()
			
			
	}
} 




// ******** for 

type AppendEntriesArgs 