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
import "../labrpc"
import "time"
import "math/rand"


//
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

	currentTerm int
	votedFor int
	logs []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	state int
	voteCount int
	applyCh chan ApplyMsg
	electWinCh chan bool
	heartbeatCh chan bool

	granted chan bool
}

type LogEntry struct {
	Term int
	Command interface{}
}

const (
	FOLLOWER = iota
	CANDIDATE = iota
	LEADER = iota
)


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state  == LEADER)
	return term, isleader
}

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
	Term int
	VoteGranted bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// return if term < currentTerm

	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
		}

		if ((args.LastLogTerm > rf.logs[len(rf.logs) - 1].Term) || 
			((args.LastLogIndex >= len(rf.logs)-1) && 
			args.LastLogTerm == rf.logs[len(rf.logs) - 1].Term)) && 
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
			rf.votedFor = args.CandidateId
			rf.granted <- true
			reply.VoteGranted = true
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()


	if ok && rf.state == CANDIDATE && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		} else {
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers) / 2 {
					rf.state = LEADER
					rf.electWinCh <- true
				}
			}
		}
	}

	return ok
}


func (rf *Raft) sendAllRequestVotes() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs)-1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()

	for i:= range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Entries []LogEntry
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextTryIndex int
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.NextTryIndex = len(rf.logs)
	} else{
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}

		rf.heartbeatCh <- true
		reply.Term = rf.currentTerm

		if args.PrevLogIndex > len(rf.logs)-1 {
			reply.NextTryIndex = len(rf.logs)
		} else {
			if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
					//confict, back

					//reply.NextTryIndex = args.PrevLogIndex
				term := rf.logs[args.PrevLogIndex].Term

				for reply.NextTryIndex = args.PrevLogIndex - 1;
					reply.NextTryIndex > 0 && rf.logs[reply.NextTryIndex].Term == term;
					reply.NextTryIndex-- {}

				reply.NextTryIndex++

			} else {
				//append logs
				overLapIsSame := 1 //assume same
				for i:= 0; 
					i < len(args.Entries) && (args.PrevLogIndex+1+i)<len(rf.logs);
					i++ {
					if args.Entries[i].Term != rf.logs[args.PrevLogIndex+1+i].Term {
							overLapIsSame = 0
					}
				}

				//different Term or leader log is longer than server log.
				if overLapIsSame == 0 || len(args.Entries) > (len(rf.logs)-args.PrevLogIndex-1) {
					rf.logs = rf.logs[:args.PrevLogIndex+1]
					for i:=0; i<len(args.Entries); i++ {
						rf.logs = append(rf.logs, args.Entries[i])
					}
				} 

				reply.Success = true
				reply.NextTryIndex = args.PrevLogIndex

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = len(rf.logs)-1
					if args.LeaderCommit <= len(rf.logs)-1 {
						rf.commitIndex = args.LeaderCommit
					}

					go rf.commitLogs()
				}
			}
		}
	}
	return
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == LEADER && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		} else {
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex+len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server]+1
			} else {
				rf.nextIndex[server] = reply.NextTryIndex
			}

			for N:= len(rf.logs)-1; N > rf.commitIndex; N-- {
				commitCount :=1

				if rf.logs[N].Term == rf.currentTerm {
					for i:=0; i<len(rf.peers); i++ {
						if rf.matchIndex[i] >= N {
							commitCount++
						}
					}
				}

				if commitCount > len(rf.peers) /2 {
					rf.commitIndex = N
					go rf.commitLogs()
					break
				}
			}

		}

	}
	return ok
}


func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i]-1
			args.LeaderCommit = rf.commitIndex

			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}

			if rf.nextIndex[i] <= len(rf.logs)-1 {
				args.Entries = rf.logs[rf.nextIndex[i] : ]
			}

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}


func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i:=rf.lastApplied+1; i<=rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0
	term := 0
	isLeader := rf.state == LEADER

	// Your code here (3B).
	if isLeader {
		index = len(rf.logs)
		term = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, command})
	}

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

//
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
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.voteCount = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electWinCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})


	rf.granted = make(chan bool)


	go rf.runServer()

	return rf
}


func (rf *Raft) runServer() {
	for {
		switch rf.state {
		case LEADER:
			rf.sendAllAppendEntries()
			time.Sleep(time.Millisecond * 150)

		case FOLLOWER:
			select {
			case <-rf.granted:
			case <-rf.heartbeatCh:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+300)):
				rf.state = CANDIDATE
			}

		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			rf.sendAllRequestVotes()

			select {
				case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+300)):
				case <-rf.heartbeatCh:
					rf.state = FOLLOWER
				case <-rf.electWinCh:
					rf.mu.Lock()
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					
					nextIndex := len(rf.logs)
					for i:=0; i<len(rf.peers); i++ {
						rf.nextIndex[i] = nextIndex
					}

					rf.mu.Unlock()
			}

		}
	}
}