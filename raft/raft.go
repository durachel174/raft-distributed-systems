package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft-project/labgob"
	"raft-project/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	currentTerm int
	myState     string
	voteCount   int
	majority    int

	votedFor int
	vflag    bool
	flag     bool

	log []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	msgChannel chan ApplyMsg
}

type Log struct {
	Index int
	Term  int
	Entry interface{}
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.myState == "leader")
	rf.mu.Unlock()

	return term, isleader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("Decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
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
	Term           int
	Success        bool
	Conflict_term  int
	Conflict_index int
	Success_index  int
	Log_Success    bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Log_Success = false
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.myState = "follower"
	rf.flag = true
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true

	if (len(rf.log) >= (args.PrevLogIndex + 1)) && (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Log_Success = false
		rf.mu.Unlock()
		return
	}

	if (len(rf.log) <= args.PrevLogIndex) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Log_Success = false
		rf.mu.Unlock()
		return
	}

	leader_c := args.LeaderCommit

	rf.log = append(rf.log[:(args.PrevLogIndex+1)], args.Entries...)
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[args.LeaderId] = len(rf.log) - 1

	if leader_c > rf.commitIndex {
		if (leader_c <= len(rf.log)-1) && (rf.log[leader_c].Term == rf.currentTerm) {
			rf.commitIndex = leader_c
		} else if (leader_c > len(rf.log)-1) && (rf.log[len(rf.log)-1].Term == rf.currentTerm) {
			rf.commitIndex = len(rf.log) - 1
		}

		if rf.commitIndex > rf.lastApplied {
			Lc := rf.commitIndex
			p := rf.lastApplied + 1

			for p <= Lc {
				command := rf.log[p].Entry
				mess := ApplyMsg{true, command, p}
				rf.msgChannel <- mess
				rf.lastApplied = p
				p++
			}
		}
	}

	reply.Success_index = len(rf.log) - 1
	rf.persist()
	rf.mu.Unlock()

	reply.Log_Success = true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.myState = "follower"
		rf.votedFor = -1
	}

	up_to_date := false
	my_last_log_term := rf.log[len(rf.log)-1].Term
	my_last_log_index := len(rf.log) - 1
	if (args.LastLogTerm > my_last_log_term) || ((args.LastLogTerm == my_last_log_term) && (args.LastLogIndex >= my_last_log_index)) {
		up_to_date = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && up_to_date {
		rf.myState = "follower"
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.vflag = true
		rf.persist()
		DPrintf("[%d] voted for %d in term %d", rf.me, args.CandidateId, args.Term)
		rf.mu.Unlock()
		return
	} else {
		reply.Term = args.Term
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Append_entries(Follower int, Next_index int) {

	rf.mu.Lock()
	ni := rf.nextIndex[Follower]
	if ni < 1 {
		ni = 1
	}
	if ni > len(rf.log) {
		ni = len(rf.log)
	}
	Cur_term := rf.currentTerm
	Leader_commit := rf.commitIndex
	Prev_index := ni - 1
	Prev_term := rf.log[Prev_index].Term
	Entries_list := rf.log[ni:]
	Last_term := rf.log[len(rf.log)-1].Term
	Leader_ID := rf.me
	DPrintf("Leader %d sending AppendEntries to %d (nextIndex=%d, entries=%d)", rf.me, Follower, ni, len(Entries_list))
	rf.mu.Unlock()

	ask := AppendEntriesArgs{
		Term:         Cur_term,
		LeaderId:     Leader_ID,
		PrevLogIndex: Prev_index,
		PrevLogTerm:  Prev_term,
		Entries:      Entries_list,
		LeaderCommit: Leader_commit,
	}
	rep := AppendEntriesReply{}
	ok := rf.sendAppendEntries(Follower, &ask, &rep)
	if ok {
		if !rep.Success {
			rf.mu.Lock()
			rf.myState = "follower"
			rf.currentTerm = rep.Term
			rf.mu.Unlock()
			return
		}

		if rep.Log_Success {
			rf.mu.Lock()
			index := rep.Success_index
			rf.nextIndex[Follower] = index + 1
			rf.matchIndex[Follower] = index
			c_index := rf.commitIndex
			term := Last_term
			length_m := len(rf.matchIndex)
			major := rf.majority
			rf.mu.Unlock()

			count := 1
			rf.mu.Lock()
			if index > c_index && term == Cur_term {
				for x := 0; x < length_m; x++ {
					if x != Leader_ID {
						if rf.matchIndex[x] >= index {
							count++
						}
					}
				}
				rf.mu.Unlock()
				if count >= major && index > Leader_commit {
					rf.mu.Lock()
					DPrintf("Leader %d advancing commitIndex %d -> %d", rf.me, rf.commitIndex, index)
					rf.commitIndex = index
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
			return
		}

		if !rep.Log_Success {
			rf.mu.Lock()
			if ni > 1 && rf.myState == "leader" {
				rf.nextIndex[Follower]--
				ni--
			}

			rf.mu.Unlock()
			go rf.Append_entries(Follower, ni)
			return
		}

		return
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	DPrintf("Leader %d received Start() for command %v", rf.me, command)

	rf.mu.Lock()
	if rf.myState != "leader" {
		rf.mu.Unlock()
		return index, term, false
	}
	index = len(rf.log)
	term = rf.currentTerm
	com := Log{
		Index: index,
		Term:  term,
		Entry: command,
	}
	rf.log = append(rf.log, com)
	rf.nextIndex[rf.me] = len(rf.log)
	for i := 0; i < len(rf.nextIndex); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.nextIndex[rf.me]
		}
	}
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	rf.persist()
	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		checkState := rf.myState
		rf.mu.Unlock()

		if checkState == "leader" {
			rf.mu.Lock()
			MyID := rf.me
			length := len(rf.peers)
			rf.mu.Unlock()

			DPrintf("Leader %d sending heartbeats", rf.me)
			for I := 0; I < length; I++ {
				if I != MyID {
					rf.mu.Lock()
					if rf.myState == "follower" {
						rf.mu.Unlock()
						break
					}

					if rf.nextIndex[I] > len(rf.log) {
						rf.nextIndex[I] = len(rf.log)
					}
					if rf.nextIndex[I] < 1 {
						rf.nextIndex[I] = 1
					}

					N_index := rf.nextIndex[I]
					rf.mu.Unlock()

					go rf.Append_entries(I, N_index)

					rf.mu.Lock()
					if rf.myState == "follower" {
						checkState = "follower"
						rf.mu.Unlock()
						break
					} else {
						rf.mu.Unlock()
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			if rf.myState == "leader" {
				lead_com := rf.commitIndex
				p := rf.lastApplied + 1
				for p <= lead_com {
					command := rf.log[p].Entry
					mess := ApplyMsg{true, command, p}
					rf.msgChannel <- mess
					rf.lastApplied = p
					p++
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}

		} else if checkState == "candidate" {
			min := 300
			max := 501
			timeout := rand.Intn(max-min) + min
			for timeout > 0 {
				rf.mu.Lock()
				if rf.flag {
					rf.myState = "follower"
					checkState = "follower"
					rf.mu.Unlock()
					break
				}
				if rf.voteCount >= rf.majority {
					rf.myState = "leader"
					checkState = "leader"
					term := rf.currentTerm
					myID := rf.me
					length := len(rf.peers)
					for x := 0; x < length; x++ {
						if x != myID {
							rf.nextIndex[x] = rf.nextIndex[myID]
							rf.matchIndex[x] = 0
						}
					}
					rf.mu.Unlock()
					DPrintf("[%d] became leader for term %d", myID, term)
					for j := 0; j < length; j++ {
						if j != myID {
							go func(j int, myID int, term int) {
								ask := AppendEntriesArgs{
									Term:     term,
									LeaderId: myID,
								}
								rep := AppendEntriesReply{}
								rf.sendAppendEntries(j, &ask, &rep)
							}(j, myID, term)
						}
					}
					break
				}

				if rf.myState != "candidate" {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				timeout -= 10
			}

			rf.mu.Lock()
			if rf.myState == "candidate" {
				rf.myState = "candidate"
				checkState = "candidate"
				rf.currentTerm++
				rf.flag = false
				rf.vflag = false
				rf.voteCount = 1
				rf.votedFor = rf.me
				term := rf.currentTerm
				myID := rf.me
				length := len(rf.peers)
				last_log_index := len(rf.log) - 1
				last_log_term := rf.log[last_log_index].Term
				rf.persist()
				rf.mu.Unlock()
				DPrintf("[%d] starting election for term %d", myID, term)
				for i := 0; i < length; i++ {
					if i != myID {
						go func(index, id int, term int, lli int, llt int) {
							ask := RequestVoteArgs{
								Term:         term,
								CandidateId:  id,
								LastLogIndex: lli,
								LastLogTerm:  llt,
							}
							rep := RequestVoteReply{}
							ok := rf.sendRequestVote(index, &ask, &rep)
							if ok && rep.VoteGranted {
								rf.mu.Lock()
								rf.voteCount++
								rf.mu.Unlock()
							}
						}(i, myID, term, last_log_index, last_log_term)
					}
				}
			} else {
				rf.mu.Unlock()
			}
		} else if checkState == "follower" {
			min := 300
			max := 501
			timeout := rand.Intn(max-min) + min
			rf.mu.Lock()
			rf.votedFor = -1
			rf.flag = false
			rf.vflag = false
			rf.mu.Unlock()
			for timeout > 0 {
				rf.mu.Lock()
				if rf.vflag {
					timeout = rand.Intn(max-min) + min
					rf.vflag = false
				}
				if rf.flag {
					timeout = rand.Intn(max-min) + min
					rf.flag = false
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				timeout -= 10
			}
			rf.mu.Lock()
			rf.myState = "candidate"
			checkState = "candidate"
			rf.currentTerm++
			rf.flag = false
			rf.vflag = false
			rf.voteCount = 1
			rf.votedFor = rf.me
			rf.persist()
			term := rf.currentTerm
			myID := rf.me
			length := len(rf.peers)
			last_log_index := len(rf.log) - 1
			last_log_term := rf.log[last_log_index].Term
			rf.mu.Unlock()
			DPrintf("[%d] election timeout, becoming candidate for term %d", myID, term)
			for i := 0; i < length; i++ {
				if i != myID {
					go func(index, id int, term int, lli int, llt int) {
						ask := RequestVoteArgs{
							Term:         term,
							CandidateId:  id,
							LastLogIndex: lli,
							LastLogTerm:  llt,
						}
						rep := RequestVoteReply{}
						ok := rf.sendRequestVote(index, &ask, &rep)
						if ok && rep.VoteGranted {
							rf.mu.Lock()
							rf.voteCount++
							rf.mu.Unlock()
						}
					}(i, myID, term, last_log_index, last_log_term)
				}
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.myState = "follower"
	rf.majority = (len(peers) / 2) + 1
	rf.votedFor = -1
	rf.flag = false
	rf.vflag = false
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.msgChannel = applyCh

	dummy := Log{0, 0, "dummy"}
	rf.log = append(rf.log, dummy)
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
