// Package raft implements the core of the Raft consensus algorithm
// (leader election + log replication) as a deterministic state machine.
//
// A Node never touches the network or the clock. The caller drives it:
//
//	node.Tick()        // one unit of logical time passed
//	node.Step(msg)     // a message arrived
//	node.Propose(cmd)  // a client wants to append a command
//	node.ReadMessages() // messages the node wants to send
//
// This keeps the algorithm free of goroutines and wall-clock time, so the
// same code runs in unit tests, in a terminal simulator and in the browser
// (compiled to WebAssembly) with reproducible results for a given seed.
package raft

import "math/rand"

// Role is the role a node plays in its current term.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// Entry is one slot of the replicated log.
type Entry struct {
	Term    int    `json:"term"`
	Command string `json:"command"`
}

// NoOp is the command a new leader appends to commit entries from older terms.
const NoOp = ""

// MsgType identifies the RPC a Message carries.
type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "RequestVote"
	case MsgVoteResp:
		return "VoteResponse"
	case MsgApp:
		return "AppendEntries"
	default:
		return "AppendResponse"
	}
}

// Message is a Raft RPC or RPC response. Only the fields relevant to Type are set.
type Message struct {
	Type MsgType `json:"type"`
	From int     `json:"from"`
	To   int     `json:"to"`
	Term int     `json:"term"`

	// RequestVote
	LastLogIndex int  `json:"lastLogIndex,omitempty"`
	LastLogTerm  int  `json:"lastLogTerm,omitempty"`
	Granted      bool `json:"granted,omitempty"`

	// AppendEntries
	PrevLogIndex int     `json:"prevLogIndex,omitempty"`
	PrevLogTerm  int     `json:"prevLogTerm,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit int     `json:"leaderCommit,omitempty"`
	Success      bool    `json:"success,omitempty"`
	MatchIndex   int     `json:"matchIndex,omitempty"` // on success: last index now matching the leader
	RejectHint   int     `json:"rejectHint,omitempty"` // on failure: follower's guess where logs may match
}

// Config holds a node's identity and timing, in ticks.
type Config struct {
	ID                 int   // 1-based, 0 means "nobody"
	Peers              []int // every node in the cluster, including ID
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatInterval  int
	MaxEntriesPerMsg   int
	Seed               int64
}

// DefaultConfig returns sensible timings for a node in a cluster of the given ids.
func DefaultConfig(id int, peers []int, seed int64) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 20,
		ElectionTimeoutMax: 40,
		HeartbeatInterval:  6,
		MaxEntriesPerMsg:   32,
		Seed:               seed,
	}
}

// Node is a single Raft participant.
type Node struct {
	cfg Config
	rng *rand.Rand

	// Persistent state (survives Restart).
	term     int
	votedFor int
	log      []Entry // log[0] is a sentinel so real entries start at index 1

	// Volatile state.
	role             Role
	leader           int
	commitIndex      int
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	votes            map[int]bool

	// Leader-only state.
	nextIndex  map[int]int
	matchIndex map[int]int

	msgs []Message
}

// New creates a follower in term 0 with an empty log.
func New(cfg Config) *Node {
	n := &Node{
		cfg: cfg,
		rng: rand.New(rand.NewSource(cfg.Seed)),
		log: []Entry{{}},
	}
	n.becomeFollower(0, 0)
	return n
}

// Restart simulates a crash + reboot: persistent state (term, vote, log) is
// kept, everything volatile is lost.
func (n *Node) Restart() {
	n.commitIndex = 0
	n.msgs = nil
	n.becomeFollower(n.term, 0)
}

// Tick advances the node's logical clock by one unit.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatInterval {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// Propose appends a command to the leader's log. It returns the index the
// command will occupy if it commits, or ok=false if this node is not the leader.
func (n *Node) Propose(command string) (index int, ok bool) {
	if n.role != Leader {
		return 0, false
	}
	n.appendEntry(command)
	n.broadcastAppend()
	return n.lastIndex(), true
}

// ReadMessages returns and clears the messages the node wants to send.
func (n *Node) ReadMessages() []Message {
	out := n.msgs
	n.msgs = nil
	return out
}

// Step feeds an incoming message to the node.
func (n *Node) Step(m Message) {
	// Rule for all servers: a higher term means our view is stale.
	if m.Term > n.term {
		leader := 0
		if m.Type == MsgApp {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	}
}

func (n *Node) handleVote(m Message) {
	grant := m.Term == n.term &&
		(n.votedFor == 0 || n.votedFor == m.From) &&
		n.isUpToDate(m.LastLogTerm, m.LastLogIndex)
	if grant {
		n.votedFor = m.From
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term || !m.Granted {
		return
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppend(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, RejectHint: n.lastIndex()})
		return
	}
	// Same term: whoever sends AppendEntries is the legitimate leader.
	if n.role != Follower {
		n.becomeFollower(m.Term, m.From)
	}
	n.leader = m.From
	n.resetElectionTimer()

	// Consistency check: our log must contain the entry right before the new ones.
	if m.PrevLogIndex > n.lastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, RejectHint: n.lastIndex()})
		return
	}
	if n.log[m.PrevLogIndex].Term != m.PrevLogTerm {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, RejectHint: m.PrevLogIndex - 1})
		return
	}

	// Append new entries, truncating our log at the first conflict only.
	// Entries that already match are left alone, so a delayed, shorter
	// AppendEntries can never erase entries we accepted later.
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + 1 + i
		if idx <= n.lastIndex() {
			if n.log[idx].Term == e.Term {
				continue
			}
			n.log = n.log[:idx]
		}
		n.log = append(n.log, m.Entries[i:]...)
		break
	}

	lastNew := m.PrevLogIndex + len(m.Entries)
	if m.LeaderCommit > n.commitIndex {
		n.commitIndex = min(m.LeaderCommit, lastNew)
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if m.Success {
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
			n.maybeCommit()
			if n.nextIndex[m.From] <= n.lastIndex() {
				n.sendAppend(m.From) // keep streaming until the follower caught up
			}
		}
		return
	}
	// Back off, but never below what we already know matches.
	next := min(n.nextIndex[m.From]-1, m.RejectHint+1)
	n.nextIndex[m.From] = max(next, n.matchIndex[m.From]+1, 1)
	n.sendAppend(m.From)
}

// maybeCommit advances commitIndex to the highest index stored on a majority,
// but only for entries from the current term (Raft §5.4.2).
func (n *Node) maybeCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.term {
			break
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			return
		}
	}
}

func (n *Node) campaign() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = 0
	n.votes = map[int]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(Message{Type: MsgVote, To: p, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()})
		}
	}
}

func (n *Node) becomeFollower(term, leader int) {
	if term > n.term {
		n.term = term
		n.votedFor = 0
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.nextIndex = nil
	n.matchIndex = nil
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.votes = nil
	n.heartbeatElapsed = 0
	n.nextIndex = map[int]int{}
	n.matchIndex = map[int]int{}
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// A no-op in the new term lets the leader commit entries left over from
	// previous terms, which it may not count replicas for directly.
	n.appendEntry(NoOp)
	n.broadcastAppend()
}

func (n *Node) appendEntry(command string) {
	n.log = append(n.log, Entry{Term: n.term, Command: command})
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.nextIndex[n.cfg.ID] = n.lastIndex() + 1
	n.maybeCommit() // a single-node cluster commits immediately
}

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to int) {
	prev := n.nextIndex[to] - 1
	end := min(n.lastIndex(), prev+n.cfg.MaxEntriesPerMsg)
	var entries []Entry
	if end > prev {
		entries = append([]Entry(nil), n.log[prev+1:end+1]...)
	}
	n.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log[prev].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.electionTimeout = n.cfg.ElectionTimeoutMin
	if span > 0 {
		n.electionTimeout += n.rng.Intn(span + 1)
	}
}

// isUpToDate reports whether a candidate's log is at least as up-to-date as ours (§5.4.1).
func (n *Node) isUpToDate(lastTerm, lastIndex int) bool {
	if lastTerm != n.lastTerm() {
		return lastTerm > n.lastTerm()
	}
	return lastIndex >= n.lastIndex()
}

func (n *Node) quorum() int    { return len(n.cfg.Peers)/2 + 1 }
func (n *Node) lastIndex() int { return len(n.log) - 1 }
func (n *Node) lastTerm() int  { return n.log[n.lastIndex()].Term }

// Read-only accessors.

func (n *Node) ID() int          { return n.cfg.ID }
func (n *Node) Role() Role       { return n.role }
func (n *Node) Term() int        { return n.term }
func (n *Node) Leader() int      { return n.leader }
func (n *Node) VotedFor() int    { return n.votedFor }
func (n *Node) CommitIndex() int { return n.commitIndex }
func (n *Node) LastIndex() int   { return n.lastIndex() }

// Log returns a copy of the log without the sentinel, so Log()[0] is index 1.
func (n *Node) Log() []Entry { return append([]Entry(nil), n.log[1:]...) }

// ElectionProgress returns how far the election timer has run, from 0 to 1.
// Leaders don't run an election timer and always return 0.
func (n *Node) ElectionProgress() float64 {
	if n.role == Leader || n.electionTimeout == 0 {
		return 0
	}
	return float64(n.electionElapsed) / float64(n.electionTimeout)
}

// MatchIndex returns the leader's view of how much of its log peer holds.
func (n *Node) MatchIndex(peer int) int { return n.matchIndex[peer] }
