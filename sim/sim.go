// Package sim runs a cluster of raft.Nodes over a simulated, unreliable
// network: messages are delayed, reordered and dropped, nodes can crash and
// restart, and the network can be split into partitions.
//
// While it runs, the cluster continuously checks Raft's safety properties
// and records any violation, which makes it usable both as a visual
// playground and as a randomized ("chaos") test harness.
package sim

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/ardazeybek-dev/raft-lab/raft"
)

// Cluster is a set of nodes plus the network between them.
type Cluster struct {
	Now int

	// Network behaviour.
	DropRate   float64 // probability that a message is lost
	MinLatency int     // ticks
	MaxLatency int     // ticks

	nodes    []*raft.Node // nodes[i] has ID i+1
	down     []bool
	group    []int // partition id per node; nodes talk only within the same group
	inflight []envelope
	rng      *rand.Rand

	events     []Event
	violations []string
	reported   map[string]bool // violations already recorded, so each is reported once

	leaderOf  map[int]int  // term -> leader id, for election safety
	committed []raft.Entry // first committed value seen at each index
	roles     []raft.Role
}

type envelope struct {
	msg    raft.Message
	sentAt int
	due    int
}

// Event is a human-readable line in the cluster's history.
type Event struct {
	Tick int    `json:"tick"`
	Text string `json:"text"`
	Kind string `json:"kind"` // election, leader, commit, fault, heal, client, violation
}

// New creates a cluster of size nodes. The same seed always produces the
// same run, given the same sequence of calls.
func New(size int, seed int64) *Cluster {
	ids := make([]int, size)
	for i := range ids {
		ids[i] = i + 1
	}
	c := &Cluster{
		DropRate:   0,
		MinLatency: 2,
		MaxLatency: 6,
		down:       make([]bool, size),
		group:      make([]int, size),
		rng:        rand.New(rand.NewSource(seed)),
		leaderOf:   map[int]int{},
		reported:   map[string]bool{},
		roles:      make([]raft.Role, size),
	}
	for _, id := range ids {
		c.nodes = append(c.nodes, raft.New(raft.DefaultConfig(id, ids, seed*1000+int64(id))))
	}
	return c
}

// Size returns the number of nodes.
func (c *Cluster) Size() int { return len(c.nodes) }

// Node returns the node with the given 1-based id.
func (c *Cluster) Node(id int) *raft.Node { return c.nodes[id-1] }

// Step advances the whole cluster by one tick.
func (c *Cluster) Step() {
	c.Now++

	// 1. Deliver messages that have arrived.
	pending := c.inflight[:0]
	var arrived []raft.Message
	for _, e := range c.inflight {
		if e.due <= c.Now {
			arrived = append(arrived, e.msg)
		} else {
			pending = append(pending, e)
		}
	}
	c.inflight = pending
	for _, m := range arrived {
		if c.canReach(m.From, m.To) && !c.down[m.To-1] {
			c.nodes[m.To-1].Step(m)
		}
	}

	// 2. Let time pass on every live node.
	for i, n := range c.nodes {
		if !c.down[i] {
			n.Tick()
		}
	}

	// 3. Put outgoing messages on the wire.
	for i, n := range c.nodes {
		for _, m := range n.ReadMessages() {
			if c.down[i] || !c.canReach(m.From, m.To) || c.rng.Float64() < c.DropRate {
				continue
			}
			latency := c.MinLatency
			if c.MaxLatency > c.MinLatency {
				latency += c.rng.Intn(c.MaxLatency - c.MinLatency + 1)
			}
			c.inflight = append(c.inflight, envelope{msg: m, sentAt: c.Now, due: c.Now + latency})
		}
	}

	c.observe()
}

// Run advances the cluster by ticks steps.
func (c *Cluster) Run(ticks int) {
	for range ticks {
		c.Step()
	}
}

// Leader returns the id of the live leader with the highest term, or 0.
func (c *Cluster) Leader() int {
	best, bestTerm := 0, -1
	for i, n := range c.nodes {
		if !c.down[i] && n.Role() == raft.Leader && n.Term() > bestTerm {
			best, bestTerm = n.ID(), n.Term()
		}
	}
	return best
}

// Propose sends a client command to the current leader.
func (c *Cluster) Propose(command string) (leader, index int, err error) {
	leader = c.Leader()
	if leader == 0 {
		return 0, 0, fmt.Errorf("no leader available")
	}
	index, _ = c.nodes[leader-1].Propose(command)
	c.logf("client", "client → node %d: %q (index %d)", leader, command, index)
	return leader, index, nil
}

// Crash stops a node. Its in-flight messages keep travelling.
func (c *Cluster) Crash(id int) {
	if c.down[id-1] {
		return
	}
	c.down[id-1] = true
	c.logf("fault", "node %d crashed", id)
}

// Restart brings a crashed node back with its persistent state.
func (c *Cluster) Restart(id int) {
	if !c.down[id-1] {
		return
	}
	c.down[id-1] = false
	c.nodes[id-1].Restart()
	c.roles[id-1] = raft.Follower
	c.logf("heal", "node %d restarted (term %d, %d log entries kept)", id, c.nodes[id-1].Term(), c.nodes[id-1].LastIndex())
}

// IsDown reports whether a node is crashed.
func (c *Cluster) IsDown(id int) bool { return c.down[id-1] }

// Partition splits the network. Each slice is a group of node ids that can
// still talk to each other; nodes not mentioned end up in their own group.
func (c *Cluster) Partition(groups ...[]int) {
	for i := range c.group {
		c.group[i] = -(i + 1)
	}
	var desc []string
	for g, ids := range groups {
		for _, id := range ids {
			c.group[id-1] = g
		}
		desc = append(desc, fmt.Sprint(ids))
	}
	c.logf("fault", "network partitioned: %s", strings.Join(desc, " | "))
}

// Isolate cuts a single node off from everyone else.
func (c *Cluster) Isolate(id int) {
	var rest []int
	for i := range c.nodes {
		if i+1 != id {
			rest = append(rest, i+1)
		}
	}
	c.Partition(rest)
}

// Heal removes all partitions.
func (c *Cluster) Heal() {
	for i := range c.group {
		c.group[i] = 0
	}
	c.logf("heal", "network healed")
}

// IsIsolated reports whether a node shares its partition with nobody.
func (c *Cluster) IsIsolated(id int) bool {
	for i := range c.nodes {
		if i+1 != id && c.group[i] == c.group[id-1] {
			return false
		}
	}
	return true
}

// Events returns the cluster history, oldest first.
func (c *Cluster) Events() []Event { return c.events }

// Violations returns every safety violation detected so far. A correct Raft
// implementation must always return an empty slice.
func (c *Cluster) Violations() []string { return c.violations }

// Committed returns the log prefix that the cluster has agreed on.
func (c *Cluster) Committed() []raft.Entry { return c.committed }

func (c *Cluster) canReach(from, to int) bool { return c.group[from-1] == c.group[to-1] }

// observe records role changes and checks the safety invariants.
func (c *Cluster) observe() {
	for i, n := range c.nodes {
		if c.down[i] {
			continue
		}
		prev := c.roles[i]
		c.roles[i] = n.Role()
		if prev == n.Role() {
			continue
		}
		switch n.Role() {
		case raft.Follower:
			if prev == raft.Leader {
				c.logf("heal", "node %d steps down: it saw term %d, newer than its own", n.ID(), n.Term())
			}
		case raft.Candidate:
			c.logf("election", "node %d timed out, starts election for term %d", n.ID(), n.Term())
		case raft.Leader:
			c.logf("leader", "node %d is leader for term %d", n.ID(), n.Term())
			// Election Safety: at most one leader per term.
			if other, ok := c.leaderOf[n.Term()]; ok && other != n.ID() {
				c.violate("two leaders in term %d: node %d and node %d", n.Term(), other, n.ID())
			}
			c.leaderOf[n.Term()] = n.ID()
		}
	}

	// State Machine Safety: once an entry is committed at an index, no node
	// may ever commit a different entry there.
	before := len(c.committed)
	for i, n := range c.nodes {
		if c.down[i] {
			continue
		}
		log := n.Log()
		for idx := 1; idx <= n.CommitIndex(); idx++ {
			e := log[idx-1]
			if idx > len(c.committed) {
				c.committed = append(c.committed, e)
			} else if c.committed[idx-1] != e {
				c.violate("node %d committed %v at index %d, cluster already committed %v", n.ID(), e, idx, c.committed[idx-1])
				break
			}
		}
	}
	for idx := before + 1; idx <= len(c.committed); idx++ {
		e := c.committed[idx-1]
		if e.Command == raft.NoOp {
			c.logf("commit", "index %d committed (leader no-op, term %d)", idx, e.Term)
		} else {
			c.logf("commit", "index %d committed: %q", idx, e.Command)
		}
	}
}

func (c *Cluster) violate(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if c.reported[msg] {
		return
	}
	c.reported[msg] = true
	c.violations = append(c.violations, msg)
	c.logf("violation", "SAFETY VIOLATION: %s", msg)
}

func (c *Cluster) logf(kind, format string, args ...any) {
	c.events = append(c.events, Event{Tick: c.Now, Kind: kind, Text: fmt.Sprintf(format, args...)})
	if len(c.events) > 500 {
		c.events = c.events[len(c.events)-500:]
	}
}
