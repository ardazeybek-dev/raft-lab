package raft

import "testing"

// network is a hand-driven message bus: the test decides exactly which
// messages get delivered, which lets us replay precise failure scenarios.
type network struct {
	nodes map[int]*Node
	down  map[int]bool
	queue []Message
}

func newNetwork(size int, maxEntries int) *network {
	nw := &network{nodes: map[int]*Node{}, down: map[int]bool{}}
	var ids []int
	for id := 1; id <= size; id++ {
		ids = append(ids, id)
	}
	for _, id := range ids {
		cfg := DefaultConfig(id, ids, int64(id))
		cfg.MaxEntriesPerMsg = maxEntries
		nw.nodes[id] = New(cfg)
	}
	return nw
}

func (nw *network) collect() {
	for id := 1; id <= len(nw.nodes); id++ {
		msgs := nw.nodes[id].ReadMessages()
		if !nw.down[id] {
			nw.queue = append(nw.queue, msgs...)
		}
	}
}

// deliver passes queued messages between the given nodes, one at a time,
// until stop returns true or nothing is left. Everything else is dropped.
func (nw *network) deliver(among []int, stop func() bool) {
	allowed := map[int]bool{}
	for _, id := range among {
		allowed[id] = true
	}
	nw.collect()
	for len(nw.queue) > 0 {
		if stop != nil && stop() {
			break
		}
		m := nw.queue[0]
		nw.queue = nw.queue[1:]
		if allowed[m.From] && allowed[m.To] && !nw.down[m.To] {
			nw.nodes[m.To].Step(m)
		}
		nw.collect()
	}
	nw.queue = nil
}

// elect makes id campaign until it wins with votes from among. Only vote
// traffic is delivered, so no log entries move.
func (nw *network) elect(t *testing.T, id int, among []int) {
	t.Helper()
	votesOnly := func() {
		nw.collect()
		for len(nw.queue) > 0 {
			m := nw.queue[0]
			nw.queue = nw.queue[1:]
			inGroup := false
			for _, a := range among {
				inGroup = inGroup || a == m.To
			}
			if inGroup && (m.Type == MsgVote || m.Type == MsgVoteResp) {
				nw.nodes[m.To].Step(m)
			}
			nw.collect()
		}
	}
	for range 5 {
		nw.nodes[id].campaign()
		votesOnly()
		if nw.nodes[id].Role() == Leader {
			nw.queue = nil
			nw.nodes[id].ReadMessages() // drop the initial AppendEntries
			return
		}
	}
	t.Fatalf("node %d could not win an election among %v", id, among)
}

// assertCommittedAgree checks State Machine Safety across all nodes,
// including crashed ones (what they committed before crashing still counts).
func assertCommittedAgree(t *testing.T, nw *network) {
	t.Helper()
	committed := map[int]Entry{}
	owner := map[int]int{}
	for id, n := range nw.nodes {
		for idx := 1; idx <= n.CommitIndex(); idx++ {
			e := n.log[idx]
			if prev, ok := committed[idx]; ok && prev != e {
				t.Fatalf("index %d: node %d committed %+v but node %d committed %+v", idx, owner[idx], prev, id, e)
			}
			committed[idx], owner[idx] = e, id
		}
	}
}

// TestFigure8 replays the scenario from Figure 8 of the Raft paper: a
// leader must not commit an entry from an older term just because it is
// stored on a majority, since a later leader can still overwrite it.
func TestFigure8(t *testing.T) {
	nw := newNetwork(5, 1) // one entry per message, so replication can stop mid-way

	// (a) S1 leads term 1 and replicates "x" to S2 only.
	nw.elect(t, 1, []int{1, 2, 3})
	nw.nodes[1].Propose("x")
	nw.deliver([]int{1, 2}, nil)

	// (b) S1 crashes. S5 wins a later term with S3, S4 and appends "y" locally.
	nw.down[1] = true
	nw.elect(t, 5, []int{3, 4, 5})
	nw.nodes[5].Propose("y")
	nw.nodes[5].ReadMessages()

	// (c) S5 crashes, S1 comes back, wins a new term and pushes its old-term
	// entries to S3. Stop as soon as "x" is on a majority (S1, S2, S3),
	// before the new term's no-op reaches anyone.
	nw.down[5] = true
	nw.down[1] = false
	nw.nodes[1].Restart()
	nw.elect(t, 1, []int{1, 2, 3})
	nw.nodes[1].broadcastAppend()
	nw.deliver([]int{1, 2, 3}, func() bool { return nw.nodes[3].LastIndex() >= 2 })

	// Nothing from the current term is on a majority yet, so nothing may commit.
	if got := nw.nodes[1].CommitIndex(); got > 0 {
		t.Errorf("leader committed old-term entries by counting replicas; commitIndex = %d", got)
	}

	// (d) S1 crashes before its no-op spreads. S5 has a newer last term than
	// S2, S3, S4, wins, and overwrites index 2 everywhere.
	nw.down[1] = true
	nw.down[5] = false
	nw.nodes[5].Restart()
	nw.elect(t, 5, []int{2, 3, 4, 5})
	nw.nodes[5].broadcastAppend()
	for range 10 {
		nw.deliver([]int{2, 3, 4, 5}, nil)
		nw.nodes[5].broadcastAppend()
	}
	if nw.nodes[5].CommitIndex() < 2 {
		t.Fatalf("S5 should have committed its entries, commitIndex = %d", nw.nodes[5].CommitIndex())
	}

	assertCommittedAgree(t, nw)
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	nw := newNetwork(3, 32)
	nw.elect(t, 1, []int{1, 2, 3})
	nw.nodes[1].Propose("x")
	nw.deliver([]int{1, 2}, nil) // node 3 misses "x"

	// Node 3 has a shorter log, so node 2 must refuse to vote for it.
	nw.nodes[3].campaign()
	nw.deliver([]int{2, 3}, nil)
	if nw.nodes[3].Role() == Leader {
		t.Fatal("node with a stale log won the election")
	}
	if nw.nodes[2].VotedFor() == 3 {
		t.Fatal("node 2 voted for a candidate with a stale log")
	}
}

func TestOneVotePerTerm(t *testing.T) {
	nw := newNetwork(3, 32)
	n := nw.nodes[1]
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 1})

	granted := map[int]bool{}
	for _, m := range n.ReadMessages() {
		granted[m.To] = m.Granted
	}
	if !granted[2] || granted[3] {
		t.Fatalf("expected vote for 2 only, got %v", granted)
	}
}

func TestConflictingSuffixIsTruncated(t *testing.T) {
	n := New(DefaultConfig(1, []int{1, 2}, 1))
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 1, Entries: []Entry{{1, "a"}, {1, "b"}, {1, "c"}}})
	// A leader of term 2 disagrees from index 2 on.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{2, "B"}}})

	got := n.Log()
	if len(got) != 2 || got[1] != (Entry{2, "B"}) {
		t.Fatalf("log = %+v, want [a B]", got)
	}
}

func TestStaleAppendDoesNotTruncate(t *testing.T) {
	n := New(DefaultConfig(1, []int{1, 2}, 1))
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 1, Entries: []Entry{{1, "a"}, {1, "b"}}})
	// A delayed, older copy of the first AppendEntries arrives late.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 1, Entries: []Entry{{1, "a"}}})

	if got := n.Log(); len(got) != 2 {
		t.Fatalf("stale message truncated the log: %+v", got)
	}
}
