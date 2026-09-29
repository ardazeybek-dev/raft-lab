package sim

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/ardazeybek-dev/raft-lab/raft"
)

// waitForLeader runs the cluster until a leader exists or the budget runs out.
func waitForLeader(t *testing.T, c *Cluster, budget int) int {
	t.Helper()
	for range budget {
		c.Step()
		if l := c.Leader(); l != 0 {
			return l
		}
	}
	t.Fatalf("no leader after %d ticks", budget)
	return 0
}

func assertSafe(t *testing.T, c *Cluster) {
	t.Helper()
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("safety violations: %v", v)
	}
}

func committedCommands(c *Cluster) []string {
	var out []string
	for _, e := range c.Committed() {
		if e.Command != raft.NoOp {
			out = append(out, e.Command)
		}
	}
	return out
}

func TestElectsExactlyOneLeader(t *testing.T) {
	c := New(5, 1)
	leader := waitForLeader(t, c, 200)
	c.Run(100)

	leaders := 0
	for id := 1; id <= c.Size(); id++ {
		n := c.Node(id)
		if n.Role() == raft.Leader {
			leaders++
		}
		if n.Leader() != leader {
			t.Errorf("node %d thinks leader is %d, want %d", id, n.Leader(), leader)
		}
	}
	if leaders != 1 {
		t.Fatalf("got %d leaders, want 1", leaders)
	}
	assertSafe(t, c)
}

func TestReplicatesAndCommits(t *testing.T) {
	c := New(3, 2)
	waitForLeader(t, c, 200)
	for i := range 5 {
		if _, _, err := c.Propose(fmt.Sprintf("SET k%d %d", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	c.Run(50)

	if got := committedCommands(c); len(got) != 5 {
		t.Fatalf("committed %v, want 5 commands", got)
	}
	for id := 1; id <= 3; id++ {
		if got := c.Node(id).CommitIndex(); got != 6 { // 5 commands + leader no-op
			t.Errorf("node %d commitIndex = %d, want 6", id, got)
		}
	}
	assertSafe(t, c)
}

func TestLeaderCrashTriggersNewElection(t *testing.T) {
	c := New(5, 3)
	old := waitForLeader(t, c, 200)
	c.Propose("SET x 1")
	c.Run(30)
	c.Crash(old)

	newLeader := waitForLeader(t, c, 300)
	if newLeader == old {
		t.Fatalf("crashed node %d is still leader", old)
	}
	if c.Node(newLeader).Term() <= c.Node(old).Term() {
		t.Fatalf("new leader term %d not greater than old %d", c.Node(newLeader).Term(), c.Node(old).Term())
	}

	// The committed entry survives the leader change.
	c.Propose("SET y 2")
	c.Run(50)
	if got := committedCommands(c); len(got) != 2 || got[0] != "SET x 1" || got[1] != "SET y 2" {
		t.Fatalf("committed = %v", got)
	}
	assertSafe(t, c)
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := New(5, 4)
	leader := waitForLeader(t, c, 200)
	c.Run(20)

	// Leave the old leader with one follower; the other three form a majority.
	var minority, majority []int
	minority = append(minority, leader)
	for id := 1; id <= 5; id++ {
		if id == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)

	// The stale leader accepts the write but can never commit it.
	c.Node(leader).Propose("SET lost 1")
	c.Run(300)
	for _, cmd := range committedCommands(c) {
		if cmd == "SET lost 1" {
			t.Fatal("minority leader committed a write")
		}
	}

	// The majority side elected its own leader and keeps making progress.
	if l := c.Leader(); l == leader || l == 0 {
		t.Fatalf("expected a majority-side leader, got %d", l)
	}
	c.Propose("SET kept 1")
	c.Run(50)

	// After healing, the old leader steps down and its uncommitted entry is overwritten.
	c.Heal()
	c.Run(200)
	if c.Node(leader).Role() == raft.Leader && c.Leader() == leader && c.Node(leader).Term() == 1 {
		t.Fatal("stale leader did not step down")
	}
	for _, e := range c.Node(leader).Log() {
		if e.Command == "SET lost 1" {
			t.Fatal("uncommitted entry from minority leader was not overwritten")
		}
	}
	assertSafe(t, c)
}

func TestRestartedNodeCatchesUp(t *testing.T) {
	c := New(3, 5)
	waitForLeader(t, c, 200)
	follower := 1
	if follower == c.Leader() {
		follower = 2
	}
	c.Crash(follower)
	for i := range 20 {
		c.Propose(fmt.Sprintf("SET k %d", i))
		c.Step()
	}
	c.Run(50)
	c.Restart(follower)
	c.Run(200)

	leader := c.Node(c.Leader())
	if got, want := c.Node(follower).CommitIndex(), leader.CommitIndex(); got != want {
		t.Fatalf("restarted node commitIndex = %d, want %d", got, want)
	}
	assertSafe(t, c)
}

// TestChaos throws random crashes, restarts, partitions and message loss at
// many clusters and checks that no safety property is ever violated, and
// that the cluster recovers once the faults stop.
func TestChaos(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 30
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			size := 3 + 2*rng.Intn(2) // 3 or 5 nodes
			c := New(size, seed)
			c.DropRate = 0.2
			c.MaxLatency = 10

			for tick := range 3000 {
				c.Step()
				switch r := rng.Intn(1000); {
				case r < 8:
					c.Propose(fmt.Sprintf("SET k%d %d", rng.Intn(10), tick))
				case r < 10:
					c.Crash(1 + rng.Intn(size))
				case r < 14:
					c.Restart(1 + rng.Intn(size))
				case r < 16:
					perm := rng.Perm(size)
					cut := 1 + rng.Intn(size-1)
					a, b := []int{}, []int{}
					for i, p := range perm {
						if i < cut {
							a = append(a, p+1)
						} else {
							b = append(b, p+1)
						}
					}
					c.Partition(a, b)
				case r < 20:
					c.Heal()
				}
			}
			assertSafe(t, c)

			// Liveness: with all faults removed, the cluster must commit again.
			for id := 1; id <= size; id++ {
				c.Restart(id)
			}
			c.Heal()
			c.DropRate, c.MinLatency, c.MaxLatency = 0, 2, 6

			// Like a real client, retry: the first leader we find may be a
			// stale one that is about to be deposed.
			committed := false
			for attempt := 0; attempt < 10 && !committed; attempt++ {
				waitForLeader(t, c, 1000)
				c.Propose("SET final 1")
				c.Run(100)
				committed = slices.Contains(committedCommands(c), "SET final 1")
			}
			if !committed {
				t.Fatalf("cluster did not commit after recovery")
			}
			assertSafe(t, c)
		})
	}
}
