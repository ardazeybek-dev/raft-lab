// Command raft-lab runs a simulated Raft cluster in the terminal.
//
//	raft-lab                      # guided demo: election, crash, partition, heal
//	raft-lab -chaos -ticks 20000  # random faults, prints a safety report
//	raft-lab -chaos -seeds 500    # run many seeds, report any violation
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"

	"github.com/ardazeybek-dev/raft-lab/raft"
	"github.com/ardazeybek-dev/raft-lab/sim"
)

var version = "dev"

func main() {
	nodes := flag.Int("nodes", 5, "cluster size")
	seed := flag.Int64("seed", 1, "random seed; the same seed replays the same run")
	chaos := flag.Bool("chaos", false, "inject random crashes, restarts, partitions and message loss")
	ticks := flag.Int("ticks", 5000, "ticks to simulate in chaos mode")
	seeds := flag.Int("seeds", 1, "in chaos mode, run this many seeds starting at -seed")
	drop := flag.Float64("drop", 0.1, "message loss rate in chaos mode")
	quiet := flag.Bool("quiet", false, "do not print the event log")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("raft-lab", version)
		return
	}
	if *nodes < 1 || *nodes > 9 {
		fmt.Fprintln(os.Stderr, "-nodes must be between 1 and 9")
		os.Exit(2)
	}

	if !*chaos {
		c := sim.New(*nodes, *seed)
		demo(c)
		report(c, *quiet)
		exitOnViolation(c)
		return
	}

	failed := 0
	for s := *seed; s < *seed+int64(*seeds); s++ {
		c := sim.New(*nodes, s)
		c.DropRate = *drop
		runChaos(c, s, *ticks)
		if *seeds == 1 {
			report(c, *quiet)
		} else if len(c.Violations()) > 0 {
			failed++
			fmt.Printf("seed %d: %d violation(s): %s\n", s, len(c.Violations()), c.Violations()[0])
		}
	}
	if *seeds > 1 {
		fmt.Printf("%d/%d seeds safe (%d nodes, %d ticks each, %.0f%% message loss)\n",
			*seeds-failed, *seeds, *nodes, *ticks, *drop*100)
		if failed > 0 {
			os.Exit(1)
		}
	}
}

// demo walks through the interesting moments of Raft on a healthy cluster.
func demo(c *sim.Cluster) {
	step := func(title string) { fmt.Printf("\n\033[1m▶ %s\033[0m\n", title) }
	untilLeader := func() int {
		for range 2000 {
			c.Step()
			if l := c.Leader(); l != 0 {
				return l
			}
		}
		return 0
	}

	step("Cluster starts, every node is a follower; the first timer to expire starts an election")
	leader := untilLeader()
	c.Run(20)
	flush(c)

	step("Client writes go to the leader and commit once a majority stores them")
	c.Propose("SET user arda")
	c.Propose("SET lang go")
	c.Run(40)
	flush(c)

	if c.Size() >= 3 {
		step(fmt.Sprintf("Leader (node %d) crashes, the rest elect a new one", leader))
		c.Crash(leader)
		untilLeader()
		c.Propose("SET topic raft")
		c.Run(40)
		flush(c)

		step("Old leader comes back as a follower and catches up")
		c.Restart(leader)
		c.Run(80)
		flush(c)
	}

	if c.Size() >= 5 {
		cur := c.Leader()
		step(fmt.Sprintf("Network splits: node %d is cut off, the majority keeps going", cur))
		c.Isolate(cur)
		c.Node(cur).Propose("SET lost true") // accepted by the stale leader, never committed
		untilLeader()
		for c.Leader() == cur {
			c.Step()
		}
		c.Propose("SET partition survived")
		c.Run(60)
		flush(c)

		step("Network heals: the stale leader steps down and its uncommitted write is discarded")
		c.Heal()
		c.Run(120)
		flush(c)
	}
}

var printed int

func flush(c *sim.Cluster) {
	events := c.Events()
	for _, e := range events[min(printed, len(events)):] {
		fmt.Printf("  %s %s\n", dim(fmt.Sprintf("t=%-5d", e.Tick)), colour(e))
	}
	printed = len(events)
}

func runChaos(c *sim.Cluster, seed int64, ticks int) {
	rng := rand.New(rand.NewSource(seed))
	size := c.Size()
	for t := range ticks {
		c.Step()
		switch r := rng.Intn(1000); {
		case r < 8:
			c.Propose(fmt.Sprintf("SET k%d %d", rng.Intn(10), t))
		case r < 10:
			c.Crash(1 + rng.Intn(size))
		case r < 14:
			c.Restart(1 + rng.Intn(size))
		case r < 16 && size > 1:
			perm := rng.Perm(size)
			cut := 1 + rng.Intn(size-1)
			var a, b []int
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
}

func report(c *sim.Cluster, quiet bool) {
	if !quiet && printed == 0 {
		flush(c)
	}
	var commands []raft.Entry
	for _, e := range c.Committed() {
		if e.Command != raft.NoOp {
			commands = append(commands, e)
		}
	}
	state := sim.Apply(c.Committed())
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var kv []string
	for _, k := range keys {
		kv = append(kv, k+"="+state[k])
	}

	term := 0
	for id := 1; id <= c.Size(); id++ {
		term = max(term, c.Node(id).Term())
	}

	fmt.Printf("\n\033[1mSummary after %d ticks\033[0m\n", c.Now)
	fmt.Printf("  leader:       node %d\n", c.Leader())
	fmt.Printf("  term:         %d\n", term)
	fmt.Printf("  committed:    %d entries (%d client commands)\n", len(c.Committed()), len(commands))
	fmt.Printf("  state:        {%s}\n", strings.Join(kv, ", "))
	if v := c.Violations(); len(v) > 0 {
		fmt.Printf("  \033[31msafety:       %d violation(s)\033[0m\n", len(v))
		for _, s := range v {
			fmt.Println("   -", s)
		}
	} else {
		fmt.Printf("  \033[32msafety:       no violations\033[0m\n")
	}
}

func exitOnViolation(c *sim.Cluster) {
	if len(c.Violations()) > 0 {
		os.Exit(1)
	}
}

func colour(e sim.Event) string {
	code := map[string]string{
		"election": "33", "leader": "32;1", "commit": "36", "fault": "31",
		"heal": "34", "client": "35", "violation": "41;37",
	}[e.Kind]
	if code == "" || os.Getenv("NO_COLOR") != "" {
		return e.Text
	}
	return "\033[" + code + "m" + e.Text + "\033[0m"
}

func dim(s string) string {
	if os.Getenv("NO_COLOR") != "" {
		return s
	}
	return "\033[2m" + s + "\033[0m"
}
