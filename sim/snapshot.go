package sim

import (
	"strings"

	"github.com/ardazeybek-dev/raft-lab/raft"
)

// Snapshot is a JSON-friendly picture of the whole cluster at one tick,
// used by the browser visualizer.
type Snapshot struct {
	Now        int           `json:"now"`
	Leader     int           `json:"leader"`
	Nodes      []NodeView    `json:"nodes"`
	Messages   []MessageView `json:"messages"`
	Committed  int           `json:"committed"`
	Violations []string      `json:"violations"`
	Events     []Event       `json:"events"`
	DropRate   float64       `json:"dropRate"`
}

// NodeView is one node inside a Snapshot.
type NodeView struct {
	ID          int               `json:"id"`
	Role        string            `json:"role"`
	Term        int               `json:"term"`
	VotedFor    int               `json:"votedFor"`
	Leader      int               `json:"leader"`
	CommitIndex int               `json:"commitIndex"`
	Log         []raft.Entry      `json:"log"`
	Timer       float64           `json:"timer"`
	Down        bool              `json:"down"`
	Isolated    bool              `json:"isolated"`
	Group       int               `json:"group"`
	State       map[string]string `json:"state"`
}

// MessageView is an in-flight message; Progress goes from 0 (sent) to 1 (arriving).
type MessageView struct {
	From     int     `json:"from"`
	To       int     `json:"to"`
	Type     string  `json:"type"`
	Term     int     `json:"term"`
	Entries  int     `json:"entries"`
	OK       bool    `json:"ok"`
	Progress float64 `json:"progress"`
	Sent     int     `json:"sent"`
	Due      int     `json:"due"`
}

// Snapshot captures the current cluster state. At most lastEvents events are included.
func (c *Cluster) Snapshot(lastEvents int) Snapshot {
	s := Snapshot{
		Now:        c.Now,
		Leader:     c.Leader(),
		Committed:  len(c.committed),
		Violations: append([]string{}, c.violations...),
		DropRate:   c.DropRate,
		Messages:   []MessageView{}, // JSON [] rather than null
	}
	for i, n := range c.nodes {
		log := append([]raft.Entry{}, n.Log()...)
		s.Nodes = append(s.Nodes, NodeView{
			ID:          n.ID(),
			Role:        n.Role().String(),
			Term:        n.Term(),
			VotedFor:    n.VotedFor(),
			Leader:      n.Leader(),
			CommitIndex: n.CommitIndex(),
			Log:         log,
			Timer:       n.ElectionProgress(),
			Down:        c.down[i],
			Isolated:    c.IsIsolated(n.ID()),
			Group:       c.group[i],
			State:       Apply(log[:n.CommitIndex()]),
		})
	}
	for _, e := range c.inflight {
		m := e.msg
		span := float64(e.due - e.sentAt)
		s.Messages = append(s.Messages, MessageView{
			From:     m.From,
			To:       m.To,
			Type:     m.Type.String(),
			Term:     m.Term,
			Entries:  len(m.Entries),
			OK:       m.Granted || m.Success,
			Progress: float64(c.Now-e.sentAt) / span,
			Sent:     e.sentAt,
			Due:      e.due,
		})
	}
	start := max(0, len(c.events)-lastEvents)
	s.Events = append([]Event{}, c.events[start:]...)
	return s
}

// Apply runs committed commands against a tiny key-value state machine.
// Supported commands: "SET key value" and "DEL key"; anything else is ignored.
func Apply(entries []raft.Entry) map[string]string {
	kv := map[string]string{}
	for _, e := range entries {
		f := strings.Fields(e.Command)
		switch {
		case len(f) >= 3 && strings.EqualFold(f[0], "SET"):
			kv[f[1]] = strings.Join(f[2:], " ")
		case len(f) == 2 && strings.EqualFold(f[0], "DEL"):
			delete(kv, f[1])
		}
	}
	return kv
}
