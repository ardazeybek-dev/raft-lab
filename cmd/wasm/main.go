//go:build js && wasm

// Command wasm exposes the simulator to JavaScript as a global `raftLab`
// object. State crosses the boundary as JSON strings to keep the bridge thin.
package main

import (
	"encoding/json"
	"syscall/js"

	"github.com/ardazeybek-dev/raft-lab/sim"
)

var cluster *sim.Cluster

func snapshot() any {
	b, _ := json.Marshal(cluster.Snapshot(60))
	return string(b)
}

func intArg(args []js.Value, i int) int { return args[i].Int() }

func main() {
	cluster = sim.New(5, 1)

	api := map[string]any{
		// reset(size, seed)
		"reset": js.FuncOf(func(_ js.Value, a []js.Value) any {
			cluster = sim.New(intArg(a, 0), int64(intArg(a, 1)))
			return snapshot()
		}),
		// step(ticks) advances the cluster and returns the new snapshot.
		"step": js.FuncOf(func(_ js.Value, a []js.Value) any {
			cluster.Run(intArg(a, 0))
			return snapshot()
		}),
		"snapshot": js.FuncOf(func(js.Value, []js.Value) any { return snapshot() }),
		// propose(command) returns an error message, or "" on success.
		"propose": js.FuncOf(func(_ js.Value, a []js.Value) any {
			if _, _, err := cluster.Propose(a[0].String()); err != nil {
				return err.Error()
			}
			return ""
		}),
		"crash":   js.FuncOf(func(_ js.Value, a []js.Value) any { cluster.Crash(intArg(a, 0)); return nil }),
		"restart": js.FuncOf(func(_ js.Value, a []js.Value) any { cluster.Restart(intArg(a, 0)); return nil }),
		"isolate": js.FuncOf(func(_ js.Value, a []js.Value) any { cluster.Isolate(intArg(a, 0)); return nil }),
		"heal":    js.FuncOf(func(js.Value, []js.Value) any { cluster.Heal(); return nil }),
		// partition(json) takes groups such as "[[1,2],[3,4,5]]".
		"partition": js.FuncOf(func(_ js.Value, a []js.Value) any {
			var groups [][]int
			if err := json.Unmarshal([]byte(a[0].String()), &groups); err != nil {
				return err.Error()
			}
			cluster.Partition(groups...)
			return ""
		}),
		"setDropRate": js.FuncOf(func(_ js.Value, a []js.Value) any {
			cluster.DropRate = a[0].Float()
			return nil
		}),
	}
	js.Global().Set("raftLab", js.ValueOf(api))
	js.Global().Call("dispatchEvent", js.Global().Get("Event").New("raftlab:ready"))
	select {} // keep the Go runtime alive
}
