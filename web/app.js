"use strict";

const $ = (s) => document.querySelector(s);
const SVG = "http://www.w3.org/2000/svg";
const CX = 300, CY = 300, R = 210, NODE_R = 40;

const ui = {
  ring: $("#ring"), play: $("#play"), step: $("#stepBtn"), speed: $("#speed"), tick: $("#tick"),
  write: $("#write"), cmd: $("#cmd"), writeMsg: $("#writeMsg"), selected: $("#selected"),
  split: $("#split"), heal: $("#heal"), drop: $("#drop"), dropOut: $("#dropOut"),
  kv: $("#kv"), size: $("#size"), seed: $("#seed"), reset: $("#reset"),
  logGrid: $("#logGrid"), events: $("#events"), safety: $("#safety"), loading: $("#loading"),
};

let snap = null;
let running = true;
let selectedId = 0;
let acc = 0;
let last = performance.now();
const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

// ---------- boot ----------

window.addEventListener("raftlab:ready", () => {
  ui.loading.remove();
  snap = JSON.parse(raftLab.snapshot());
  renderAll(0);
  requestAnimationFrame(frame);
});

const go = new Go();
WebAssembly.instantiateStreaming(fetch("raft.wasm"), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((err) => { ui.loading.textContent = "Could not load the simulator: " + err.message; });

// ---------- loop ----------

function frame(now) {
  const dt = Math.min(now - last, 250);
  last = now;
  if (running) {
    acc += dt * ui.speed.value / 1000; // speed = ticks per second
    const whole = Math.floor(acc);
    if (whole > 0) {
      acc -= whole;
      snap = JSON.parse(raftLab.step(whole));
      renderSlow();
    }
  }
  renderRing(running && !reduceMotion ? acc : 0);
  requestAnimationFrame(frame);
}

function act(fn) {
  fn();
  snap = JSON.parse(raftLab.snapshot());
  renderAll(acc);
}

function renderAll(frac) {
  renderSlow();
  renderRing(frac);
}

// Everything except the ring changes at most once per tick.
function renderSlow() {
  ui.tick.textContent = "t = " + snap.now;
  renderSafety();
  renderSelected();
  renderKV();
  renderLogs();
  renderEvents();
}

// ---------- ring ----------

function pos(id) {
  const n = snap.nodes.length;
  const a = -Math.PI / 2 + (2 * Math.PI * (id - 1)) / n;
  return [CX + R * Math.cos(a), CY + R * Math.sin(a)];
}

function el(tag, attrs, parent) {
  const e = document.createElementNS(SVG, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}

const roleLabel = { leader: "leader", candidate: "candidate", follower: "follower" };

function renderRing(frac) {
  const svg = ui.ring;
  svg.replaceChildren();
  const nodes = snap.nodes;

  // Links only between servers that can currently reach each other.
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      const a = nodes[i], b = nodes[j];
      if (a.down || b.down || a.group !== b.group) continue;
      const [x1, y1] = pos(a.id), [x2, y2] = pos(b.id);
      el("line", { class: "link", x1, y1, x2, y2 }, svg);
    }
  }

  // Centre caption.
  const leader = snap.leader;
  el("text", { class: "center-big", x: CX, y: CY - 6 }, svg).textContent =
    leader ? `Node ${leader} leads term ${nodes[leader - 1].term}` : "No leader";
  el("text", { class: "center", x: CX, y: CY + 18 }, svg).textContent =
    `${snap.committed} entries committed`;

  // In-flight messages, offset sideways so requests and replies don't overlap.
  for (const m of snap.messages) {
    const [x1, y1] = pos(m.from), [x2, y2] = pos(m.to);
    const t = Math.min(1, Math.max(0, (snap.now - m.sent + frac) / Math.max(1, m.due - m.sent)));
    const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1;
    const ox = (-dy / len) * 7, oy = (dx / len) * 7;
    const x = x1 + dx * t + ox, y = y1 + dy * t + oy;
    let cls = "msg-resp" + (m.ok ? "" : " nack");
    if (m.type === "RequestVote") cls = "msg-vote";
    if (m.type === "AppendEntries") cls = "msg-app";
    const r = m.entries > 0 ? 7 : 4.5;
    el("circle", { class: cls, cx: x, cy: y, r }, svg);
  }

  for (const n of nodes) {
    const [x, y] = pos(n.id);
    const g = el("g", {
      class: `node ${n.role}${n.role === "leader" && n.id !== snap.leader ? " stale" : ""}${n.down ? " down" : ""}${n.id === selectedId ? " selected" : ""}`,
      transform: `translate(${x},${y})`,
      tabindex: 0,
      role: "button",
      "aria-label": `Node ${n.id}, ${n.down ? "crashed" : n.role}, term ${n.term}`,
    }, svg);
    g.addEventListener("click", () => select(n.id));
    g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(n.id); } });

    el("circle", { class: "halo", r: NODE_R + 10 }, g);
    el("circle", { class: "body", r: NODE_R }, g);
    if (!n.down && n.role !== "leader" && n.timer > 0) {
      el("circle", {
        class: "timer", r: NODE_R + 5, pathLength: 1, transform: "rotate(-90)",
        "stroke-dasharray": `${Math.min(1, n.timer)} 1`,
      }, g);
    }
    el("text", { class: "term", y: -4 }, g).textContent = n.term;
    el("text", { class: "sub", y: 20 }, g).textContent = "term";
    el("text", { class: "name", y: NODE_R + 28 }, g).textContent = `Node ${n.id}`;
    el("text", { class: "role", y: NODE_R + 45 }, g).textContent =
      n.down ? "crashed"
        : n.role === "leader" && n.id !== snap.leader ? "stale leader"
        : n.isolated && nodes.length > 1 ? `${roleLabel[n.role]}, cut off` : roleLabel[n.role];
  }
}

// ---------- side panel ----------

function renderSafety() {
  const v = snap.violations;
  ui.safety.className = "safety " + (v.length ? "bad" : "ok");
  ui.safety.innerHTML = v.length
    ? `<strong>${v.length} safety violation${v.length > 1 ? "s" : ""}</strong><span>${escapeHTML(v[0])}</span>`
    : `<strong>Safety checks passing</strong><span>No two leaders in a term, no committed entry ever changed.</span>`;
}

function select(id) {
  selectedId = selectedId === id ? 0 : id;
  renderSelected();
  renderRing(acc);
}

function renderSelected() {
  const n = snap.nodes[selectedId - 1];
  if (!n) {
    ui.selected.innerHTML = `<p class="hint">Select a server in the diagram to crash it, restart it or cut it off.</p>`;
    return;
  }
  const voted = n.votedFor ? `node ${n.votedFor}` : "nobody";
  const leader = n.leader ? `node ${n.leader}` : "unknown";
  const status = n.down ? "crashed" : n.role + (n.isolated ? ", cut off" : "");
  ui.selected.innerHTML = `
    <div class="node-card">
      <h3>Node ${n.id} <span class="hint">${status}</span></h3>
      <dl class="facts">
        <div><dt>Term</dt><dd>${n.term}</dd></div>
        <div><dt>Voted for</dt><dd>${voted}</dd></div>
        <div><dt>Knows leader</dt><dd>${leader}</dd></div>
        <div><dt>Log length</dt><dd>${n.log.length}</dd></div>
        <div><dt>Committed</dt><dd>${n.commitIndex}</dd></div>
      </dl>
      <div class="row wrap">
        ${n.down
          ? `<button type="button" data-node="restart">Restart node ${n.id}</button>`
          : `<button type="button" class="danger" data-node="crash">Crash node ${n.id}</button>`}
        ${n.isolated && snap.nodes.length > 1
          ? `<button type="button" data-node="heal">Reconnect</button>`
          : `<button type="button" data-node="isolate">Cut node ${n.id} off</button>`}
      </div>
    </div>`;
}

ui.selected.addEventListener("click", (e) => {
  const b = e.target.closest("[data-node]");
  if (!b) return;
  const id = selectedId;
  act(() => {
    if (b.dataset.node === "crash") raftLab.crash(id);
    if (b.dataset.node === "restart") raftLab.restart(id);
    if (b.dataset.node === "isolate") raftLab.isolate(id);
    if (b.dataset.node === "heal") raftLab.heal();
  });
});

function renderKV() {
  // Every node's committed state is a prefix of the same history, so the
  // live node that has committed the most shows the most complete picture.
  const live = snap.nodes.filter((n) => !n.down);
  const ref = (live.length ? live : snap.nodes).reduce((a, b) => (b.commitIndex > a.commitIndex ? b : a));
  const keys = Object.keys(ref.state).sort();
  ui.kv.innerHTML = keys.length
    ? keys.map((k) => `<dt>${escapeHTML(k)}</dt><dd>${escapeHTML(ref.state[k])}</dd>`).join("")
    : `<dd class="empty">Nothing committed yet. Send a write.</dd>`;
}

// ---------- logs & events ----------

const termColor = (t) => `hsl(${(t * 67 + 170) % 360} 55% 42%)`;

function renderLogs() {
  const rows = snap.nodes.map((n) => {
    const cells = n.log.map((e, i) => {
      const done = i < n.commitIndex ? " done" : "";
      const noop = e.command === "" ? " noop" : "";
      const tip = e.command === "" ? `#${i + 1} no-op (term ${e.term})` : `#${i + 1} ${e.command} (term ${e.term})`;
      return `<span class="cell${done}${noop}" style="--c:${termColor(e.term)}" title="${escapeHTML(tip)}">${e.term}</span>`;
    }).join("");
    return `<div class="log-row${n.down ? " down" : ""}"><span class="who">Node ${n.id}<small>${n.down ? "crashed" : n.role}</small></span>${cells}</div>`;
  }).join("");
  const stick = ui.logGrid.scrollLeft + ui.logGrid.clientWidth >= ui.logGrid.scrollWidth - 4;
  ui.logGrid.innerHTML = rows;
  if (stick) ui.logGrid.scrollLeft = ui.logGrid.scrollWidth;
}

let shownEvents = "";
function renderEvents() {
  const key = snap.events.length ? snap.events[snap.events.length - 1].tick + snap.events[snap.events.length - 1].text : "";
  if (key === shownEvents) return;
  shownEvents = key;
  ui.events.innerHTML = [...snap.events].reverse()
    .map((e) => `<li class="${e.kind}"><time>t=${e.tick}</time><span>${escapeHTML(e.text)}</span></li>`).join("");
}

function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// ---------- controls ----------

ui.play.addEventListener("click", () => {
  running = !running;
  ui.play.textContent = running ? "Pause" : "Play";
});

ui.step.addEventListener("click", () => {
  running = false;
  ui.play.textContent = "Play";
  acc = 0;
  snap = JSON.parse(raftLab.step(1));
  renderAll(0);
});

ui.write.addEventListener("submit", (e) => {
  e.preventDefault();
  const cmd = ui.cmd.value.trim();
  if (!cmd) return;
  let err = "";
  act(() => { err = raftLab.propose(cmd); });
  ui.writeMsg.classList.toggle("error", !!err);
  ui.writeMsg.textContent = err
    ? "No leader right now. Wait for an election to finish, then send again."
    : `Sent to node ${snap.leader}. Watch the cell turn solid once a majority has it.`;
  if (!err) ui.cmd.value = nextSuggestion();
});

const samples = ["SET region eu-west", "SET owner arda", "SET replicas 5", "DEL color", "SET color amber", "SET lang go"];
let sample = 0;
const nextSuggestion = () => samples[sample++ % samples.length];

ui.split.addEventListener("click", () => act(() => {
  // A random majority/minority split.
  const ids = snap.nodes.map((n) => n.id).sort(() => Math.random() - .5);
  const cut = Math.floor(ids.length / 2);
  raftLab.partition(JSON.stringify([ids.slice(0, cut), ids.slice(cut)]));
}));
ui.heal.addEventListener("click", () => act(() => raftLab.heal()));

ui.drop.addEventListener("input", () => {
  ui.dropOut.textContent = ui.drop.value + "%";
  raftLab.setDropRate(ui.drop.value / 100);
});

ui.reset.addEventListener("click", () => {
  selectedId = 0;
  ui.drop.value = 0;
  ui.dropOut.textContent = "0%";
  snap = JSON.parse(raftLab.reset(+ui.size.value, Math.max(1, +ui.seed.value || 1)));
  renderAll(0);
});

document.querySelector(".try").addEventListener("click", (e) => {
  const b = e.target.closest("[data-try]");
  if (!b || !snap) return;
  const leader = snap.leader;
  const say = (msg, isErr) => { ui.writeMsg.textContent = msg; ui.writeMsg.classList.toggle("error", !!isErr); };
  switch (b.dataset.try) {
    case "crash":
      if (!leader) return say("No leader to crash yet. Wait a moment.", true);
      selectedId = leader;
      act(() => raftLab.crash(leader));
      break;
    case "isolate":
      if (!leader) return say("No leader to cut off yet. Wait a moment.", true);
      selectedId = leader;
      act(() => raftLab.isolate(leader));
      ui.cmd.value = "SET orphan true";
      say(`Node ${leader} is cut off. Send a write now: it reaches the old leader only if you are quick.`);
      break;
    case "heal":
      act(() => raftLab.heal());
      break;
    case "storm":
      ui.drop.value = 40;
      ui.drop.dispatchEvent(new Event("input"));
      break;
  }
  if (!running) ui.play.click();
  $(".lab").scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth", block: "start" });
});
