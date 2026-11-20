// Given: quorum.js / quorum.wasm, the real engine behind web::Simulation.
// Approach: the page only asks the engine to act (tick, crash, restart,
//           partition, heal, submit, load, chaos) and draws what snapshot()
//           and eventsJson() report. No Raft logic lives here. Between ticks,
//           positions are interpolated so messages glide.
//   - Every action is recorded with the tick it happened at. The URL holds
//     the seed and that list, so a copied link replays the same run exactly.
//   - Scrubbing the timeline back rebuilds the engine from the seed and
//     replays the recorded actions up to that tick (fast: about 1 ms per 2,000
//     ticks). The rest of the recording stays pending, so playing on retraces
//     it; any new action instead starts a new branch from that moment.
//   - The canvases are sized in device pixels, so they stay crisp on HiDPI.

import createQuorum from './quorum.js';

const SERVERS = 5;
const SPEEDS = [5, 15, 40, 100, 300];          // ticks per second
const SPEED_NAMES = ['very slow', 'slow', 'normal', 'fast', 'very fast'];
const MAX_TICKS_PER_FRAME = 400;
const LOG_ROWS = 6;
const DROP_MEMORY = 12;                        // matches Simulation::kDropMemory
const TL_SCALE = 0.25;                         // timeline pixels per tick

const css = getComputedStyle(document.documentElement);
const color = name => css.getPropertyValue(name).trim();
const C = {
  page: color('--page'), card: color('--card'), ink: color('--ink'), soft: color('--ink-soft'),
  line: color('--line'), accent: color('--accent'), accentSoft: color('--accent-soft'),
  heartbeat: color('--heartbeat'), vote: color('--vote'), append: color('--append'), client: color('--client'),
};
const KIND_COLOR = {
  heartbeat: C.heartbeat, vote: C.vote, append: C.append, request: C.client, reply: C.client, other: C.soft,
};

const $ = id => document.getElementById(id);
const canvas = $('ring');
const ctx = canvas.getContext('2d');

let Q = null;          // the WASM module
let sim = null;        // web::Simulation
let snap = null;       // last parsed snapshot
let playing = false;
let speed = 2;
let acc = 0;           // fractional ticks not yet run
let seed = 0;
let actions = [];      // [tick, code] applied on this branch, in order
let pending = [];      // recorded actions still ahead of now (replay or after scrubbing back)
let events = [];       // the engine's events, mirrored incrementally
let ghost = null;      // after scrubbing back: {events, head} of the recorded run ahead
let splitMode = null;  // Set of server ids on side A while choosing
const angle = new Map();       // server id -> current drawn angle
const lastCommitted = new Map(); // server id -> highest committed index drawn
const lastLogSig = new Map();
let lastOpsSig = '';
let localErrors = [];  // console lines the engine never saw (parse errors)
let view = { w: 0, h: 0, dpr: 1 };
let tlView = { w: 0, h: 0, dpr: 1 };

// ---------------------------------------------------------------- engine --

function parseHash() {
  const h = new URLSearchParams(location.hash.slice(1));
  const s = Number.parseInt(h.get('seed') ?? '', 10);
  let run = [];
  try { run = JSON.parse(h.get('run') ?? '[]'); } catch { run = []; }
  if (!Array.isArray(run)) run = [];
  return {
    seed: Number.isFinite(s) && s >= 0 && s < 1e9 ? s : null,
    run: run.filter(a => Array.isArray(a) && a.length === 2 && Number.isInteger(a[0]) && typeof a[1] === 'string'),
  };
}

// The whole recording of this branch: what happened plus what is still ahead.
function recording() { return actions.concat(pending); }

function writeHash() {
  const p = new URLSearchParams({ seed: String(seed) });
  const run = recording();
  if (run.length) p.set('run', JSON.stringify(run));
  history.replaceState(null, '', '#' + p.toString());
}

// A fresh engine for `s`, with `replay` scheduled. Nothing runs yet.
function rebuild(s, replay) {
  if (sim) sim.delete();
  seed = s;
  sim = Q.create(s, SERVERS);
  actions = [];
  pending = replay.slice().sort((a, b) => a[0] - b[0]);
  events = [];
  lastCommitted.clear(); lastLogSig.clear(); lastOpsSig = '';
  acc = 0;
}

function reset(s, replay = []) {
  rebuild(s, replay);
  ghost = null;
  angle.clear(); localErrors = [];
  cancelSplit();
  $('seed').value = String(s);
  applyDue();
  refresh();
  writeHash();
}

// Runs one recorded action against the engine.
function perform(code) {
  const kind = code[0];
  const arg = code.slice(1);
  if (kind === 'c') return sim.crash(Number(arg));
  if (kind === 'r') return sim.restart(Number(arg));
  if (kind === 'p') return sim.partition(arg);
  if (kind === 'h') { sim.heal(); return true; }
  if (kind === 'l') { sim.setLoad(arg === '1'); return true; }
  if (kind === 'x') return sim.startChaos();
  if (kind === 's') {
    const [op, key, value] = arg.split(':').map(decodeURIComponent);
    return sim.submit(1, op, key, value ?? '') > 0;
  }
  return false;
}

// A user action. Whatever was recorded ahead of now is dropped: this starts a
// new branch from the current moment.
function act(code) {
  pending = [];
  ghost = null;
  if (perform(code)) actions.push([sim.now(), code]);
  writeHash();
  refresh();
}

function applyDue() {
  while (pending.length && pending[0][0] <= sim.now()) {
    const [, code] = pending.shift();
    if (perform(code)) actions.push([sim.now(), code]);
  }
}

// Advances n ticks, stopping at every recorded action's tick.
function advance(n) {
  while (n > 0) {
    applyDue();
    const next = pending.length ? pending[0][0] - sim.now() : n;
    const step = Math.max(1, Math.min(n, next));
    sim.tick(step);
    n -= step;
  }
  applyDue();
}

// The furthest tick this branch has reached.
function head() { return Math.max(sim.now(), ghost ? ghost.head : 0); }

// Shows exactly tick t: forward by running on, backward by rebuilding from
// the seed and replaying the recording up to t.
function scrubTo(t) {
  t = Math.max(0, Math.min(Math.round(t), head()));
  if (t === sim.now()) return;
  setPlaying(false);
  if (!ghost) ghost = { events: events.slice(), head: sim.now() };
  if (t < sim.now()) {
    const run = recording();
    rebuild(seed, run);
  }
  advance(t - sim.now());
  refresh();
}

function fetchEvents() {
  const r = JSON.parse(sim.eventsJson(events.length));
  if (r.first > events.length) events.length = 0;  // the engine dropped old ones
  for (const e of r.events) events.push(e);
  // Past the end of the old recording: nothing is left to retrace.
  if (ghost && sim.now() >= ghost.head && pending.length === 0) ghost = null;
}

function refresh() {
  snap = JSON.parse(sim.snapshot());
  fetchEvents();
  renderLogs();
  renderConsole();
  renderStatus();
  renderMetrics();
  tlDirty = true;
}

// ---------------------------------------------------------------- layout --

function resize() {
  const r = canvas.parentElement.getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 3);
  view = { w: r.width, h: r.height, dpr };
  canvas.width = Math.round(r.width * dpr);
  canvas.height = Math.round(r.height * dpr);
}

function geometry() {
  const { w, h } = view;
  const s = Math.min(w, h);
  const nodeR = Math.max(20, Math.min(36, s * 0.075));
  return { cx: w / 2, cy: h / 2 + (w < 500 ? 0 : 4), R: s * 0.33, nodeR, client: { x: 26, y: h - 16 } };
}

// Target ring slots: by id, or grouped by partition side so a wall can run
// between the sides.
function targetAngles(nodes) {
  const order = nodes.map(n => n).sort((a, b) => (a.group - b.group) || (a.id - b.id));
  const out = new Map();
  order.forEach((n, k) => out.set(n.id, -Math.PI / 2 + (2 * Math.PI * k) / order.length));
  return out;
}

function partitioned(nodes) { return nodes.some(n => n.group !== 0); }

function nodePos(id, g) {
  if (id >= 1000) return { x: g.client.x, y: g.client.y };
  const a = angle.get(id) ?? 0;
  return { x: g.cx + g.R * Math.cos(a), y: g.cy + g.R * Math.sin(a) };
}

// ---------------------------------------------------------------- canvas --

function draw(dt) {
  const { w, h, dpr } = view;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  if (!snap || w === 0) return;
  const g = geometry();
  const nodes = snap.nodes;

  // Glide servers toward their slots.
  const target = targetAngles(nodes);
  const k = 1 - Math.exp(-dt * 6);
  let settled = true;
  for (const n of nodes) {
    const t = target.get(n.id);
    if (!angle.has(n.id)) { angle.set(n.id, t); continue; }
    let d = t - angle.get(n.id);
    d = Math.atan2(Math.sin(d), Math.cos(d));
    if (Math.abs(d) > 0.002) settled = false;
    angle.set(n.id, angle.get(n.id) + d * k);
  }

  // The ring.
  ctx.strokeStyle = C.line;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(g.cx, g.cy, g.R, 0, Math.PI * 2);
  ctx.stroke();

  if (partitioned(nodes)) drawWall(nodes, target, g, settled);
  drawClient(g);

  const now = snap.now + (playing ? acc : 0);
  // Drops: a fading cross a little way along the path.
  for (const d of snap.drops) {
    const age = now - d.at;
    if (age < 0 || age > DROP_MEMORY) continue;
    const a = nodePos(d.from, g), b = nodePos(d.to, g);
    const p = d.reason === 'partition' ? 0.42 : 0.2;
    cross(a.x + (b.x - a.x) * p, a.y + (b.y - a.y) * p, 4, C.soft, 1 - age / DROP_MEMORY);
  }
  // Messages in flight.
  for (const m of snap.messages) {
    const a = nodePos(m.from, g), b = nodePos(m.to, g);
    const span = Math.max(1, m.at - m.sent);
    const p = Math.min(1, Math.max(0, (now - m.sent) / span));
    const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1;
    const ox = (-dy / len) * 4, oy = (dx / len) * 4;  // keep the two directions apart
    const fromR = m.from >= 1000 ? 12 : g.nodeR, toR = m.to >= 1000 ? 12 : g.nodeR;
    const s = fromR / len, e = 1 - toR / len;
    const q = s + (e - s) * p;
    const x = a.x + dx * q + ox, y = a.y + dy * q + oy;
    const col = KIND_COLOR[m.kind] ?? C.soft;
    ctx.globalAlpha = m.doomed ? 0.45 : 1;
    ctx.beginPath();
    ctx.arc(x, y, m.kind === 'append' ? 5 : 4, 0, Math.PI * 2);
    if (m.doomed) {
      ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.stroke();
    } else {
      ctx.fillStyle = col; ctx.fill();
    }
    ctx.globalAlpha = 1;
  }

  for (const n of nodes) drawNode(n, g);
}

function drawWall(nodes, target, g, settled) {
  // A dashed wall from the centre out between every pair of neighbouring
  // slots that are on different sides.
  const order = [...nodes].sort((a, b) => target.get(a.id) - target.get(b.id));
  ctx.save();
  ctx.globalAlpha = settled ? 1 : 0.35;
  ctx.strokeStyle = C.soft;
  ctx.lineWidth = 2;
  ctx.setLineDash([7, 6]);
  ctx.lineCap = 'round';
  for (let i = 0; i < order.length; ++i) {
    const a = order[i], b = order[(i + 1) % order.length];
    if (a.group === b.group) continue;
    let ta = target.get(a.id), tb = target.get(b.id);
    if (tb <= ta) tb += Math.PI * 2;
    const mid = (ta + tb) / 2;
    ctx.beginPath();
    ctx.moveTo(g.cx, g.cy);
    ctx.lineTo(g.cx + Math.cos(mid) * g.R * 1.32, g.cy + Math.sin(mid) * g.R * 1.32);
    ctx.stroke();
  }
  ctx.restore();
}

function drawClient(g) {
  const { x, y } = g.client;
  ctx.fillStyle = C.card;
  ctx.strokeStyle = C.client;
  ctx.lineWidth = 1.5;
  roundRect(x - 13, y - 9, 26, 18, 5);
  ctx.fill(); ctx.stroke();
  ctx.fillStyle = C.soft;
  ctx.font = '12px "IBM Plex Sans", system-ui, sans-serif';
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillText('client', x + 19, y + 0.5);
}

function drawNode(n, g) {
  const { x, y } = nodePos(n.id, g);
  const r = g.nodeR;
  const leader = n.up && n.role === 'leader';
  const candidate = n.up && n.role === 'candidate';
  const side = splitMode ? (splitMode.has(n.id) ? 'a' : 'b') : null;

  // Election timer: how close a follower or candidate is to starting an election.
  if (n.up && !leader && n.timeout > 0) {
    ctx.strokeStyle = candidate ? C.vote : C.line;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(x, y, r + 5, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * Math.min(1, n.timer / n.timeout));
    ctx.stroke();
  }

  ctx.beginPath();
  ctx.arc(x, y, r, 0, Math.PI * 2);
  if (!n.up) {
    ctx.fillStyle = '#F1E8DB';
    ctx.fill();
    ctx.setLineDash([4, 4]);
    ctx.strokeStyle = C.soft;
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.setLineDash([]);
  } else {
    ctx.fillStyle = leader ? C.accent : candidate ? '#FFF6E2' : '#FFFFFF';
    ctx.fill();
    ctx.strokeStyle = leader ? C.accent : candidate ? C.vote : C.ink;
    ctx.lineWidth = candidate ? 2.5 : 1.5;
    ctx.stroke();
  }
  if (side) {
    ctx.strokeStyle = side === 'a' ? C.accent : C.client;
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(x, y, r + 9, 0, Math.PI * 2);
    ctx.stroke();
  }

  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = leader ? '#FFFFFF' : n.up ? C.ink : C.soft;
  ctx.font = `600 ${Math.round(r * 0.5)}px "IBM Plex Sans", system-ui, sans-serif`;
  ctx.fillText(`S${n.id}`, x, y - (n.up ? r * 0.12 : 0));
  if (n.up) {
    ctx.font = `${Math.round(Math.max(10, r * 0.32))}px "IBM Plex Sans", system-ui, sans-serif`;
    ctx.fillStyle = leader ? 'rgba(255,255,255,.85)' : C.soft;
    ctx.fillText(`term ${n.term}`, x, y + r * 0.36);
  }

  // Role label on the outside of the ring: above servers in the top half,
  // below the others.
  ctx.font = `${Math.round(Math.max(11, r * 0.36))}px "IBM Plex Sans", system-ui, sans-serif`;
  ctx.fillStyle = leader ? C.accent : C.soft;
  const above = y < g.cy - 1;
  ctx.fillText(n.up ? n.role : 'crashed', x, above ? y - r - 16 : y + r + 18);
}

function cross(x, y, s, col, alpha) {
  ctx.save();
  ctx.globalAlpha = Math.max(0, alpha);
  ctx.strokeStyle = col;
  ctx.lineWidth = 1.6;
  ctx.beginPath();
  ctx.moveTo(x - s, y - s); ctx.lineTo(x + s, y + s);
  ctx.moveTo(x + s, y - s); ctx.lineTo(x - s, y + s);
  ctx.stroke();
  ctx.restore();
}

function roundRect(x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function hit(px, py) {
  if (!snap) return null;
  const g = geometry();
  for (const n of snap.nodes) {
    const { x, y } = nodePos(n.id, g);
    if (Math.hypot(px - x, py - y) <= g.nodeR + 6) return n;
  }
  return null;
}

// ---------------------------------------------------------------- panels --

const CHECK = '<svg viewBox="0 0 12 12" aria-hidden="true"><path d="M2.4 6.4l2.3 2.3 4.9-5"/></svg>';

function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function describeEntry(e) {
  if (e.noop) return 'no-op';
  if (e.raw !== undefined) return e.raw;
  return e.op === 'put' ? `put ${e.key} = ${e.value}` : `${e.op} ${e.key}`;
}

function renderLogs() {
  const box = $('logs');
  if (box.children.length !== snap.nodes.length) {
    box.style.setProperty('--servers', snap.nodes.length);
    box.innerHTML = snap.nodes.map(n => `<article class="card log" data-id="${n.id}"></article>`).join('');
  }
  snap.nodes.forEach((n, k) => {
    const el = box.children[k];
    const sig = JSON.stringify([n.up, n.role, n.term, n.commit, n.lastIndex, n.log.length && n.log[n.log.length - 1]]);
    if (lastLogSig.get(n.id) === sig) return;
    lastLogSig.set(n.id, sig);
    const prev = lastCommitted.get(n.id) ?? 0;
    const rows = n.log.slice(-LOG_ROWS).map(e => {
      const cls = [e.committed ? 'committed' : '', e.noop ? 'noop' : '', e.committed && e.i > prev && prev > 0 ? 'fresh' : '']
        .filter(Boolean).join(' ');
      return `<li class="${cls}" title="index ${e.i}, term ${e.t}${e.committed ? ', committed' : ''}">` +
        `<span class="i">${e.i}</span><span class="t">t${e.t}</span><span class="c">${esc(describeEntry(e))}</span>${CHECK}</li>`;
    }).join('');
    lastCommitted.set(n.id, n.up ? n.commit : 0);
    el.className = `card log${n.up && n.role === 'leader' ? ' leader' : ''}${n.up ? '' : ' down'}`;
    const sub = n.up
      ? `commit ${n.commit} · last ${n.lastIndex}`
      : `log kept on disk · last ${n.lastIndex}`;
    el.innerHTML =
      `<header><b>S${n.id}</b><span class="role">${n.up ? n.role : 'crashed'} · term ${n.term}</span></header>` +
      `<div class="sub">${sub}</div>` +
      (rows ? `<ol>${rows}</ol>` : `<ol><li class="empty">empty log</li></ol>`);
  });
}

function plural(n, word) { return `${n} ${word}${n === 1 ? '' : 's'}`; }

function renderConsole() {
  const ops = snap.ops.filter(o => o.client === 1);
  const chaos = snap.chaos;
  const left = chaos && chaos.phase === 'running'
    ? Math.max(0, Math.ceil((chaos.end - snap.now) / snap.metrics.ticksPerSecond)) : 0;
  const sig = JSON.stringify([ops, localErrors, chaos, left]);
  if (sig === lastOpsSig) return;
  lastOpsSig = sig;
  const out = $('out');
  const items = ops.map(o => {
    const cmd = o.op === 'put' ? `put ${o.key} ${o.value}` : `${o.op} ${o.key}`;
    let reply, meta = '', cls = '';
    if (o.status === 'queued') {
      reply = 'queued behind the previous command'; cls = 'pending';
    } else if (o.status === 'running') {
      reply = 'waiting for a reply…'; cls = 'pending';
      meta = `sent ${plural(o.sends, 'time')} so far, started at t=${o.invoked}`;
    } else {
      reply = o.op === 'get' ? (o.result === null ? 'not found' : `"${o.result}"`) : 'ok';
      const retries = o.sends - 1;
      const why = [o.redirects && plural(o.redirects, 'redirect'), o.timeouts && plural(o.timeouts, 'timeout')]
        .filter(Boolean).join(', ');
      meta = `S${o.server} answered · ${plural(o.completed - o.invoked, 'tick')} · ` +
        (retries ? `${retries} ${retries === 1 ? 'retry' : 'retries'} (${why})` : 'no retries');
    }
    return { at: o.invoked ?? Infinity, html: `<li class="${cls}"><div class="cmdline">${esc(cmd)}</div><div class="reply">${esc(reply)}</div>${meta ? `<div class="meta">${esc(meta)}</div>` : ''}</li>` };
  });
  for (const e of localErrors) items.push({ at: e.at, html: `<li class="error"><div class="cmdline">${esc(e.line)}</div><div class="reply">${esc(e.msg)}</div></li>` });
  if (chaos) {
    const secs = Math.round((chaos.end - chaos.start) / snap.metrics.ticksPerSecond);
    let reply, cls = 'check', detail = '';
    if (chaos.phase === 'running') {
      reply = `running: ${left} s left`;
    } else if (chaos.phase === 'settling') {
      reply = 'healing and waiting for the clients to finish…';
    } else if (chaos.verdict === 'linearizable') {
      reply = `${chaos.ops} operations, linearizable ✓`;
      cls += ' ok';
    } else {
      reply = `${chaos.ops} operations, ${chaos.verdict}`;
      cls += ' bad';
      detail = `<pre>${esc(chaos.explanation)}</pre>`;
    }
    items.push({ at: chaos.start, html: `<li class="${cls}" id="chaosResult"><div class="cmdline">chaos for ${secs} simulated seconds</div><div class="reply">${esc(reply)}</div>${detail}</li>` });
  }
  items.sort((a, b) => a.at - b.at);
  out.innerHTML = items.length
    ? items.map(i => i.html).join('')
    : '<li class="empty">Try <b>put x 1</b>, then <b>get x</b>. Commands go through a client that finds the leader, follows redirects and retries on timeouts.</li>';
  out.scrollTop = out.scrollHeight;
}

function renderMetrics() {
  const m = snap.metrics;
  const secs = t => (t / m.ticksPerSecond).toFixed(1) + ' s';
  $('mWrites').textContent = m.writesPerSec.toFixed(1);
  $('mLatency').textContent = m.p50 === null ? '–' : `${m.p50} / ${m.p99} ticks`;
  $('mLatency').title = `over the last ${m.latencyOps} operations; 1 tick = ${1000 / m.ticksPerSecond} ms`;
  $('mElections').textContent = String(m.elections);
  $('mLeaderless').textContent = secs(m.leaderlessTicks);
  $('mLeaderless').title = 'simulated time with no leader that can reach a majority';
  $('load').checked = snap.load;
}

function renderStatus() {
  const leader = snap.leader ? snap.nodes.find(n => n.id === snap.leader) : null;
  const who = leader ? `leader S${leader.id}, term ${leader.term}` : 'no leader yet: an election is due';
  $('status').textContent = `t = ${snap.now} · ${who} · ${plural(snap.messages.length, 'message')} in flight`;
  const split = partitioned(snap.nodes);
  $('heal').disabled = !split;
  $('split').disabled = !!splitMode;
  const busy = snap.chaos && snap.chaos.phase !== 'done';
  $('chaos').disabled = !!busy;
  $('chaos').textContent = !busy ? 'Chaos'
    : snap.chaos.phase === 'running' ? `Chaos: ${Math.max(0, Math.ceil((snap.chaos.end - snap.now) / snap.metrics.ticksPerSecond))} s`
    : 'Checking…';
  const t = sim.now();
  $('tlTime').textContent = `t = ${t} (${(t / snap.metrics.ticksPerSecond).toFixed(1)} s)` +
    (t < head() ? ` · viewing the past, recorded to ${head()}` : '');
  $('live').hidden = !(t < head());
}

// ---------------------------------------------------------------- timeline --

let tlDirty = true;
let tlFollow = true;   // keep the cursor in view while it moves
const tl = $('tl');
const tlCtx = tl.getContext('2d');
const tlScroll = $('tlScroll');

const EVENT_STYLE = {
  elect: { lane: 0, color: () => C.accent },
  crash: { lane: 0, color: () => C.ink },
  restart: { lane: 0, color: () => C.soft },
  split: { lane: 0, color: () => C.client },
  heal: { lane: 0, color: () => C.client },
  chaos: { lane: 0, color: () => C.vote },
  check: { lane: 0, color: () => C.append },
  op: { lane: 1, color: () => C.client },
};

function tlResize() {
  const r = tlScroll.getBoundingClientRect();
  const dpr = Math.min(window.devicePixelRatio || 1, 3);
  tlView = { w: r.width, h: 58, dpr };
  tl.style.width = r.width + 'px';
  tl.width = Math.round(r.width * dpr);
  tl.height = Math.round(58 * dpr);
  tlDirty = true;
}

function tlTotalWidth() { return Math.max(tlView.w, (head() + 400) * TL_SCALE); }

// Spans (split..heal, chaos) as [start, end] pairs over a list of events.
function spans(list, open, close, until) {
  const out = [];
  let start = null;
  for (const e of list) {
    if (e.kind === open && (open !== 'chaos' || e.text.startsWith('Chaos started'))) { if (start === null) start = e.t; }
    else if (start !== null && e.kind === close && (close !== 'chaos' || !e.text.startsWith('Chaos started'))) { out.push([start, e.t]); start = null; }
  }
  if (start !== null) out.push([start, until]);
  return out;
}

function drawTimeline() {
  if (!snap || tlView.w === 0) return;
  $('tlSpacer').style.width = tlTotalWidth() + 'px';
  const now = sim.now();
  if (tlFollow) {
    const x = now * TL_SCALE;
    if (x < tlScroll.scrollLeft + 20 || x > tlScroll.scrollLeft + tlView.w - 40)
      tlScroll.scrollLeft = Math.max(0, x - tlView.w * 0.75);
  }
  const off = tlScroll.scrollLeft;
  const { w, h, dpr } = tlView;
  const c = tlCtx;
  c.setTransform(dpr, 0, 0, dpr, 0, 0);
  c.clearRect(0, 0, w, h);
  const X = t => t * TL_SCALE - off;
  const tps = snap.metrics.ticksPerSecond;
  const first = Math.floor(off / TL_SCALE), last = Math.ceil((off + w) / TL_SCALE);

  // Past events are the engine's; events ahead of now come from the recording.
  const future = ghost ? ghost.events.filter(e => e.t > now) : [];
  const all = events.concat(future);
  const end = head();

  // Bands: network splits and chaos runs.
  for (const [a, b] of spans(all, 'split', 'heal', end)) {
    c.fillStyle = 'rgba(111, 168, 200, .13)';
    c.fillRect(X(a), 14, (b - a) * TL_SCALE, 18);
  }
  for (const [a, b] of spans(all, 'chaos', 'check', end)) {
    c.fillStyle = 'rgba(224, 169, 59, .16)';
    c.fillRect(X(a), 14, (b - a) * TL_SCALE, 36);
  }

  // Ruler: a mark every second, labels every 10 seconds.
  c.fillStyle = C.soft;
  c.font = '10.5px "IBM Plex Sans", system-ui, sans-serif';
  c.textBaseline = 'top';
  c.textAlign = 'left';
  for (let t = Math.floor(first / tps) * tps; t <= last; t += tps) {
    const big = t % (10 * tps) === 0;
    c.fillStyle = big ? C.soft : C.line;
    c.fillRect(Math.round(X(t)), 0, 1, big ? 6 : 3);
    if (big) c.fillText(`${t / tps} s`, X(t) + 3, 0);
  }

  // Events.
  for (const e of all) {
    if (e.t < first - 50 || e.t > last + 50) continue;
    const st = EVENT_STYLE[e.kind] ?? EVENT_STYLE.op;
    const x = X(e.t);
    c.globalAlpha = e.t > now ? 0.35 : 1;
    c.fillStyle = st.color();
    c.strokeStyle = st.color();
    if (st.lane === 1) {
      c.fillRect(Math.round(x), 40, 1, 8);
    } else if (e.kind === 'elect') {
      c.beginPath(); c.moveTo(x, 17); c.lineTo(x + 4, 25); c.lineTo(x - 4, 25); c.closePath(); c.fill();
    } else if (e.kind === 'crash') {
      c.lineWidth = 1.5;
      c.beginPath(); c.moveTo(x - 3, 18); c.lineTo(x + 3, 24); c.moveTo(x + 3, 18); c.lineTo(x - 3, 24); c.stroke();
    } else if (e.kind === 'restart') {
      c.lineWidth = 1.5;
      c.beginPath(); c.arc(x, 21, 3, 0, Math.PI * 2); c.stroke();
    } else if (e.kind === 'check') {
      c.lineWidth = 2;
      c.beginPath(); c.moveTo(x - 4, 21); c.lineTo(x - 1, 24); c.lineTo(x + 4, 17); c.stroke();
    } else {
      c.fillRect(Math.round(x), 14, 1.5, 18);
    }
  }
  c.globalAlpha = 1;

  // The recorded run ahead of the cursor is dimmed; the cursor is the accent.
  if (end > now) {
    c.fillStyle = 'rgba(251, 244, 233, .5)';
    c.fillRect(X(now), 12, (end - now) * TL_SCALE, h - 12);
  }
  const cx = Math.round(X(now)) + 0.5;
  c.strokeStyle = C.accent;
  c.lineWidth = 2;
  c.beginPath(); c.moveTo(cx, 10); c.lineTo(cx, h); c.stroke();
  c.fillStyle = C.accent;
  c.beginPath(); c.arc(cx, 10, 4, 0, Math.PI * 2); c.fill();
  tlDirty = false;
}

function tlTickAt(clientX) {
  const r = tl.getBoundingClientRect();
  return (clientX - r.left + tlScroll.scrollLeft) / TL_SCALE;
}

function tlEventNear(clientX) {
  const t = tlTickAt(clientX);
  const now = sim.now();
  const all = events.concat(ghost ? ghost.events.filter(e => e.t > now) : []);
  let best = null, bestD = 4 / TL_SCALE;
  for (const e of all) {
    const d = Math.abs(e.t - t);
    if (d < bestD && (e.kind !== 'op' || !best)) { best = e; bestD = d; }
  }
  return best;
}

// ---------------------------------------------------------------- input --

function setPlaying(p) {
  playing = p;
  $('play').textContent = p ? 'Pause' : 'Play';
}

function setSpeed(i) {
  speed = i;
  $('speed').value = String(i);
  $('speedLabel').textContent = `${SPEEDS[i]}/s`;
  $('speedLabel').title = `${SPEED_NAMES[i]}: ${SPEEDS[i]} ticks per second`;
  $('speed').setAttribute('aria-valuetext', `${SPEED_NAMES[i]}, ${SPEEDS[i]} ticks per second`);
}

function startSplit() {
  if (!snap) return;
  const ids = snap.nodes.map(n => n.id);
  const leader = snap.leader;
  // Start with the leader and one neighbour on side A: the interesting split.
  const a = new Set(leader ? [leader, ids[(ids.indexOf(leader) + 1) % ids.length]] : ids.slice(0, 2));
  splitMode = a;
  $('splitPanel').hidden = false;
  updateSplitPanel();
  renderStatus();
}

function updateSplitPanel() {
  const ids = snap.nodes.map(n => n.id);
  const a = ids.filter(i => splitMode.has(i)), b = ids.filter(i => !splitMode.has(i));
  $('sideA').textContent = a.map(i => 'S' + i).join(' ') || 'none';
  $('sideB').textContent = b.map(i => 'S' + i).join(' ') || 'none';
  $('applySplit').disabled = a.length === 0 || b.length === 0;
}

function cancelSplit() {
  splitMode = null;
  $('splitPanel').hidden = true;
  if (snap) renderStatus();
}

function applySplit() {
  const ids = snap.nodes.map(n => n.id);
  const a = ids.filter(i => splitMode.has(i)), b = ids.filter(i => !splitMode.has(i));
  cancelSplit();
  act('p' + a.join(',') + '|' + b.join(','));
}

function submitLine(line) {
  const parts = line.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return;
  const [verb, key, ...rest] = parts;
  const op = { put: 'put', set: 'put', get: 'get', delete: 'delete', del: 'delete', rm: 'delete' }[verb.toLowerCase()];
  const value = rest.join(' ');
  let msg = null;
  if (!op) msg = 'Unknown command. Use put, get or delete.';
  else if (!key) msg = `Usage: ${op}${op === 'put' ? ' <key> <value>' : ' <key>'}`;
  else if (op === 'put' && !value) msg = 'Usage: put <key> <value>';
  else if (op !== 'put' && value) msg = `Usage: ${op} <key>`;
  else if (key.length > 64 || value.length > 64 || key.includes(':')) msg = 'Keys and values: up to 64 characters, no ":" in keys.';
  if (msg) {
    localErrors.push({ at: snap.now, line, msg });
    lastOpsSig = '';
    renderConsole();
    return;
  }
  act('s' + [op, key, value].map(encodeURIComponent).join(':'));
  if (!playing) setPlaying(true);
}

function bind() {
  $('play').onclick = () => setPlaying(!playing);
  $('step').onclick = () => { setPlaying(false); advance(1); refresh(); };
  $('speed').oninput = e => setSpeed(Number(e.target.value));
  $('split').onclick = startSplit;
  $('cancelSplit').onclick = cancelSplit;
  $('applySplit').onclick = applySplit;
  $('heal').onclick = () => act('h');
  $('reset').onclick = () => {
    const v = Number.parseInt($('seed').value, 10);
    tlFollow = true;
    reset(Number.isFinite(v) && v >= 0 && v < 1e9 ? v : randomSeed());
  };
  $('seed').onkeydown = e => { if (e.key === 'Enter') $('reset').click(); };
  $('share').onclick = async () => {
    writeHash();
    try {
      await navigator.clipboard.writeText(location.href);
      $('share').textContent = 'Copied';
    } catch {
      $('share').textContent = 'Link in address bar';
    }
    setTimeout(() => { $('share').textContent = 'Copy link'; }, 1600);
  };
  $('cmd').onsubmit = e => {
    e.preventDefault();
    submitLine($('line').value);
    $('line').value = '';
  };
  $('start').onclick = () => { $('intro').hidden = true; setPlaying(true); };
  $('load').onchange = e => act(e.target.checked ? 'l1' : 'l0');
  $('chaos').onclick = () => {
    act('x');
    if (speed < 3) setSpeed(3);  // 60 simulated seconds take a minute at "fast"
    setPlaying(true);
  };
  $('live').onclick = () => { tlFollow = true; scrubTo(head()); setPlaying(true); };

  // Timeline: drag to scrub, arrow keys to step, scroll to look around.
  let dragging = false, want = null;
  const scrubSoon = t => {
    if (want === null) requestAnimationFrame(() => { scrubTo(want); want = null; });
    want = t;
  };
  tl.addEventListener('pointerdown', e => {
    dragging = true;
    tlFollow = true;
    tl.setPointerCapture(e.pointerId);
    scrubSoon(tlTickAt(e.clientX));
  });
  tl.addEventListener('pointermove', e => {
    if (dragging) { scrubSoon(tlTickAt(e.clientX)); return; }
    const ev = tlEventNear(e.clientX);
    tl.title = ev ? `t=${ev.t}: ${ev.text}` : 'Drag to go back to any moment';
  });
  const stop = () => { dragging = false; };
  tl.addEventListener('pointerup', stop);
  tl.addEventListener('pointercancel', stop);
  tlScroll.addEventListener('scroll', () => { tlDirty = true; if (!dragging) tlFollow = false; });
  tlScroll.addEventListener('keydown', e => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    e.preventDefault();
    tlFollow = true;
    const step = (e.shiftKey ? 100 : 1) * (e.key === 'ArrowLeft' ? -1 : 1);
    scrubTo(sim.now() + step);
  });
  new ResizeObserver(tlResize).observe(tlScroll);

  canvas.addEventListener('click', e => {
    const r = canvas.getBoundingClientRect();
    const n = hit(e.clientX - r.left, e.clientY - r.top);
    if (!n) return;
    if (splitMode) {
      if (splitMode.has(n.id)) splitMode.delete(n.id); else splitMode.add(n.id);
      updateSplitPanel();
      return;
    }
    act((n.up ? 'c' : 'r') + n.id);
  });
  canvas.addEventListener('mousemove', e => {
    const r = canvas.getBoundingClientRect();
    const n = hit(e.clientX - r.left, e.clientY - r.top);
    canvas.style.cursor = n ? 'pointer' : 'default';
    canvas.title = !n ? '' : splitMode ? `Move S${n.id} to the other side` : n.up ? `Crash S${n.id}` : `Restart S${n.id}`;
  });
  document.addEventListener('keydown', e => {
    if (e.target instanceof HTMLInputElement || !$('intro').hidden) return;
    if (e.key === ' ') { e.preventDefault(); setPlaying(!playing); }
    if (e.key === '.') $('step').click();
  });
  new ResizeObserver(resize).observe(canvas.parentElement);
}

function randomSeed() { return Math.floor(Math.random() * 1e6); }

// ---------------------------------------------------------------- loop --

let last = performance.now();
function frame(t) {
  const dt = Math.min(0.1, (t - last) / 1000);
  last = t;
  if (sim && playing) {
    acc += dt * SPEEDS[speed];
    const n = Math.min(MAX_TICKS_PER_FRAME, Math.floor(acc));
    acc -= Math.floor(acc);
    if (n > 0) { advance(n); refresh(); }
  }
  draw(dt);
  if (tlDirty) drawTimeline();
  requestAnimationFrame(frame);
}

async function main() {
  bind();
  setSpeed(speed);
  resize();
  try {
    Q = await createQuorum();
  } catch (err) {
    $('loadError').hidden = false;
    $('loadError').textContent = `The engine failed to load: ${err}`;
    return;
  }
  const { seed, run } = parseHash();
  reset(seed ?? randomSeed(), run);
  // Hooks for scripts/web_smoke.mjs, which drives the page through real clicks
  // and typing: the latest snapshot, where a server is drawn, and runTo(t),
  // which pauses and runs the same advance() as playback up to tick t (to
  // check that a shared link replays a run exactly).
  window.quorum = {
    snapshot: () => snap,
    events: () => events,
    head,
    // Where tick t is drawn on the timeline, in page coordinates (null if
    // scrolled out of view).
    timelineX: t => {
      const r = tl.getBoundingClientRect();
      const x = r.left + t * TL_SCALE - tlScroll.scrollLeft;
      return x < r.left || x > r.right ? null : { x, y: r.top + r.height / 2 };
    },
    runTo: t => {
      setPlaying(false);
      if (t > sim.now()) advance(t - sim.now());
      refresh();
      return snap.now;
    },
    position: id => {
      const r = canvas.getBoundingClientRect();
      const p = nodePos(id, geometry());
      return { x: r.left + p.x, y: r.top + p.y };
    },
  };
  requestAnimationFrame(frame);
}

main();
