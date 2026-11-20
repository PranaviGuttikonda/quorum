// Given: quorum.js / quorum.wasm, the real engine behind web::Simulation.
// Approach: the page only asks the engine to act (tick, crash, restart,
//           partition, heal, submit) and draws what snapshot() reports. No
//           Raft logic lives here. Between ticks, positions are interpolated so
//           messages glide.
//   - Every action is recorded with the tick it happened at. The URL holds
//     the seed and that list, so a copied link replays the same run exactly.
//   - The canvas is sized in device pixels, so it stays crisp on HiDPI.

import createQuorum from './quorum.js';

const SERVERS = 5;
const SPEEDS = [5, 15, 40, 100, 300];          // ticks per second
const SPEED_NAMES = ['very slow', 'slow', 'normal', 'fast', 'very fast'];
const MAX_TICKS_PER_FRAME = 400;
const LOG_ROWS = 7;
const DROP_MEMORY = 12;                        // matches Simulation::kDropMemory

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
let actions = [];      // [tick, code], in order
let pending = [];      // actions still to replay, from a shared link
let splitMode = null;  // Set of server ids on side A while choosing
const angle = new Map();       // server id -> current drawn angle
const lastCommitted = new Map(); // server id -> highest committed index drawn
const lastLogSig = new Map();
let lastOpsSig = '';
let localErrors = [];  // console lines the engine never saw (parse errors)
let view = { w: 0, h: 0, dpr: 1 };

// ---------------------------------------------------------------- engine --

function parseHash() {
  const h = new URLSearchParams(location.hash.slice(1));
  const seed = Number.parseInt(h.get('seed') ?? '', 10);
  let run = [];
  try { run = JSON.parse(h.get('run') ?? '[]'); } catch { run = []; }
  if (!Array.isArray(run)) run = [];
  return {
    seed: Number.isFinite(seed) && seed >= 0 && seed < 1e9 ? seed : null,
    run: run.filter(a => Array.isArray(a) && a.length === 2 && Number.isInteger(a[0]) && typeof a[1] === 'string'),
  };
}

function writeHash() {
  const p = new URLSearchParams({ seed: String(sim.seed()) });
  if (actions.length) p.set('run', JSON.stringify(actions));
  history.replaceState(null, '', '#' + p.toString());
}

function reset(seed, replay = []) {
  if (sim) sim.delete();
  sim = Q.create(seed, SERVERS);
  actions = [];
  pending = replay.slice().sort((a, b) => a[0] - b[0]);
  angle.clear(); lastCommitted.clear(); lastLogSig.clear(); lastOpsSig = ''; localErrors = [];
  acc = 0;
  cancelSplit();
  $('seed').value = String(seed);
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
  if (kind === 's') {
    const [op, key, value] = arg.split(':').map(decodeURIComponent);
    return sim.submit(1, op, key, value ?? '') > 0;
  }
  return false;
}

// A user action: drops whatever was left to replay, then records it.
function act(code) {
  pending = [];
  if (perform(code)) {
    actions.push([sim.now(), code]);
    writeHash();
  }
  refresh();
}

function applyDue() {
  while (pending.length && pending[0][0] <= sim.now()) {
    const [, code] = pending.shift();
    if (perform(code)) actions.push([sim.now(), code]);
  }
}

// Advances n ticks, stopping at every replayed action's tick.
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

function refresh() {
  snap = JSON.parse(sim.snapshot());
  renderLogs();
  renderConsole();
  renderStatus();
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
  const sig = JSON.stringify([ops, localErrors]);
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
  items.sort((a, b) => a.at - b.at);
  out.innerHTML = items.length
    ? items.map(i => i.html).join('')
    : '<li class="empty">Try <b>put x 1</b>, then <b>get x</b>. Commands go through a client that finds the leader, follows redirects and retries on timeouts.</li>';
  out.scrollTop = out.scrollHeight;
}

function renderStatus() {
  const leader = snap.leader ? snap.nodes.find(n => n.id === snap.leader) : null;
  const who = leader ? `leader S${leader.id}, term ${leader.term}` : 'no leader yet: an election is due';
  $('status').textContent = `t = ${snap.now} · ${who} · ${plural(snap.messages.length, 'message')} in flight`;
  const split = partitioned(snap.nodes);
  $('heal').disabled = !split;
  $('split').disabled = !!splitMode;
}

// ---------------------------------------------------------------- input --

function setPlaying(p) {
  playing = p;
  $('play').textContent = p ? 'Pause' : 'Play';
}

function setSpeed(i) {
  speed = i;
  $('speed').value = String(i);
  $('speedLabel').textContent = `${SPEED_NAMES[i]}, ${SPEEDS[i]}/s`;
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
