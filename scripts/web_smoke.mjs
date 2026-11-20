// Given: the built site (build-web/site) and a local Chrome.
// Approach: serve the site, open it in headless Chrome over the DevTools
//           protocol, and use it the way a person would, with real mouse
//           clicks and typing, at 1280x800 and 390x844:
//             start, wait for a leader, crash it by clicking it, wait for a
//             new leader, put and get through the console, split the network
//             and heal it.
//           Each step waits on what the engine reports (window.quorum
//           snapshot). Screenshots go to the output directory. The script
//           fails on any page error, and prints the bytes transferred.
//
//   node scripts/web_smoke.mjs build-web/site docs/screenshots
//   CHROME=/path/to/chrome node scripts/web_smoke.mjs ...

import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';

const [siteDir, outDir = 'screenshots'] = process.argv.slice(2);
if (!siteDir) {
  console.error('usage: node scripts/web_smoke.mjs <site dir> [screenshot dir]');
  process.exit(2);
}
mkdirSync(outDir, { recursive: true });

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css',
  '.wasm': 'application/wasm', '.woff2': 'font/woff2', '.txt': 'text/plain',
};
const server = createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  const rel = url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname.slice(1));
  const root = path.resolve(siteDir);
  const file = path.join(root, path.normalize('/' + rel));  // can't climb out of root
  if (!file.startsWith(root + path.sep)) {
    res.writeHead(403).end();
    return;
  }
  if (!existsSync(file)) { res.writeHead(404).end(); return; }
  res.writeHead(200, { 'content-type': TYPES[path.extname(file)] ?? 'application/octet-stream' });
  res.end(readFileSync(file));
});
await new Promise(r => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/`;

const chromePath = process.env.CHROME ?? (process.platform === 'darwin'
  ? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
  : 'google-chrome');
const profile = mkdtempSync(path.join(os.tmpdir(), 'quorum-chrome-'));
const chrome = spawn(chromePath, [
  '--headless=new', '--remote-debugging-port=0', `--user-data-dir=${profile}`,
  '--no-first-run', '--no-default-browser-check', '--hide-scrollbars', 'about:blank',
], { stdio: ['ignore', 'ignore', 'pipe'] });
const wsUrl = await new Promise((resolve, reject) => {
  let buf = '';
  chrome.stderr.on('data', d => {
    buf += d;
    const m = buf.match(/DevTools listening on (ws:\/\/\S+)/);
    if (m) resolve(m[1]);
  });
  chrome.on('exit', c => reject(new Error(`chrome exited with ${c}`)));
  setTimeout(() => reject(new Error('chrome did not start')), 20000);
});

// Minimal CDP client over the browser socket, with one page session.
const ws = new WebSocket(wsUrl);
await new Promise(r => ws.addEventListener('open', r, { once: true }));
let nextId = 1;
const waiting = new Map();
const listeners = [];
ws.addEventListener('message', ev => {
  const msg = JSON.parse(ev.data);
  if (msg.id && waiting.has(msg.id)) {
    const { resolve, reject } = waiting.get(msg.id);
    waiting.delete(msg.id);
    msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
  } else if (msg.method) {
    for (const l of listeners) l(msg);
  }
});
function send(method, params = {}, sessionId) {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params, sessionId }));
  return new Promise((resolve, reject) => waiting.set(id, { resolve, reject }));
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const failures = [];

async function run(name, width, height, mobile) {
  const { targetId } = await send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true });
  const S = (m, p) => send(m, p, sessionId);
  const errors = [];
  let bytes = 0;
  const files = new Map();
  const urls = new Map();
  listeners.push(msg => {
    if (msg.sessionId !== sessionId) return;
    if (msg.method === 'Runtime.exceptionThrown') errors.push(msg.params.exceptionDetails.text + ' ' + (msg.params.exceptionDetails.exception?.description ?? ''));
    if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error')
      errors.push(msg.params.args.map(a => a.value ?? a.description).join(' '));
    if (msg.method === 'Network.responseReceived') urls.set(msg.params.requestId, msg.params.response.url);
    if (msg.method === 'Network.loadingFinished') {
      bytes += msg.params.encodedDataLength;
      const u = urls.get(msg.params.requestId);
      if (u) files.set(u.replace(base, '') || 'index.html', msg.params.encodedDataLength);
    }
  });
  await S('Runtime.enable');
  await S('Network.enable');
  await S('Network.setCacheDisabled', { cacheDisabled: true });
  await S('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 2, mobile });
  await S('Page.enable');
  await S('Page.navigate', { url: base + '#seed=7' });

  const evaluate = async expr => {
    const r = await S('Runtime.evaluate', { expression: expr, returnByValue: true, awaitPromise: true });
    if (r.exceptionDetails) throw new Error(`${expr}: ${r.exceptionDetails.text}`);
    return r.result.value;
  };
  const until = async (what, expr, ms = 30000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (await evaluate(expr)) return;
      await sleep(100);
    }
    throw new Error(`${name}: timed out waiting for ${what}`);
  };
  const click = async (x, y) => {
    await S('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
    await S('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
    await S('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
  };
  const clickEl = async id => {
    await evaluate(`document.getElementById('${id}').scrollIntoView({block: 'center'})`);
    const r = await evaluate(`(() => { const b = document.getElementById('${id}').getBoundingClientRect(); return {x: b.x + b.width / 2, y: b.y + b.height / 2}; })()`);
    await click(r.x, r.y);
  };
  const clickServer = async id => {
    await evaluate(`document.getElementById('ring').scrollIntoView({block: 'center'})`);
    await sleep(150);
    const p = await evaluate(`window.quorum.position(${id})`);
    await click(p.x, p.y);
  };
  const type = async text => {
    await clickEl('line');
    for (const ch of text) await S('Input.dispatchKeyEvent', { type: 'char', text: ch });
    await S('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13, text: '\r' });
    await S('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 13 });
  };
  const shot = async label => {
    await sleep(250);
    const full = mobile;
    const { data } = await S('Page.captureScreenshot', { format: 'png', captureBeyondViewport: full });
    const file = path.join(outDir, `${name}-${label}.png`);
    writeFileSync(file, Buffer.from(data, 'base64'));
    console.log(`  ${file}`);
  };
  const snap = expr => `(() => { const s = window.quorum && window.quorum.snapshot(); return !!s && (${expr}); })()`;

  console.log(`${name} ${width}x${height}`);
  await until('the engine', `!!(window.quorum && window.quorum.snapshot())`);
  await until('the font', `document.fonts.check('15px "IBM Plex Sans"')`);
  await shot('1-intro');
  await clickEl('start');
  await until('a leader', snap('s.leader !== null'));
  const first = await evaluate(snap('s.leader'));
  // Let one heartbeat round show on the wire.
  await until('messages in flight', snap('s.messages.length > 0'));
  await shot('2-leader');

  await clickServer(first);
  await until(`S${first} to crash`, snap(`!s.nodes[${first - 1}].up`), 5000);
  await until('a new leader', snap(`s.leader !== null && s.leader !== ${first}`));
  const second = await evaluate(snap('s.leader'));
  console.log(`  crashed S${first}, new leader S${second}`);
  await shot('3-new-leader');

  await type('put x 1');
  await until('the put to finish', snap(`s.ops.some(o => o.op === 'put' && o.status === 'done')`));
  await type('get x');
  await until('the get to return "1"', snap(`s.ops.some(o => o.op === 'get' && o.status === 'done' && o.result === '1')`));
  const get = await evaluate(snap(`s.ops.find(o => o.op === 'get')`));
  console.log(`  get x -> "${get.result}" from S${get.server} in ${get.completed - get.invoked} ticks`);
  await shot('4-console');

  await clickEl('split');
  await until('the split panel', `!document.getElementById('splitPanel').hidden`, 3000);
  await clickEl('applySplit');
  await until('the partition', snap('s.nodes.some(n => n.group !== 0)'), 3000);
  await sleep(1500);
  await shot('5-split');
  await clickEl('heal');
  await until('the heal', snap('s.nodes.every(n => n.group === 0)'), 3000);
  await clickServer(first);  // restart the crashed server
  await until(`S${first} to restart`, snap(`s.nodes[${first - 1}].up`), 5000);
  await until('a stable leader after healing', snap('s.leader !== null'));
  await sleep(2500);
  await shot('6-healed');

  // The copied link must replay this run exactly: same seed, same actions at
  // the same ticks, so the same snapshot at the same tick.
  if (!mobile) {
    await clickEl('play');  // pause
    const t = await evaluate(snap('s.now'));
    const here = await evaluate(`JSON.stringify(window.quorum.snapshot())`);
    const link = await evaluate('location.href');
    const replay = await send('Target.createTarget', { url: 'about:blank' });
    const rs = (await send('Target.attachToTarget', { targetId: replay.targetId, flatten: true })).sessionId;
    await send('Runtime.enable', {}, rs);
    await send('Page.navigate', { url: link }, rs);
    const revaluate = async expr => (await send('Runtime.evaluate', { expression: expr, returnByValue: true }, rs)).result.value;
    const end = Date.now() + 15000;
    while (!(await revaluate('!!(window.quorum && window.quorum.snapshot())')) && Date.now() < end) await sleep(100);
    const reached = await revaluate(`window.quorum.runTo(${t})`);
    const there = await revaluate(`JSON.stringify(window.quorum.snapshot())`);
    const actions = JSON.parse(new URLSearchParams(new URL(link).hash.slice(1)).get('run') ?? '[]').length;
    if (reached !== t || there !== here) errors.push(`the shared link did not replay the run (tick ${reached} vs ${t})`);
    else console.log(`  shared link replays exactly: ${actions} actions, identical snapshot at t=${t}`);
    await send('Target.closeTarget', { targetId: replay.targetId });
  }

  const overflow = await evaluate(`document.documentElement.scrollWidth > window.innerWidth`);
  if (overflow) errors.push('horizontal overflow');
  if (!mobile) {
    const fits = await evaluate(`document.documentElement.scrollHeight <= window.innerHeight + 1`);
    if (!fits) errors.push('page does not fit the window');
  }
  console.log(`  transferred ${bytes} bytes:`);
  for (const [f, b] of [...files].sort()) console.log(`    ${f.padEnd(40)} ${b}`);
  if (errors.length) failures.push(`${name}: ${errors.join('; ')}`);
  await send('Target.closeTarget', { targetId });
  return bytes;
}

let code = 0;
try {
  await run('desktop', 1280, 800, false);
  await run('phone', 390, 844, true);
} catch (e) {
  failures.push(String(e.stack ?? e));
}
if (failures.length) {
  console.error('FAILED\n' + failures.join('\n'));
  code = 1;
} else {
  console.log('OK');
}
ws.close();
const exited = new Promise(r => chrome.once('exit', r));
chrome.kill();
await Promise.race([exited, sleep(5000)]);
server.close();
try { rmSync(profile, { recursive: true, force: true, maxRetries: 5 }); } catch { /* temp dir; harmless */ }
process.exit(code);
