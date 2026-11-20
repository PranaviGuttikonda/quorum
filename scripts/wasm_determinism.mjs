// Given: the WASM build must behave exactly like native for the same seed.
// Approach: load the web build's module in Node and print scenarioTrace for
//           each seed, in the same format as the native quorum_scenario tool,
//           so CI can diff the two. It also replays the scenario through the
//           JavaScript API (create, tick, crash, ...), the path the page uses,
//           and exits 1 if that run's final snapshot differs from the trace.
//
//   node scripts/wasm_determinism.mjs build-web/site/quorum.js 1 2 3

import path from 'node:path';
import { pathToFileURL } from 'node:url';

const [modulePath, ...seedArgs] = process.argv.slice(2);
if (!modulePath) {
  console.error('usage: node scripts/wasm_determinism.mjs <quorum.js> [seeds...]');
  process.exit(2);
}
const seeds = seedArgs.length ? seedArgs.map(Number) : [1, 2, 3];
const { default: createQuorum } = await import(pathToFileURL(path.resolve(modulePath)).href);
const q = await createQuorum();

// Mirrors web::scenario_trace in src/web/simulation.cpp.
function replay(seed) {
  const s = q.create(seed, 5);
  const leader = () => JSON.parse(s.snapshot()).leader;
  s.tick(400);
  const first = leader() ?? 1;
  s.crash(first);
  s.tick(500);
  s.submit(1, 'put', 'a', '1');
  s.submit(2, 'put', 'b', '2');
  s.tick(300);
  s.submit(1, 'get', 'a', '');
  s.submit(2, 'delete', 'b', '');
  s.tick(300);
  const second = leader() ?? 2;
  s.partition(`${second}|${first}`);
  s.submit(1, 'put', 'a', '3');
  s.tick(700);
  s.heal();
  s.restart(first);
  s.tick(700);
  for (let i = 0; i < 5; ++i) s.tick(37);
  s.setLoad(true);
  s.tick(500);
  s.startChaos();
  s.tick(6000 + 1000);
  s.setLoad(false);
  s.tick(600);
  const snap = s.snapshot();
  s.delete();
  return snap;
}

let ok = true;
for (const seed of seeds) {
  const trace = q.scenarioTrace(seed);
  process.stdout.write(`== seed ${seed}\n${trace}`);
  const lines = trace.trimEnd().split('\n');
  if (replay(seed) !== lines[lines.length - 1]) {
    console.error(`seed ${seed}: the JavaScript API run differs from scenarioTrace`);
    ok = false;
  }
}
process.exit(ok ? 0 : 1);
