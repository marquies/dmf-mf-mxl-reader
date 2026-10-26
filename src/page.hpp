// The ring view the reader serves at /: one self-contained page that draws
// each flow's ring from the /stream events and its counters from /status.

#pragma once

namespace
{
    constexpr char const* RingPage = R"PAGE(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>mxl-reader ring</title>
<style>
  :root {
    --bg: #002f2d; --panel: #004643; --ink: #ffffff; --dim: rgba(255,255,255,0.6);
    --ok: #c8f169; --warn: #ffd966; --bad: #f05012; --miss: #b48cff; --commit: #6fd3ff;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
         font: 14px/1.4 system-ui, -apple-system, 'Segoe UI', sans-serif; }
  header { display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap; padding: 14px 20px; }
  header h1 { font-size: 16px; letter-spacing: 0.06em; text-transform: uppercase; margin: 0; }
  header .version { color: var(--dim); font-size: 12px; }
  header .conn { margin-left: auto; font-size: 12px; }
  .tabs { display: flex; gap: 8px; flex-wrap: wrap; padding: 0 20px 12px; }
  .tab { background: rgba(255,255,255,0.08); color: var(--ink); border: 0; border-radius: 999px;
         padding: 6px 14px; cursor: pointer; font: inherit; font-size: 13px; }
  .tab.on { background: var(--ok); color: #0b090d; font-weight: 700; }
  main { display: grid; grid-template-columns: minmax(300px, 460px) minmax(0, 1fr); gap: 16px; padding: 0 20px 20px; }
  @media (max-width: 900px) { main { grid-template-columns: minmax(0, 1fr); } }
  .panel { background: var(--panel); border-radius: 18px; padding: 16px; }
  .ring canvas { width: 100%; aspect-ratio: 1; display: block; }
  .side { display: flex; flex-direction: column; gap: 16px; min-width: 0; }
  .numbers { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }
  .num { background: rgba(255,255,255,0.06); border-radius: 12px; padding: 8px 12px; }
  .num .k { font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--dim); }
  .num .v { font-size: 22px; font-weight: 800; font-variant-numeric: tabular-nums; }
  .num .v small { font-size: 12px; font-weight: 600; color: var(--dim); }
  .ringinfo { margin-top: 12px; grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .ringinfo .num .v { font-size: 17px; }
  .strip canvas { width: 100%; height: 240px; display: block; }
  .legend { display: flex; flex-wrap: wrap; gap: 12px; font-size: 12px; color: var(--dim); margin-top: 8px; }
  .legend i { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 5px; vertical-align: -1px; }
  .chips { display: flex; flex-wrap: wrap; gap: 6px; }
  .chip { font-size: 12px; padding: 2px 10px; border-radius: 999px; background: rgba(255,255,255,0.08); }
  .chip.ok { color: var(--ok); } .chip.warn { color: var(--warn); } .chip.bad { color: #ff9a73; }
  .chip.zero { opacity: 0.4; }
  .err { color: #ff9a73; font-size: 13px; margin-top: 8px; }
  .flowid { font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace; color: var(--dim);
            margin: -4px 0 10px; overflow-wrap: anywhere; user-select: all; }
  h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; margin: 0 0 10px; color: var(--dim); }
</style>
</head>
<body>
<header>
  <h1>Reader ring</h1>
  <span class="version" id="version"></span>
  <span class="conn" id="conn">connecting</span>
</header>
<div class="tabs" id="tabs"></div>
<main>
  <section class="panel ring">
    <h2 id="ringTitle">Ring</h2>
    <div class="flowid" id="flowId"></div>
    <canvas id="ring" width="900" height="900"></canvas>
    <div class="numbers ringinfo" id="ringInfo"></div>
    <div class="legend">
      <span><i style="background:var(--ok)"></i>within a period</span>
      <span><i style="background:var(--warn)"></i>within the grace</span>
      <span><i style="background:var(--bad)"></i>past the grace</span>
      <span><i style="background:var(--miss)"></i>missing</span>
      <span><i style="background:#ff9a73"></i>invalid, incomplete, wrong</span>
      <span>&#9650; head &nbsp; &#9661; reader</span>
    </div>
  </section>
  <section class="side">
    <div class="panel">
      <h2>Last second</h2>
      <div class="numbers" id="numbers"></div>
    </div>
    <div class="panel strip">
      <h2>Arrival per grain, last 6 s, in ms after its media timestamp (log scale)</h2>
      <canvas id="strip" width="1600" height="480"></canvas>
      <div class="legend">
        <span><i style="background:var(--ok)"></i>read by the reader</span>
        <span><i style="background:var(--commit)"></i>committed on this node</span>
        <span><i style="background:var(--warn)"></i>too early</span>
        <span><i style="background:var(--bad)"></i>too late</span>
        <span><i style="background:#ff9a73"></i>stall</span>
      </div>
    </div>
    <div class="panel">
      <h2>Counters since the reader started</h2>
      <div class="chips" id="chips"></div>
      <div class="err" id="err"></div>
    </div>
  </section>
</main>
<script>
"use strict";
const WINDOW_MS = 6000;
const flows = new Map();      // id -> { meta, events: [], slots: Map(segment -> {arr, kind, seen}) }
let selected = null;
let skew = 0;                 // server clock minus client clock, in ms
let lastBatchAt = 0;

const colorFor = (ev, meta) => {
  if (ev.kind === 'm') return '#b48cff';
  if ('vpsxf'.includes(ev.kind)) return '#ff9a73';
  if (ev.arr <= meta.periodMs + 2) return '#c8f169';
  if (ev.arr <= meta.graceMs) return '#ffd966';
  return '#f05012';
};

function segments(meta) { return Math.max(1, Math.min(meta.ring || 1, 360)); }
function segmentOf(index, meta) {
  const ring = meta.ring || 1;
  // Multiply before dividing: (k / ring) * slots rounds just below a whole
  // number for some k, which leaves a slot that never receives a grain.
  return Math.floor((index % ring) * segments(meta) / ring);
}

function onBatch(batch) {
  skew = batch.now - Date.now();
  lastBatchAt = performance.now();
  for (const f of batch.flows) {
    let flow = flows.get(f.id);
    if (!flow) {
      flow = { meta: f, events: [], slots: new Map() };
      flows.set(f.id, flow);
      if (!selected) selected = f.id;
      renderTabs();
    }
    flow.meta = f;
    flow.batchAt = batch.now;
    for (const [index, seen, arr, commit, behind, kind, count] of f.ev) {
      const ev = { index, seen, arr, commit, behind, kind, count };
      flow.events.push(ev);
      if ('kfmvpsx'.includes(kind)) flow.slots.set(segmentOf(index, f), ev);
    }
    const cutoff = batch.now - WINDOW_MS - 1000;
    while (flow.events.length && flow.events[0].seen < cutoff) flow.events.shift();
  }
}

function renderTabs() {
  const tabs = document.getElementById('tabs');
  tabs.innerHTML = '';
  for (const [id, flow] of flows) {
    const b = document.createElement('button');
    b.className = 'tab' + (id === selected ? ' on' : '');
    b.textContent = (flow.meta.label ? flow.meta.label + '  ' : '') + id.slice(0, 8);
    b.onclick = () => { selected = id; renderTabs(); };
    tabs.appendChild(b);
  }
}

function fmtBytes(n) {
  if (!n) return ['--', ''];
  if (n >= 1e9) return [(n / 1e9).toFixed(2), 'GB'];
  if (n >= 1e6) return [(n / 1e6).toFixed(n >= 1e8 ? 0 : 1), 'MB'];
  if (n >= 1e3) return [(n / 1e3).toFixed(1), 'KB'];
  return [String(n), 'B'];
}

// The ring itself: how many slots, the payload of one, all of them, and how
// much media time that holds.
function drawRingInfo(meta) {
  const ring = meta.ring || 0;
  const slot = fmtBytes(meta.slotBytes);
  const total = fmtBytes(ring * (meta.slotBytes || 0));
  const span = ring && meta.periodMs ? ring * meta.periodMs / 1000 : null;
  const cells = [
    ['slots', ring || '--', meta.unit === 'samples' ? 'samples' : 'grains'],
    ['slot size', slot[0], slot[1]],
    ['ring total', total[0], total[1]],
    ['history', span === null ? '--' : span.toFixed(2), 's'],
  ];
  document.getElementById('ringInfo').innerHTML = cells
    .map(([k, v, u]) => `<div class="num"><div class="k">${k}</div><div class="v">${v} <small>${u}</small></div></div>`)
    .join('');
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
}

function fmtMs(v) { return v === null || v === undefined ? '--' : v.toFixed(1); }

function drawRing(flow, now) {
  const c = document.getElementById('ring');
  const g = c.getContext('2d');
  const W = c.width, cx = W / 2, cy = W / 2, R = W * 0.42, thick = W * 0.09;
  g.clearRect(0, 0, W, W);
  const meta = flow.meta;
  const n = segments(meta);
  const ringMs = (meta.ring || 1) * meta.periodMs;
  const step = (Math.PI * 2) / n;
  const gap = n > 120 ? 0 : step * 0.08;
  for (let s = 0; s < n; s++) {
    const ev = flow.slots.get(s);
    const a0 = -Math.PI / 2 + s * step + gap / 2, a1 = a0 + step - gap;
    g.beginPath();
    g.arc(cx, cy, R, a0, a1);
    g.arc(cx, cy, R - thick, a1, a0, true);
    g.closePath();
    if (ev) {
      const age = now - ev.seen;
      g.globalAlpha = Math.max(0.18, 1 - age / Math.max(ringMs, 200) * 0.8);
      g.fillStyle = colorFor(ev, meta);
    } else {
      g.globalAlpha = 1;
      g.fillStyle = 'rgba(255,255,255,0.06)';
    }
    g.fill();
  }
  g.globalAlpha = 1;
  // Head and reader move between batches at one grain per period.
  const drift = meta.periodMs > 0 && meta.unit === 'grains' && meta.state === 'open'
    ? Math.min(Math.floor((performance.now() - lastBatchAt) / meta.periodMs), 3) : 0;
  const angleOf = (index) => -Math.PI / 2 + ((index % (meta.ring || 1)) / (meta.ring || 1)) * Math.PI * 2 + step / 2;
  const marker = (index, outside, color) => {
    const a = angleOf(index);
    const r = outside ? R + W * 0.025 : R - thick - W * 0.025;
    const tip = outside ? R + 2 : R - thick - 2;
    g.beginPath();
    g.moveTo(cx + Math.cos(a) * tip, cy + Math.sin(a) * tip);
    g.lineTo(cx + Math.cos(a - 0.03) * r, cy + Math.sin(a - 0.03) * r);
    g.lineTo(cx + Math.cos(a + 0.03) * r, cy + Math.sin(a + 0.03) * r);
    g.closePath();
    g.fillStyle = color;
    g.fill();
  };
  if (meta.head) marker(meta.head + drift, true, '#ffffff');
  if (meta.reader) marker(meta.reader + drift, false, '#c8f169');
  // Centre: the newest grain's arrival, large.
  const last = [...flow.events].reverse().find((e) => 'kfmvpsx'.includes(e.kind));
  g.fillStyle = '#ffffff';
  g.textAlign = 'center';
  g.font = `800 ${W * 0.11}px system-ui, sans-serif`;
  g.fillText(last ? last.arr.toFixed(1) : '--', cx, cy + W * 0.02);
  g.font = `600 ${W * 0.035}px system-ui, sans-serif`;
  g.fillStyle = 'rgba(255,255,255,0.6)';
  g.fillText('ms after media time', cx, cy + W * 0.075);
  g.fillText(`${meta.ring || '?'} ${meta.unit === 'samples' ? 'samples' : 'slots'}, ${meta.state}`, cx, cy - W * 0.1);
}

function drawStrip(flow, now) {
  const c = document.getElementById('strip');
  const g = c.getContext('2d');
  const W = c.width, H = c.height, pad = 40;
  g.clearRect(0, 0, W, H);
  const meta = flow.meta;
  const from = now - WINDOW_MS;
  const visible = flow.events.filter((e) => e.seen >= from);
  // Logarithmic, 0.1 ms to 1 s: a grain 1 ms late and one 80 ms late both
  // read at a glance on one axis.
  const LO = 0.1, HI = 1000;
  const x = (t) => pad + ((t - from) / WINDOW_MS) * (W - pad - 10);
  const y = (v) => {
    const c = Math.min(HI, Math.max(LO, v));
    return H - pad - (Math.log10(c / LO) / Math.log10(HI / LO)) * (H - pad - 12);
  };
  // Thresholds: one period and the too-early grace.
  g.font = '20px system-ui, sans-serif';
  for (const [v, label, color] of [[meta.periodMs, 'period', 'rgba(200,241,105,0.5)'], [meta.graceMs, 'grace', 'rgba(240,80,18,0.6)']]) {
    if (!v) continue;
    g.strokeStyle = color; g.setLineDash([8, 8]); g.beginPath();
    g.moveTo(pad, y(v)); g.lineTo(W - 10, y(v)); g.stroke(); g.setLineDash([]);
    g.fillStyle = color; g.textAlign = 'left'; g.fillText(`${label} ${v.toFixed(1)} ms`, pad + 6, y(v) - 6);
  }
  // Axis: a gridline per decade.
  g.textAlign = 'right';
  for (const v of [0.1, 1, 10, 100, 1000]) {
    g.strokeStyle = 'rgba(255,255,255,0.08)'; g.beginPath(); g.moveTo(pad, y(v)); g.lineTo(W - 10, y(v)); g.stroke();
    g.fillStyle = 'rgba(255,255,255,0.5)'; g.fillText(v >= 1 ? v.toFixed(0) : v.toString(), pad - 6, y(v) + 6);
  }
  g.textAlign = 'center';
  for (let s = 0; s <= 6; s++) g.fillText(s === 6 ? 'now' : `-${6 - s}s`, x(from + s * 1000), H - 10);
  // Events.
  const barW = Math.max(1.5, (W - pad) / Math.max(1, (WINDOW_MS / Math.max(meta.periodMs, 1))) * 0.7);
  for (const e of visible) {
    const px = x(e.seen);
    if (e.kind === 'l' || e.kind === 't') {
      g.fillStyle = e.kind === 'l' ? 'rgba(240,80,18,0.85)' : 'rgba(255,154,115,0.6)';
      g.fillRect(px - 2, 12, 4, H - pad - 12);
      continue;
    }
    if (e.kind === 'e') {
      g.fillStyle = '#ffd966';
      g.fillRect(px - 3, 12, 6, 14);
      continue;
    }
    g.fillStyle = colorFor(e, meta);
    g.fillRect(px - barW / 2, y(e.arr), barW, H - pad - y(e.arr));
    if (e.commit !== null) {
      g.fillStyle = '#6fd3ff';
      g.beginPath(); g.arc(px, y(e.commit), Math.max(2.5, barW / 2), 0, Math.PI * 2); g.fill();
    }
  }
}

function drawNumbers(flow, now) {
  const reads = flow.events.filter((e) => e.seen >= now - 1000 && 'kfmvpsx'.includes(e.kind));
  const arr = reads.map((e) => e.arr).sort((a, b) => a - b);
  const com = reads.map((e) => e.commit).filter((v) => v !== null).sort((a, b) => a - b);
  const units = reads.reduce((n, e) => n + e.count, 0);
  const last = reads[reads.length - 1];
  const behindMs = last ? last.behind * flow.meta.periodMs : null;
  const cells = [
    ['arrival p50', fmtMs(percentile(arr, 0.5)), 'ms'],
    ['arrival p99', fmtMs(percentile(arr, 0.99)), 'ms'],
    ['arrival max', fmtMs(arr.length ? arr[arr.length - 1] : null), 'ms'],
    ['commit p50', fmtMs(percentile(com, 0.5)), 'ms'],
    ['commit max', fmtMs(com.length ? com[com.length - 1] : null), 'ms'],
    ['behind head', behindMs === null ? '--' : behindMs.toFixed(1), last ? `ms, ${last.behind} ${flow.meta.unit}` : ''],
    ['rate', units.toFixed(0), `${flow.meta.unit}/s`],
  ];
  document.getElementById('numbers').innerHTML = cells
    .map(([k, v, u]) => `<div class="num"><div class="k">${k}</div><div class="v">${v} <small>${u}</small></div></div>`)
    .join('');
}

let numbersAt = 0;
function frame() {
  const now = Date.now() + skew;
  const flow = flows.get(selected);
  if (flow) {
    document.getElementById('ringTitle').textContent = `Ring of ${flow.meta.label || flow.meta.id}`;
    // A slot an NMOS controller connects is named on its own and may read
    // another flow, or none.
    const meta = flow.meta;
    document.getElementById('flowId').textContent = !meta.flowId ? `slot ${meta.id}, no flow connected`
      : meta.flowId === meta.id ? `flow ${meta.flowId}` : `slot ${meta.id}, flow ${meta.flowId}`;
    drawRing(flow, now);
    drawStrip(flow, now);
    // Four times a second: faster than that, digits blur instead of reading.
    if (Date.now() - numbersAt > 250) {
      numbersAt = Date.now();
      drawNumbers(flow, now);
      drawRingInfo(flow.meta);
    }
  }
  requestAnimationFrame(frame);
}

const SEVERITY = { ok: 'ok', too_early: 'warn', too_late: 'warn', missing: 'bad', invalid: 'bad', incomplete: 'bad',
  wrong_size: 'bad', wrong_format: 'bad', illegal_values: 'bad', permission_error: 'bad', other_error: 'bad' };

async function pollStatus() {
  try {
    const status = await (await fetch('status', { cache: 'no-store' })).json();
    document.getElementById('version').textContent = status.version || '';
    const f = status.flows.find((x) => x.id === selected) || status.flows[0];
    if (f) {
      const chips = Object.entries(f.reads).map(([k, v]) => [k, v, SEVERITY[k] || 'bad'])
        .concat([['skipped', f.skipped, 'warn'], ['stalls', f.stalls, 'warn'], ['lost', f.lost, 'warn']])
        .concat(Object.entries(f.openFailures).map(([k, v]) => ['open ' + k, v, 'bad']));
      document.getElementById('chips').innerHTML = chips
        .map(([k, v, sev]) => `<span class="chip ${v ? sev : 'zero'}">${k.replace('_', ' ')} <b>${v}</b></span>`).join('');
      document.getElementById('err').textContent = f.lastError
        ? `last error ${f.lastError.secondsAgo.toFixed(0)} s ago: ${f.lastError.message}` : '';
    }
  } catch (e) { /* the stream says whether the reader is there */ }
}
setInterval(pollStatus, 1000);
pollStatus();

function connect() {
  const conn = document.getElementById('conn');
  const es = new EventSource('stream');
  es.onopen = () => { conn.textContent = 'live'; conn.style.color = '#c8f169'; };
  es.onmessage = (m) => onBatch(JSON.parse(m.data));
  es.onerror = () => { conn.textContent = 'reconnecting'; conn.style.color = '#ffd966'; };
}
connect();
requestAnimationFrame(frame);
</script>
</body>
</html>
)PAGE";
}
