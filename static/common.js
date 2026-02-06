// Shared UI helpers for all pages.

function $(sel, root=document){ return root.querySelector(sel); }
function $all(sel, root=document){ return Array.from(root.querySelectorAll(sel)); }

async function jget(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
async function jpost(url, body) {
  const r = await fetch(url, {
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body || {})
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function fmtUptime(startedAtSec){
  if (!startedAtSec) return "—";
  const s = Math.max(0, Math.floor(Date.now()/1000 - startedAtSec));
  const hh = Math.floor(s/3600);
  const mm = Math.floor((s%3600)/60);
  const ss = s%60;
  if (hh>0) return `${hh}h ${mm}m ${ss}s`;
  if (mm>0) return `${mm}m ${ss}s`;
  return `${ss}s`;
}

function setBadge(el, kind, text){
  if (!el) return;
  el.classList.remove("good","bad","warn");
  if (kind) el.classList.add(kind);
  el.textContent = text || "";
}

let _toastTimer = null;
function toast(msg, kind="info", ms=2400){
  const el = document.querySelector("#toast");
  if (!el) { console.log("[toast]", msg); return; }
  el.textContent = msg;
  el.classList.remove("good","bad","warn");
  if (kind==="good"||kind==="bad"||kind==="warn") el.classList.add(kind);
  el.classList.add("show");
  if (_toastTimer) clearTimeout(_toastTimer);
  _toastTimer = setTimeout(()=>el.classList.remove("show"), ms);
}

async function copyText(text){
  try{
    await navigator.clipboard.writeText(String(text));
    toast("Copied", "good");
  }catch(e){
    // fallback
    const ta=document.createElement("textarea");
    ta.value=String(text);
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    ta.remove();
    toast("Copied", "good");
  }
}

// Prevent periodic refresh from overwriting form edits.
let _netFormLockedUntil = 0;
function lockNetworkFormFor(ms){
  _netFormLockedUntil = Date.now() + (ms||1200);
}
function isNetworkFormLocked(){
  return Date.now() < _netFormLockedUntil;
}
// Auto-wire TV-Headend button(s) to this device's hostname on port 9981.
// Use: <a class="miniBtn" data-tvh-link href="#">TV-Headend</a>
function setTvheadendLinks(){
  try{
    const host = window.location.hostname;
    if (!host) return;
    const url = `http://${host}:9981/`;
    document.querySelectorAll('[data-tvh-link]').forEach(a => {
      a.href = url;
      a.target = "_blank";
      a.rel = "noopener";
      a.title = "Open TV-Headend";
    });
  }catch(e){
    // ignore
  }
}
document.addEventListener('DOMContentLoaded', setTvheadendLinks);
