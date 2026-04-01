// ──────────────────────────────────────────────
// DOM refs
// ──────────────────────────────────────────────
const chatContainer       = document.getElementById('chat-container');
const chatScrollContainer = document.getElementById('chat-scroll');
const chatForm            = document.getElementById('chat-form');
const userInput           = document.getElementById('user-input');
const effectOverlay       = document.getElementById('weather-overlay');

const API_BASE = 'http://localhost:8010';

// ──────────────────────────────────────────────
// Session ID
// One UUID per browser tab.  Resets on tab close.
// ──────────────────────────────────────────────
function getOrCreateSid() {
    let id = sessionStorage.getItem('agribot_sid');
    if (!id) { id = crypto.randomUUID(); sessionStorage.setItem('agribot_sid', id); }
    return id;
}
let SID = getOrCreateSid();

// ──────────────────────────────────────────────
// DEBUG PANEL  (Ctrl+Shift+D to toggle)
// ──────────────────────────────────────────────
const DEBUG = new URLSearchParams(location.search).has('debug');
let _dbgPanel = null, _dbgLog = null;
let pendingSlotValues = {};

const DBG = {
    _ts() { return new Date().toLocaleTimeString('en-IN',{hour12:false})+'.'+String(Date.now()%1000).padStart(3,'0'); },
    _push(type, label, data) {
        if (!DEBUG) return;
        if (!_dbgPanel) DBG._init();
        const row = document.createElement('div');
        row.style.cssText = 'padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.06);font-size:11px;line-height:1.5';
        const C = {info:'#86efac',warn:'#fde68a',error:'#fca5a5',api:'#93c5fd',state:'#d8b4fe'};
        row.innerHTML = `<span style="color:#94a3b8">${DBG._ts()}</span>
            <span style="color:${C[type]||'#e2e8f0'};font-weight:600;margin:0 6px">[${type.toUpperCase()}]</span>
            <span style="color:#f1f5f9">${label}</span>
            ${data!==undefined?`<pre style="margin:2px 0 0 12px;color:#94a3b8;white-space:pre-wrap;font-size:10px">${JSON.stringify(data,null,2)}</pre>`:''}`;
        _dbgLog.prepend(row);
        if (_dbgLog.children.length > 80) _dbgLog.lastChild.remove();
    },
    _init() {
        _dbgPanel = document.createElement('div');
        _dbgPanel.style.cssText = 'position:fixed;bottom:0;right:0;width:420px;height:320px;background:#0f172a;border:1px solid #1e293b;border-radius:8px 0 0 0;display:flex;flex-direction:column;z-index:9999;font-family:monospace;box-shadow:0 -4px 24px rgba(0,0,0,.5);resize:both;overflow:hidden';
        _dbgPanel.innerHTML = `
            <div style="display:flex;align-items:center;justify-content:space-between;padding:6px 10px;background:#1e293b;flex-shrink:0">
                <span style="color:#86efac;font-weight:700;font-size:11px">🐛 AgriBot
                    <span style="color:#94a3b8;font-weight:400">— ${API_BASE} · ${SID.slice(0,8)}…</span>
                </span>
                <div style="display:flex;gap:6px">
                    <button id="dbg-reset" style="background:#374151;color:#fde68a;border:none;padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer">Reset Session</button>
                    <button id="dbg-clear" style="background:#374151;color:#d1d5db;border:none;padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer">Clear</button>
                    <button id="dbg-x"     style="background:#374151;color:#d1d5db;border:none;padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer">✕</button>
                </div>
            </div>
            <div id="dbg-log" style="flex:1;overflow-y:auto;padding:6px 10px;scrollbar-width:thin"></div>`;
        document.body.appendChild(_dbgPanel);
        _dbgLog = document.getElementById('dbg-log');
        document.getElementById('dbg-clear').onclick = () => _dbgLog.innerHTML = '';
        document.getElementById('dbg-x').onclick     = () => _dbgPanel.style.display = 'none';
        document.getElementById('dbg-reset').onclick = () => resetSession();
    },
    info (l,d){ console.log (`[INFO]  ${l}`,d??''); DBG._push('info', l,d); },
    warn (l,d){ console.warn(`[WARN]  ${l}`,d??''); DBG._push('warn', l,d); },
    error(l,d){ console.error(`[ERR]  ${l}`,d??''); DBG._push('error',l,d); },
    api  (l,d){ console.log (`[API]   ${l}`,d??''); DBG._push('api',  l,d); },
    state(l,d){ console.log (`[STATE] ${l}`,d??''); DBG._push('state',l,d); },
};

document.addEventListener('keydown', e => {
    if (e.ctrlKey && e.shiftKey && e.key === 'D') {
        e.preventDefault();
        if (!_dbgPanel) DBG._init();
        _dbgPanel.style.display = _dbgPanel.style.display === 'none' ? 'flex' : 'none';
    }
});
if (DEBUG) DBG.info('Debug active', { sid: SID });

// ──────────────────────────────────────────────
// CORE API CALL
// Every request goes through here with the session ID header.
// ──────────────────────────────────────────────
async function api(payload) {
    DBG.api('POST /api/chat', payload);
    const r = await fetch(`${API_BASE}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Session-ID': SID },
        body: JSON.stringify(payload),
    });
    if (!r.ok) {
        const e = await r.json().catch(() => ({}));
        throw new Error(e?.detail ?? `HTTP ${r.status}`);
    }
    const data = await r.json();
    DBG.api('Response', data);
    return data;
}

async function resetSession() {
    try { await fetch(`${API_BASE}/api/session/${SID}`, { method: 'DELETE' }); } catch(_){}
    sessionStorage.removeItem('agribot_sid');
    SID = getOrCreateSid();
    DBG.info('Session reset', { new_sid: SID });
}

// ──────────────────────────────────────────────
// FORM SUBMIT
// ──────────────────────────────────────────────
chatForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const q = userInput.value.trim();
    if (!q) return;
    displayMessage(q, 'user', false);
    userInput.value = '';
    userInput.style.height = 'auto';
    await run({ message: q });
});

userInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        chatForm.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    }
});

// ──────────────────────────────────────────────
// MAIN RUNNER
// Calls the backend and routes the typed response.
// ──────────────────────────────────────────────
async function run(payload) {
    const spinner = displayTypingIndicator();
    try {
        const resp = await api(payload);
        spinner.remove();
        render(resp);
    } catch (err) {
        spinner.remove();
        DBG.error('run() failed', { msg: err.message });
        displayMessage(`Connection error: ${err.message}`, 'bot', true);
    }
}

// ──────────────────────────────────────────────
// RESPONSE ROUTER
// ──────────────────────────────────────────────
function render(resp) {
    DBG.state('render', { type: resp.type });
    switch (resp.type) {
        case 'ask_slots':           renderAskSlots(resp);         break;
        case 'ask_location_choice': renderLocationChoice(resp);   break;
        case 'weather_result':      renderWeatherReport(resp.data, resp.location_label, resp.intent, resp.is_hourly); break;
        case 'pest_result':         renderPestReport(resp.data, resp.state, resp.is_next_week, resp.intent); break;
        case 'no_location':         displayEmptyState(resp.message, 'generic', resp.suggestions ?? []); break;
        case 'error':               displayEmptyState(resp.message, 'generic', ['weather in Hyderabad','pests in paddy in Guntur']); break;
        default:                    displayMessage(`Unknown response type: ${resp.type}`, 'bot', true);
    }
}

async function submitSlotsSequentially(values) {
    const resp = await api({ message: "", slots: values });
    
    if (resp.type === 'ask_location_choice') {
        pendingSlotValues = {};  // no pending needed anymore
        render(resp);
        return;
    }
    render(resp);
}

function renderAskSlots(resp) {
    const fields  = resp.fields  ?? [];
    const crop    = resp.crop_slug ? ` for <strong>${resp.crop_slug}</strong>` : '';
    const isPest  = resp.intent_type === 'pest';

    const HEADING = isPest
        ? `🌾 A few more details needed${crop}`
        : `📍 Which location are you asking about?`;

    const FIELD_META = {
        location:    { label: 'Location',    hint: 'e.g. Hyderabad, Guntur, Vijayawada', icon: 'location_on' },
        crop:        { label: 'Crop',        hint: 'e.g. paddy, wheat, cotton',           icon: 'grass' },
        sowing_date: { label: 'Sowing Date', hint: 'DD-MM-YYYY  e.g. 10-12-2025',        icon: 'calendar_today' },
    };

    const inputsHtml = fields.map(f => {
        const meta  = FIELD_META[f.slot] ?? { label: f.label, hint: f.hint, icon: 'info' };
        const errHtml = f.error
            ? `<p class="text-xs text-red-500 mt-1">${f.error}</p>`
            : '';
        return `
        <div class="space-y-1">
            <label class="text-xs font-bold text-slate-500 uppercase tracking-wider flex items-center gap-1">
                <span class="material-symbols-outlined text-sm">${meta.icon}</span>
                ${meta.label}
            </label>
            <input id="slot-${f.slot}" data-slot="${f.slot}" type="text"
                   placeholder="${meta.hint}" autocomplete="off"
                   class="w-full px-3 py-2.5 text-sm rounded-xl border
                          ${f.error ? 'border-red-400 ring-1 ring-red-300' : 'border-slate-200'}
                          bg-white dark:bg-slate-900 dark:border-slate-700
                          focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary
                          transition placeholder-slate-300" />
            ${errHtml}
        </div>`;
    }).join('');

    const cardId = `slot-card-${Date.now()}`;
    displayMessage(`
    <div id="${cardId}"
         class="bg-white dark:bg-slate-800/60 border border-slate-200 dark:border-slate-700
                rounded-2xl p-5 shadow-sm space-y-4">
        <div class="flex items-center gap-2 text-amber-600 dark:text-amber-400">
            <span class="material-symbols-outlined text-xl">info</span>
            <p class="text-sm font-semibold">${HEADING}</p>
        </div>
        <div class="space-y-3">${inputsHtml}</div>
        <button id="${cardId}-submit"
                class="w-full py-2.5 bg-primary text-white text-sm font-bold rounded-xl
                       hover:bg-primary/90 active:scale-[0.98] transition-all">
            Continue →
        </button>
    </div>`, 'bot', true);

    // Focus first input
    setTimeout(() => document.getElementById(`slot-${fields[0]?.slot}`)?.focus(), 100);

    document.getElementById(`${cardId}-submit`).addEventListener('click', async () => {
        let allFilled = true;
        const values = {};

        fields.forEach(f => {
            const el  = document.getElementById(`slot-${f.slot}`);
            const val = el?.value?.trim();
            if (!val) {
                allFilled = false;
                el?.classList.add('border-red-400','ring-1','ring-red-300');
            } else {
                el?.classList.remove('border-red-400','ring-1','ring-red-300');
                values[f.slot] = val;
            }
        });

        if (!allFilled) return;

        // Collapse card
        const card = document.getElementById(cardId);
        if (card) {
            const summary = Object.entries(values)
                .map(([k,v]) => `<strong>${k.replace('_',' ')}</strong>: ${v}`)
                .join(' · ');
            card.innerHTML = `
            <p class="text-xs text-slate-400 flex items-center gap-1.5">
                <span class="material-symbols-outlined text-sm text-emerald-500">check_circle</span>
                ${summary} — fetching data…
            </p>`;
        }

        await submitSlotsSequentially(values);
    });
}

// ──────────────────────────────────────────────
// LOCATION DISAMBIGUATION CHIPS
// ──────────────────────────────────────────────
function renderLocationChoice(resp) {
    const wrap = document.createElement('div');
    wrap.className = 'flex flex-wrap gap-2 mt-4';

    (resp.candidates ?? []).forEach(c => {
        const btn = document.createElement('button');
        btn.className = 'px-4 py-2 rounded-xl border border-slate-200 bg-white hover:bg-primary/10 hover:border-primary/30 hover:text-primary transition-all text-sm font-medium';
        btn.textContent = c.label;
        btn.onclick = async () => {
            wrap.querySelectorAll('button').forEach(b => {
                b.disabled = true;
                b.classList.add('opacity-50');
            });
            btn.closest('.group,.msg-anim')?.remove();
            // Location choice — other slots already saved in backend session
            await run({ message: c.label, location_choice_index: c.index });
        };
        wrap.appendChild(btn);
    });

    displayMessage(`<p>Which location did you mean?</p><div id="loc-chips"></div>`, 'bot', true);
    const target = document.getElementById('loc-chips');
    if (target) { target.appendChild(wrap); target.removeAttribute('id'); }
}

// ──────────────────────────────────────────────
// INJECT QUERY  (suggestion chips)
// ──────────────────────────────────────────────
function injectQuery(btn) {
    const q = btn.dataset.query;
    if (!q) return;
    userInput.value = q;
    chatForm.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
}

// ──────────────────────────────────────────────
// EMPTY STATE
// ──────────────────────────────────────────────
const EMPTY_CFG = {
    weather: { icon: '🌤', title: 'No Forecast Data', color: '#3b82f6' },
    pest:    { icon: '🐛', title: 'No Pest Data',     color: '#f59e0b' },
    generic: { icon: '📭', title: 'No Data Found',    color: '#94a3b8' },
};

function displayEmptyState(message, type = 'generic', suggestions = []) {
    const cfg = EMPTY_CFG[type] ?? EMPTY_CFG.generic;
    const sugg = suggestions.length ? `
    <div class="mt-4 border-t border-slate-100 pt-4">
        <p class="text-xs text-slate-400 mb-2 font-semibold uppercase tracking-wider">Try asking</p>
        <div class="flex flex-wrap gap-2">
            ${suggestions.map(s=>`
            <button onclick="injectQuery(this)" data-query="${s.replace(/"/g,'&quot;')}"
                    class="px-3 py-1.5 text-xs font-medium rounded-lg border border-slate-200
                           bg-slate-50 hover:bg-primary/10 hover:border-primary/30 hover:text-primary
                           transition-all cursor-pointer">${s}</button>`).join('')}
        </div>
    </div>` : '';

    displayMessage(`
    <div class="bg-white dark:bg-slate-800/60 border border-slate-100 dark:border-slate-700
                rounded-2xl p-6 shadow-sm text-center space-y-3">
        <div class="text-4xl">${cfg.icon}</div>
        <div>
            <p class="font-bold text-slate-700 dark:text-slate-200 text-base">${cfg.title}</p>
            <p class="text-sm text-slate-500 dark:text-slate-400 mt-1 leading-relaxed">${message}</p>
        </div>
        <div class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold"
             style="background:${cfg.color}18;color:${cfg.color}">
            <span class="material-symbols-outlined text-sm">info</span>
            Data unavailable for this query
        </div>
        ${sugg}
    </div>`, 'bot', true);
    triggerWeatherEffectFromData([]);
}

// ──────────────────────────────────────────────
// WEATHER EFFECTS
// ──────────────────────────────────────────────
function triggerWeatherEffectFromData(records) {
    if (!records?.length) { clearWeatherEffect(); return; }
    const maxRain = Math.max(...records.map(r => r.Rainfall ?? 0));
    const minTemp = Math.min(...records.map(r => r.Tmin ?? 99));
    const maxTemp = Math.max(...records.map(r => r.Tmax ?? 0));
    const maxWind = Math.max(...records.map(r => r.Wind_Speed ?? 0));
    clearWeatherEffect();
    if (maxRain > 0.5)     createRainEffect(Math.min(Math.max(maxRain/20,0.15),1));
    else if (minTemp < 15) createColdEffect();
    else if (maxTemp > 36) createHeatEffect();
    else if (maxWind > 25) createWindEffect();
}
function clearWeatherEffect() {
    effectOverlay.innerHTML = '';
    effectOverlay.style.background = '';
    effectOverlay.className = 'fixed inset-0 z-50 pointer-events-none overflow-hidden';
}
function createRainEffect(i=0.5) {
    for (let n=0;n<Math.round(40+i*110);n++) {
        const d=document.createElement('div'); d.className='rain-drop';
        d.style.left=Math.random()*100+'vw';
        d.style.animationDuration=(Math.random()*.4+.35)+'s';
        d.style.animationDelay=Math.random()*2+'s';
        d.style.width=i>.6?'2px':'1.5px';
        effectOverlay.appendChild(d);
    }
    effectOverlay.style.background=`rgba(15,23,42,${(0.04+i*0.08).toFixed(3)})`;
}
function createHeatEffect() {
    const h=document.createElement('div'); h.className='heat-wave';
    effectOverlay.appendChild(h); effectOverlay.style.background='rgba(251,146,60,0.03)';
}
function createColdEffect() {
    const f=document.createElement('div'); f.className='frost-edge'; effectOverlay.appendChild(f);
    for(let i=0;i<50;i++){
        const s=document.createElement('div'); s.className='snowflake'; s.innerHTML='❄';
        s.style.left=Math.random()*100+'vw';
        s.style.opacity=(Math.random()*.6+.3).toString();
        s.style.animationDuration=(Math.random()*3+2)+'s';
        s.style.animationDelay=Math.random()*5+'s';
        effectOverlay.appendChild(s);
    }
    effectOverlay.style.background='rgba(186,230,253,0.04)';
}
function createWindEffect() {
    const w=document.createElement('div'); w.className='heat-wave';
    w.style.background='radial-gradient(ellipse at 30% 40%,rgba(148,163,184,.2) 0%,transparent 65%)';
    effectOverlay.appendChild(w);
}

// ──────────────────────────────────────────────
// MESSAGE HELPERS
// ──────────────────────────────────────────────
function displayMessage(content, sender, isBot=false) {
    const w = document.createElement('div');
    if (isBot || !sender) {
        w.className = 'flex gap-3 group msg-anim';
        w.innerHTML = `
        <div class="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white shrink-0 mt-1">
            <span class="material-symbols-outlined text-lg fill-1">smart_toy</span>
        </div>
        <div class="space-y-4 flex-1">
            <div class="bg-slate-50 dark:bg-slate-800/50 p-5 rounded-2xl rounded-tl-none border border-slate-100 dark:border-slate-800 shadow-sm">
                <div class="leading-relaxed text-slate-800 dark:text-slate-200">${content}</div>
            </div>
        </div>`;
    } else {
        w.className = 'flex gap-3 flex-row-reverse msg-anim';
        w.innerHTML = `
        <div class="w-8 h-8 rounded-full bg-slate-200 overflow-hidden shrink-0 mt-1">
            <img class="w-full h-full object-cover" src="https://lh3.googleusercontent.com/aida-public/AB6AXuCa8NI8CsYhNzys5loTTFFAyky2OsF8ynjnr4qeLl9L-bM2aKmWcHsy3w95kRJMIqZBt5Dni2QvquaQfHtNyt5TEm4YpICq7VbV8DVAr3VsmV7X8tUg4CFpNKgc4aySVYwwYQrAFS7ZCkjVA9BTas5hQm83FXAYcA4X5Psda5rCkhK49-60l0GmuZGe5c5IG-Z1jXIUNhA6qk_DntnNsP9IBVVcR3SdrU9udfJYDlPI4PGuXXi2oizTNiEkfr0JD5awnCNbB7kCjeEL" alt="User">
        </div>
        <div class="max-w-[80%]">
            <div class="bg-primary text-white p-4 rounded-2xl rounded-tr-none shadow-md">
                <p class="leading-relaxed">${content}</p>
            </div>
            <p class="text-[10px] text-slate-400 mt-1 text-right">Delivered now</p>
        </div>`;
    }
    chatContainer.appendChild(w);
    requestAnimationFrame(() => requestAnimationFrame(() => {
        chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
    }));
}

function displayTypingIndicator() {
    const w = document.createElement('div');
    w.className = 'flex gap-4 group typing-indicator-msg';
    w.innerHTML = `
    <div class="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white shrink-0 mt-1">
        <span class="material-symbols-outlined text-lg fill-1">smart_toy</span>
    </div>
    <div class="space-y-4 flex-1">
        <div class="bg-slate-50 dark:bg-slate-800/50 p-3 w-20 rounded-2xl rounded-tl-none border border-slate-100 flex justify-center gap-1">
            <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce"></div>
            <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce" style="animation-delay:.1s"></div>
            <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce" style="animation-delay:.2s"></div>
        </div>
    </div>`;
    chatContainer.appendChild(w);
    requestAnimationFrame(() => requestAnimationFrame(() => {
        chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
    }));
    return w;
}

// ──────────────────────────────────────────────
// WEATHER RENDER
// ──────────────────────────────────────────────
function renderWeatherReport(data, location, intent, isHourly=false) {
    const records = data['Forecast data'] ?? data['weather_data'] ?? [];
    if (!records.length) {
        const msg = data._message || 'No forecast data available.';
        displayEmptyState(
            msg.toLowerCase().includes('onwards') ? 'GFS only provides <strong>future forecasts</strong>.' : msg,
            'weather', ['weather today in Hyderabad','rain tomorrow in Vijayawada','7-day forecast in Mumbai']
        );
        triggerWeatherEffectFromData([]);
        return;
    }

    const metric  = intent?.metric ?? 'ALL';
    const isRange = records.length > 1;
    const w       = records[0];

    if (isHourly) {
        const fmtH = raw => {
            const d = new Date(raw);
            return d.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',hour12:true})
                 + ' · ' + d.toLocaleDateString('en-IN',{weekday:'short',day:'numeric',month:'short'});
        };
        const cols = metric === 'Rainfall' ? [
            {h:'Time',  c:r=>fmtH(r.Date_time),                     cl:'text-slate-500 whitespace-nowrap'},
            {h:'Rain',  c:r=>`${(r.Rainfall??0).toFixed(1)} mm`,     cl:'font-bold text-blue-600'},
            {h:'Humid', c:r=>`${Math.round(r.RH??0)}%`,              cl:'text-slate-500'},
        ] : metric === 'Wind_Speed' ? [
            {h:'Time',  c:r=>fmtH(r.Date_time),                     cl:'text-slate-500 whitespace-nowrap'},
            {h:'Wind',  c:r=>`${Math.round(r.Wind_Speed??0)} km/h`,  cl:'font-bold text-slate-700'},
            {h:'Humid', c:r=>`${Math.round(r.RH??0)}%`,              cl:'text-slate-500'},
        ] : [
            {h:'Time',  c:r=>fmtH(r.Date_time),                                            cl:'text-slate-500 whitespace-nowrap'},
            {h:'Temp',  c:r=>`${Math.round(r.Tavg??r.Tmax??0)}°C`,                          cl:'font-bold text-primary'},
            {h:'Feels', c:r=>`${Math.round(r.Tmin??0)}°–${Math.round(r.Tmax??0)}°`,         cl:'text-slate-500'},
            {h:'Rain',  c:r=>`${(r.Rainfall??0).toFixed(1)}mm`,                             cl:'text-blue-500'},
            {h:'Humid', c:r=>`${Math.round(r.RH??0)}%`,                                    cl:'text-cyan-600'},
        ];
        const rows = records.map(r=>`<tr class="border-t border-slate-100 hover:bg-slate-50 transition-colors">${cols.map(c=>`<td class="py-2.5 pr-4 text-xs ${c.cl}">${c.c(r)}</td>`).join('')}</tr>`).join('');
        displayMessage(`
        <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
            <div class="flex justify-between items-center mb-3">
                <h3 class="text-base font-bold text-slate-800">📍 ${location}</h3>
                <span class="text-xs text-slate-400">⏱ Next ${records.length} hours</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left">
                    <thead><tr class="text-[9px] uppercase tracking-widest text-slate-400">${cols.map(c=>`<th class="pb-2 pr-4 font-bold">${c.h}</th>`).join('')}</tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>`, 'bot', true);
        triggerWeatherEffectFromData(records);
        return;
    }

    const fmtD  = raw => new Date(raw).toLocaleDateString('en-IN',{weekday:'short',day:'numeric',month:'short'});
    const dateL = new Date(w.Date_time||w.Date).toLocaleDateString('en-IN',{weekday:'long',day:'numeric',month:'short',year:'numeric'});

    const HERO = {
        Tmax:       {label:'Max Temperature',value:()=>`${Math.round(w.Tmax??0)}°C`,          icon:'🌡',color:'#f97316'},
        Tmin:       {label:'Min Temperature',value:()=>`${Math.round(w.Tmin??0)}°C`,          icon:'🥶',color:'#3b82f6'},
        Tavg:       {label:'Temperature',    value:()=>`${Math.round(w.Tavg??0)}°C`,          icon:'🌡',color:'#2e7d32'},
        Rainfall:   {label:'Rainfall',       value:()=>`${(w.Rainfall??0).toFixed(1)} mm`,    icon:'🌧',color:'#2563eb'},
        RH:         {label:'Humidity',       value:()=>`${Math.round(w.RH??0)}%`,             icon:'💧',color:'#0891b2'},
        Wind_Speed: {label:'Wind Speed',     value:()=>`${Math.round(w.Wind_Speed??0)} km/h`, icon:'💨',color:'#475569'},
    };
    const hero     = HERO[metric] ?? null;
    const showHero = hero && metric !== 'ALL';

    const cond = (() => {
        const rain=w.Rainfall??0,tmax=w.Tmax??0,tmin=w.Tmin??0,wind=w.Wind_Speed??0,rh=w.RH??0;
        if(rain>10) return '⛈ Heavy Rain'; if(rain>2) return '🌧 Moderate Rain';
        if(rain>.5) return '🌦 Light Showers'; if(tmax>40) return '🔥 Extreme Heat';
        if(tmax>36) return '☀️ Very Hot'; if(tmin<10) return '🥶 Very Cold';
        if(tmin<15) return '❄️ Cold'; if(wind>40) return '🌪 Strong Winds';
        if(wind>25) return '💨 Windy'; if(rh>85) return '😮‍💨 Very Humid';
        return '☀️ Clear';
    })();

    const stats = metric==='ALL' ? [
        {label:'Max',      val:`${Math.round(w.Tmax??0)}°C`},
        {label:'Min',      val:`${Math.round(w.Tmin??0)}°C`},
        {label:'Humidity', val:`${Math.round(w.RH??0)}%`},
        {label:'Rainfall', val:`${(w.Rainfall??0).toFixed(1)} mm`},
        {label:'Wind',     val:`${Math.round(w.Wind_Speed??0)} km/h`},
    ] : [
        {label:'Max Temp',   val:`${Math.round(w.Tmax??0)}°C`,          show:metric!=='Tmax'},
        {label:'Min Temp',   val:`${Math.round(w.Tmin??0)}°C`,          show:metric!=='Tmin'},
        {label:'Humidity',   val:`${Math.round(w.RH??0)}%`,             show:metric!=='RH'},
        {label:'Rainfall',   val:`${(w.Rainfall??0).toFixed(1)} mm`,    show:metric!=='Rainfall'},
        {label:'Wind Speed', val:`${Math.round(w.Wind_Speed??0)} km/h`, show:metric!=='Wind_Speed'},
    ].filter(s=>s.show);

    const single = `
    <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
        <div class="flex justify-between items-start">
            <div>
                <p class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">${dateL}</p>
                <h3 class="text-base font-bold text-slate-800 mt-0.5">📍 ${location}</h3>
            </div>
            <span class="text-xs font-semibold px-2.5 py-1 bg-slate-100 text-slate-600 rounded-full">${cond}</span>
        </div>
        ${showHero ? `
        <div class="mt-4 py-4 border-y border-slate-100 flex items-center gap-4">
            <span style="font-size:2.25rem;line-height:1">${hero.icon}</span>
            <div>
                <p class="text-[10px] text-slate-400 uppercase font-bold tracking-wider">${hero.label}</p>
                <p style="font-size:2.5rem;font-weight:900;line-height:1;margin-top:.25rem;color:${hero.color}">${hero.value()}</p>
            </div>
        </div>` : `
        <div class="mt-4 flex items-end gap-2">
            <span class="font-black text-primary leading-none" style="font-size:3rem">${Math.round(w.Tavg??0)}°C</span>
            <span class="text-sm text-slate-400 mb-1">avg</span>
        </div>`}
        <div style="display:grid;grid-template-columns:repeat(${metric==='ALL'?3:2},1fr);gap:.5rem;margin-top:1rem">
            ${stats.map(s=>`
            <div style="padding:.625rem;background:#f8fafc;border:1px solid #f1f5f9;border-radius:.75rem">
                <p style="font-size:.6rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.06em">${s.label}</p>
                <p style="font-size:.875rem;font-weight:700;color:#334155;margin-top:.125rem">${s.val}</p>
            </div>`).join('')}
        </div>
    </div>`;

    const rangeCols = metric==='Wind_Speed' ? [
        {h:'Date',      c:r=>fmtD(r.Date_time||r.Date), cl:'text-slate-500'},
        {h:'Wind Speed',c:r=>`${Math.round(r.Wind_Speed??0)} km/h`,cl:'font-bold text-slate-700'},
        {h:'Condition', c:r=>(r.Rainfall??0)>.5?'🌧':(r.Wind_Speed??0)>25?'💨':'☀️',cl:''},
    ] : metric==='Rainfall' ? [
        {h:'Date',    c:r=>fmtD(r.Date_time||r.Date),          cl:'text-slate-500'},
        {h:'Rainfall',c:r=>`${(r.Rainfall??0).toFixed(1)} mm`, cl:'font-bold text-blue-600'},
        {h:'Humidity',c:r=>`${Math.round(r.RH??0)}%`,          cl:'text-slate-600'},
    ] : metric==='RH' ? [
        {h:'Date',    c:r=>fmtD(r.Date_time||r.Date),          cl:'text-slate-500'},
        {h:'Humidity',c:r=>`${Math.round(r.RH??0)}%`,          cl:'font-bold text-cyan-600'},
        {h:'Rainfall',c:r=>`${(r.Rainfall??0).toFixed(1)} mm`, cl:'text-slate-600'},
    ] : metric==='Tmax' ? [
        {h:'Date',    c:r=>fmtD(r.Date_time||r.Date),      cl:'text-slate-500'},
        {h:'Max Temp',c:r=>`${Math.round(r.Tmax??0)}°C`,   cl:'font-bold text-orange-500'},
        {h:'Min Temp',c:r=>`${Math.round(r.Tmin??0)}°C`,   cl:'text-slate-500'},
    ] : metric==='Tmin' ? [
        {h:'Date',    c:r=>fmtD(r.Date_time||r.Date),      cl:'text-slate-500'},
        {h:'Min Temp',c:r=>`${Math.round(r.Tmin??0)}°C`,   cl:'font-bold text-blue-500'},
        {h:'Max Temp',c:r=>`${Math.round(r.Tmax??0)}°C`,   cl:'text-slate-500'},
    ] : [
        {h:'Date',c:r=>fmtD(r.Date_time||r.Date),          cl:'text-slate-500'},
        {h:'Avg', c:r=>`${Math.round(r.Tavg??0)}°C`,        cl:'font-bold'},
        {h:'Max', c:r=>`${Math.round(r.Tmax??0)}°C`,        cl:'text-orange-500'},
        {h:'Rain',c:r=>`${(r.Rainfall??0).toFixed(1)}mm`,  cl:'text-blue-500'},
        {h:'Wind',c:r=>`${Math.round(r.Wind_Speed??0)}km/h`,cl:'text-slate-500'},
    ];
    const rrows = records.map(r=>`<tr class="border-t border-slate-100 hover:bg-slate-50 transition-colors">${rangeCols.map(c=>`<td class="py-2.5 pr-3 text-xs ${c.cl}">${c.c(r)}</td>`).join('')}</tr>`).join('');
    const range = `
    <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
        <div class="flex justify-between items-center mb-3">
            <h3 class="text-base font-bold text-slate-800">📍 ${location}</h3>
            <span class="text-xs text-slate-400">${records.length}-day forecast</span>
        </div>
        <div class="overflow-x-auto">
            <table class="w-full text-left">
                <thead><tr class="text-[9px] uppercase tracking-widest text-slate-400">${rangeCols.map(c=>`<th class="pb-2 pr-3 font-bold">${c.h}</th>`).join('')}</tr></thead>
                <tbody>${rrows}</tbody>
            </table>
        </div>
    </div>`;

    displayMessage(`
    <div class="space-y-3">
        ${data._message?`<div class="p-3 bg-emerald-50 border-l-4 border-emerald-400 rounded-r-lg text-sm text-emerald-800">${data._message}</div>`:''}
        ${isRange ? range : single}
    </div>`, 'bot', true);

    triggerWeatherEffectFromData(records);
    requestAnimationFrame(() => requestAnimationFrame(() => { chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight; }));
}

// ──────────────────────────────────────────────
// PEST RENDER
// ──────────────────────────────────────────────
function renderPestReport(data, state, isNextWeek, intent) {
    if (!data.status || !Array.isArray(data.data) || !data.data.length) {
        displayEmptyState('No pest data available for this crop, region, or sowing date.', 'pest',
            ['paddy pests in Hyderabad sown 10-12-2025','wheat pests in Punjab sown 01-11-2025']);
        return;
    }
    const weekKey   = isNextWeek ? 'next_week' : 'current_week';
    const weekLabel = isNextWeek ? 'Next Week' : 'This Week';
    const crop      = intent?.crop_slug ?? 'paddy';

    const active = data.data
        .map(r => ({
            name: r.infestation_name,
            prob: (r.chances_percentage??{})[weekKey] ?? 0,
            risk: riskLabel((r.chances_percentage??{})[weekKey] ?? 0),
        }))
        .filter(r => r.prob > 0)
        .sort((a,b) => b.prob - a.prob);

    const clearCount = data.data.length - active.length;
    const RS = {High:'bg-red-100 text-red-700', Medium:'bg-amber-100 text-amber-700', Low:'bg-yellow-100 text-yellow-700'};

    const listHtml = active.length ? active.map(r=>`
    <div class="flex items-center justify-between p-3 bg-slate-50 rounded-lg border border-slate-100">
        <span class="text-sm font-semibold text-slate-700">${r.name}</span>
        <div class="flex items-center gap-2">
            <div class="w-20 bg-slate-200 rounded-full h-1.5">
                <div class="h-1.5 rounded-full ${r.risk==='High'?'bg-red-500':r.risk==='Medium'?'bg-amber-500':'bg-yellow-400'}"
                     style="width:${Math.min(r.prob,100)}%"></div>
            </div>
            <span class="text-xs font-bold px-2 py-1 rounded-full ${RS[r.risk]??'bg-slate-100 text-slate-600'}">
                ${r.prob}% · ${r.risk}
            </span>
        </div>
    </div>`).join('')
    : `<p class="text-sm text-slate-500 italic">✅ No significant infestation risks detected ${weekLabel.toLowerCase()}.</p>`;

    const meta = data.request_location
        ? `<p class="text-xs text-slate-400 mt-1">${data.request_location.district??''}, ${data.request_location.state??''}</p>`
        : '';

    displayMessage(`
    <div class="bg-white border border-slate-200 rounded-xl p-6 shadow-sm">
        <div class="flex items-start justify-between mb-4">
            <div class="flex items-center gap-2">
                <span class="material-symbols-outlined text-amber-500">bug_report</span>
                <div>
                    <h3 class="font-bold text-lg leading-tight">Pest Report · ${state??'Unknown'}</h3>
                    ${meta}
                </div>
            </div>
            <span class="text-xs font-semibold px-2 py-1 bg-slate-100 text-slate-600 rounded-lg">${weekLabel}</span>
        </div>
        <div class="space-y-2">${listHtml}</div>
        ${active.length?`
        <div class="mt-4 p-4 bg-amber-50 border-l-4 border-amber-400 rounded-r-lg text-xs text-amber-800">
            <strong>⚠ Precaution:</strong> High risk levels indicate immediate action may be required.
            Consult your local agricultural advisor.
        </div>`:''}
        <div class="mt-3 text-[10px] text-slate-400 text-right">
            ${clearCount} pest${clearCount!==1?'s':''} at 0% risk · Crop: ${crop}
        </div>
    </div>`, 'bot', true);

    clearWeatherEffect();
}

function riskLabel(p) {
    if (p >= 60) return 'High';
    if (p >= 30) return 'Medium';
    if (p > 0)   return 'Low';
    return 'None';
}

// ──────────────────────────────────────────────
// GREETING
// ──────────────────────────────────────────────
window.onload = () => displayMessage(
    "Hello! I'm <strong>Niruthi Bot</strong> 🌱<br>" +
    "<span class='text-sm text-slate-500 font-normal'>Ask me about weather forecasts or pest infestation risks for any region in India.</span>",
    'bot', true
);