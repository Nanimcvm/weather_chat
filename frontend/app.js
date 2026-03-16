const chatContainer = document.getElementById('chat-container');
const chatScrollContainer = document.getElementById('chat-scroll');
const chatForm = document.getElementById('chat-form');
const userInput = document.getElementById('user-input');
const effectOverlay = document.getElementById('weather-overlay');

const API_BASE = 'http://localhost:8010';

// ──────────────────────────────────────────────
// DEBUG PANEL
// Toggle with Ctrl+Shift+D  or  ?debug=1 in URL
// ──────────────────────────────────────────────
const DEBUG_ENABLED = new URLSearchParams(location.search).has('debug');
let _debugPanel = null, _debugLog = null;

const DBG = {
    _ts() { return new Date().toLocaleTimeString('en-IN', { hour12: false }) + '.' + String(Date.now() % 1000).padStart(3, '0'); },
    _push(type, label, data) {
        if (!DEBUG_ENABLED) return;
        if (!_debugPanel) DBG._init();
        const row = document.createElement('div');
        row.style.cssText = 'padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.06);font-size:11px;line-height:1.5';
        const COLORS = { info: '#86efac', warn: '#fde68a', error: '#fca5a5', api: '#93c5fd', intent: '#d8b4fe', data: '#67e8f9' };
        row.innerHTML = `
                <span style="color:#94a3b8">${DBG._ts()}</span>
                <span style="color:${COLORS[type] ?? '#e2e8f0'};font-weight:600;margin:0 6px">[${type.toUpperCase()}]</span>
                <span style="color:#f1f5f9">${label}</span>
                ${data !== undefined ? `<pre style="margin:2px 0 0 12px;color:#94a3b8;white-space:pre-wrap;word-break:break-all;font-size:10px">${JSON.stringify(data, null, 2)}</pre>` : ''}
            `;
        _debugLog.prepend(row);    // newest on top
        if (_debugLog.children.length > 80) _debugLog.lastChild.remove();
    },
    _init() {
        _debugPanel = document.createElement('div');
        _debugPanel.id = 'dbg-panel';
        _debugPanel.style.cssText = `
                position:fixed;bottom:0;right:0;width:420px;height:320px;
                background:#0f172a;border:1px solid #1e293b;border-radius:8px 0 0 0;
                display:flex;flex-direction:column;z-index:9999;font-family:monospace;
                box-shadow:0 -4px 24px rgba(0,0,0,0.5);resize:both;overflow:hidden
            `;
        _debugPanel.innerHTML = `
                <div style="display:flex;align-items:center;justify-content:space-between;padding:6px 10px;background:#1e293b;flex-shrink:0">
                    <span style="color:#86efac;font-weight:700;font-size:11px">🐛 AgriBot Debug  <span id="dbg-env" style="color:#94a3b8;font-weight:400">— ${API_BASE}</span></span>
                    <div style="display:flex;gap:6px;align-items:center">
                        <button id="dbg-clear" style="background:#374151;color:#d1d5db;border:none;padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer">Clear</button>
                        <button id="dbg-close" style="background:#374151;color:#d1d5db;border:none;padding:2px 8px;border-radius:4px;font-size:10px;cursor:pointer">✕</button>
                    </div>
                </div>
                <div id="dbg-log" style="flex:1;overflow-y:auto;padding:6px 10px;scrollbar-width:thin"></div>
            `;
        document.body.appendChild(_debugPanel);
        _debugLog = document.getElementById('dbg-log');
        document.getElementById('dbg-clear').onclick = () => _debugLog.innerHTML = '';
        document.getElementById('dbg-close').onclick = () => { _debugPanel.style.display = 'none'; };
    },
    info(label, data) { console.log(`[INFO]  ${label}`, data ?? ''); DBG._push('info', label, data); },
    warn(label, data) { console.warn(`[WARN]  ${label}`, data ?? ''); DBG._push('warn', label, data); },
    error(label, data) { console.error(`[ERROR] ${label}`, data ?? ''); DBG._push('error', label, data); },
    api(label, data) { console.log(`[API]   ${label}`, data ?? ''); DBG._push('api', label, data); },
    intent(label, data) { console.log(`[INTENT]${label}`, data ?? ''); DBG._push('intent', label, data); },
    data(label, data) { console.log(`[DATA]  ${label}`, data ?? ''); DBG._push('data', label, data); },
};

// Ctrl+Shift+D toggles panel at runtime even without ?debug=1
document.addEventListener('keydown', e => {
    if (e.ctrlKey && e.shiftKey && e.key === 'D') {
        e.preventDefault();
        if (!_debugPanel) DBG._init();
        _debugPanel.style.display = _debugPanel.style.display === 'none' ? 'flex' : 'none';
    }
});

if (DEBUG_ENABLED) { DBG.info('Debug panel active', { api_base: API_BASE }); }

// ──────────────────────────────────────────────
// Pest defaults — prompt user if not yet set
// ──────────────────────────────────────────────
const PEST_DEFAULTS = {
    sowing_date: '10-01-2026',   // DD-MM-YYYY
    crop_slug: 'paddy',
};


function triggerWeatherEffectFromData(records) {
    if (!records || records.length === 0) {
        DBG.info('Weather effect: cleared (no records)');
        clearWeatherEffect();
        return;
    }

    const maxRain = Math.max(...records.map(r => r.Rainfall ?? 0));
    const minTemp = Math.min(...records.map(r => r.Tmin ?? 99));
    const maxTemp = Math.max(...records.map(r => r.Tmax ?? 0));
    const maxWind = Math.max(...records.map(r => r.Wind_Speed ?? 0));

    clearWeatherEffect();

    let effect = 'clear';
    if (maxRain > 0.5) {
        const intensity = Math.min(Math.max(maxRain / 20, 0.15), 1);
        createRainEffect(intensity);
        effect = `rain (${maxRain.toFixed(1)}mm, intensity=${intensity.toFixed(2)})`;
    } else if (minTemp < 15) {
        createColdEffect();
        effect = `cold (Tmin=${minTemp}°C)`;
    } else if (maxTemp > 36) {
        createHeatEffect();
        effect = `heat (Tmax=${maxTemp}°C)`;
    } else if (maxWind > 25) {
        createWindEffect();
        effect = `wind (${maxWind}km/h)`;
    }
    DBG.info('Weather effect triggered', { effect, maxRain, minTemp, maxTemp, maxWind });
}

function clearWeatherEffect() {
    effectOverlay.innerHTML = '';
    effectOverlay.style.background = '';
    effectOverlay.className = 'fixed inset-0 z-50 pointer-events-none overflow-hidden';
}

// Legacy no-op — displayMessage still calls this but effects are now
// driven by triggerWeatherEffectFromData() called from renderWeatherReport()
function triggerWeatherEffect() { }

function createRainEffect(intensity = 0.5) {
    const dropCount = Math.round(40 + intensity * 110);
    for (let i = 0; i < dropCount; i++) {
        const drop = document.createElement('div');
        drop.className = 'rain-drop';
        drop.style.left = Math.random() * 100 + 'vw';
        drop.style.animationDuration = (Math.random() * 0.4 + 0.35) + 's';
        drop.style.animationDelay = Math.random() * 2 + 's';
        drop.style.width = intensity > 0.6 ? '2px' : '1.5px';
        effectOverlay.appendChild(drop);
    }
    effectOverlay.style.background = `rgba(15,23,42,${(0.04 + intensity * 0.08).toFixed(3)})`;
}

function createHeatEffect() {
    const heat = document.createElement('div');
    heat.className = 'heat-wave';
    effectOverlay.appendChild(heat);
    effectOverlay.style.background = 'rgba(251,146,60,0.03)';
}

function createColdEffect() {
    const frost = document.createElement('div');
    frost.className = 'frost-edge';
    effectOverlay.appendChild(frost);
    for (let i = 0; i < 50; i++) {
        const flake = document.createElement('div');
        flake.className = 'snowflake';
        flake.innerHTML = '❄';
        flake.style.left = Math.random() * 100 + 'vw';
        flake.style.opacity = (Math.random() * 0.6 + 0.3).toString();
        flake.style.animationDuration = (Math.random() * 3 + 2) + 's';
        flake.style.animationDelay = Math.random() * 5 + 's';
        effectOverlay.appendChild(flake);
    }
    effectOverlay.style.background = 'rgba(186,230,253,0.04)';
}

function createWindEffect() {
    const wind = document.createElement('div');
    wind.className = 'heat-wave';
    wind.style.background = 'radial-gradient(ellipse at 30% 40%, rgba(148,163,184,0.2) 0%, transparent 65%)';
    effectOverlay.appendChild(wind);
}

// ──────────────────────────────────────────────
// Message / UI helpers
// ──────────────────────────────────────────────
function displayMessage(content, sender, isBot = false) {
    const messageWrapper = document.createElement('div');
    if (isBot) {
        messageWrapper.className = 'flex gap-3 group msg-anim';
        messageWrapper.innerHTML = `
                <div class="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white shrink-0 mt-1">
                    <span class="material-symbols-outlined text-lg fill-1">smart_toy</span>
                </div>
                <div class="space-y-4 flex-1">
                    <div class="bg-slate-50 dark:bg-slate-800/50 p-5 rounded-2xl rounded-tl-none border border-slate-100 dark:border-slate-800 shadow-sm">
                        <div class="leading-relaxed text-slate-800 dark:text-slate-200">${content}</div>
                    </div>
                </div>
            `;
    } else if (sender) {
        messageWrapper.className = 'flex gap-3 flex-row-reverse msg-anim';
        messageWrapper.innerHTML = `
                <div class="w-8 h-8 rounded-full bg-slate-200 overflow-hidden shrink-0 mt-1">
                    <img class="w-full h-full object-cover" src="https://lh3.googleusercontent.com/aida-public/AB6AXuCa8NI8CsYhNzys5loTTFFAyky2OsF8ynjnr4qeLl9L-bM2aKmWcHsy3w95kRJMIqZBt5Dni2QvquaQfHtNyt5TEm4YpICq7VbV8DVAr3VsmV7X8tUg4CFpNKgc4aySVYwwYQrAFS7ZCkjVA9BTas5hQm83FXAYcA4X5Psda5rCkhK49-60l0GmuZGe5c5IG-Z1jXIUNhA6qk_DntnNsP9IBVVcR3SdrU9udfJYDlPI4PGuXXi2oizTNiEkfr0JD5awnCNbB7kCjeEL" alt="User Avatar">
                </div>
                <div class="max-w-[80%]">
                    <div class="bg-primary text-white p-4 rounded-2xl rounded-tr-none shadow-md">
                        <p class="leading-relaxed">${content}</p>
                    </div>
                    <p class="text-[10px] text-slate-400 mt-1 text-right">Delivered now</p>
                </div>
            `;
    } else {
        messageWrapper.className = 'flex gap-3 group msg-anim';
        messageWrapper.innerHTML = `
                <div class="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white shrink-0 mt-1">
                    <span class="material-symbols-outlined text-lg fill-1">smart_toy</span>
                </div>
                <div class="space-y-4 flex-1">
                    <div class="bg-slate-50 dark:bg-slate-800/50 p-5 rounded-2xl rounded-tl-none border border-slate-100 dark:border-slate-800 shadow-sm">
                        <div class="leading-relaxed text-slate-800 dark:text-slate-200">${content}</div>
                    </div>
                </div>
            `;
    }

    chatContainer.appendChild(messageWrapper);
    requestAnimationFrame(() => requestAnimationFrame(() => {
        chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
    }));
    if (isBot) triggerWeatherEffect();
}

function displayTypingIndicator() {
    const typingWrapper = document.createElement('div');
    typingWrapper.className = 'flex gap-4 group typing-indicator-msg';
    typingWrapper.innerHTML = `
            <div class="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white shrink-0 mt-1">
                <span class="material-symbols-outlined text-lg fill-1">smart_toy</span>
            </div>
            <div class="space-y-4 flex-1">
                <div class="bg-slate-50 dark:bg-slate-800/50 p-3 w-20 rounded-2xl rounded-tl-none border border-slate-100 flex justify-center gap-1">
                    <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce"></div>
                    <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce" style="animation-delay: 0.1s"></div>
                    <div class="w-1.5 h-1.5 bg-primary/40 rounded-full animate-bounce" style="animation-delay: 0.2s"></div>
                </div>
            </div>
        `;
    chatContainer.appendChild(typingWrapper);
    requestAnimationFrame(() => requestAnimationFrame(() => {
        chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
    }));
    return typingWrapper;
}

// ──────────────────────────────────────────────
// Form submit
// ──────────────────────────────────────────────
chatForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const query = userInput.value.trim();
    if (!query) return;

    DBG.info('User query', { query });
    displayMessage(query, 'user', false);
    userInput.value = '';
    userInput.style.height = 'auto';

    const typingIndicator = displayTypingIndicator();
    try {
        const url = `${API_BASE}/api/search?q=${encodeURIComponent(query)}`;
        DBG.api('GET /api/search', { url });
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Search failed: ${response.status}`);
        const data = await response.json();
        typingIndicator.remove();

        const docs = data?.response?.docs ?? [];
        const intent = data?.intent ?? {};
        LAST_INTENT = intent;

        DBG.intent('Parsed intent', intent);
        DBG.data('Solr docs', { count: docs.length, first: docs[0] ? { loc: docs[0].village || docs[0].district || docs[0].state, lat: docs[0]._best_lat, lon: docs[0]._best_lon } : null });

        if (docs.length === 0) {
            DBG.warn('No locations found', { query });
            displayMessage("I couldn't find that location. Please try specifying the district or state.", 'bot', true);
            return;
        }

        if (docs.length === 1) {
            routeToDataFetch(docs[0], intent);
        } else {
            DBG.info('Multiple locations — showing chips', { count: docs.length });
            displayLocationSuggestions(docs, intent);
        }
    } catch (error) {
        typingIndicator.remove();
        DBG.error('Search failed', { message: error.message, stack: error.stack });
        displayMessage(`Connection error: ${error.message}. Please ensure the backend is running.`, 'bot', true);
    }
});

userInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        chatForm.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    }
});

// ──────────────────────────────────────────────
// Route: weather vs pest
// ──────────────────────────────────────────────
function routeToDataFetch(location, intent) {
    DBG.info('Routing', { is_pest: intent.is_pest, metric: intent.metric, query_type: intent.query_type });
    if (intent.is_pest) {
        fetchPestData(location, intent);
    } else {
        fetchWeatherData(location, intent);
    }
}

// ──────────────────────────────────────────────
// Location suggestion chips
// ──────────────────────────────────────────────
function displayLocationSuggestions(locations, intent) {
    const chipContainer = document.createElement('div');
    chipContainer.className = 'flex flex-wrap gap-2 mt-4';

    locations.forEach(loc => {
        const chip = document.createElement('button');
        chip.className = 'px-4 py-2 rounded-xl border border-slate-200 bg-white hover:bg-primary/10 hover:border-primary/30 hover:text-primary transition-all text-sm font-medium';
        const label = [loc.village || loc.district || loc.state, loc.state].filter(Boolean).join(', ');
        chip.textContent = label;
        chip.onclick = () => {
            chip.closest('.group').remove();
            routeToDataFetch(loc, LAST_INTENT);
        };
        chipContainer.appendChild(chip);
    });

    const content = `<p>Which location are we analyzing?</p><div id="suggestion-target"></div>`;
    displayMessage(content, 'bot', true);
    document.getElementById('suggestion-target').appendChild(chipContainer);
    document.getElementById('suggestion-target').id = '';
}

// ──────────────────────────────────────────────
// Weather fetch + render
// ──────────────────────────────────────────────
async function fetchWeatherData(location, intent) {
    const typingIndicator = displayTypingIndicator();
    const lat   = location._best_lat;
    const lon   = location._best_lon;
    const label = [location.village || location.district || location.state, location.state]
        .filter(Boolean).join(', ');

    try {
        let url;
        const hourRange = Number(intent.hour_range);
        const isHourly  = hourRange > 0 || intent.query_type === 'hourly';  // ← ADD THIS

        DBG.info("Weather routing decision", {
            query_type:  intent.query_type,
            hour_range:  hourRange,
            isHourly,                                                         // ← ADD THIS
            day_offset:  intent.day_offset,
            target_date: intent.target_date,
            range_days:  intent.range_days,
            condition:   intent.condition,
        });

        if (isHourly) {                                                       // ← CHANGE hourRange > 0 to isHourly
            const params = new URLSearchParams({ lat, lon, hours: hourRange || 24 });
            url = `${API_BASE}/api/weather/hourly?${params}`;
            DBG.api('GET /api/weather/hourly', { url });

        } else {
            // ── Daily ─────────────────────────────────────────────────
            const params = new URLSearchParams({ lat, lon });
            const qt = intent.query_type || 'single';
            params.set("query_type", qt);

            // Queries that use range_days — never send day_offset for these
            const RANGE_TYPES = new Set([
                'range', 'range_few', 'range_week',
                'conditional_rain', 'conditional_condition'
            ]);

            if (RANGE_TYPES.has(qt)) {
                // ── Range / Conditional branch ────────────────────────
                if (intent.range_days && Number(intent.range_days) > 0) {
                    params.set("range_days", Number(intent.range_days));
                }

                // condition only makes sense for conditional_condition, not rain
                if (qt === 'conditional_condition' && intent.condition 
                    && intent.condition !== 'null' && intent.condition !== 'rain') {
                    params.set("condition", intent.condition);
                }
                // Never send day_offset for range/conditional queries

            } else {
                // ── Single day branch ────────────────────────────────
                let offset = null;

                const rawOffset = intent.day_offset;
                const hasOffset = rawOffset !== null
                               && rawOffset !== undefined
                               && rawOffset !== 'null'
                               && rawOffset !== ''
                               && !isNaN(Number(rawOffset));

                if (hasOffset) {
                    offset = Number(rawOffset);
                } else if (intent.target_date 
                        && intent.target_date !== 'null' 
                        && intent.target_date !== '') {
                    const [y, m, d] = String(intent.target_date).split('-').map(Number);
                    const now       = new Date();
                    const todayUTC  = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
                    const targetUTC = Date.UTC(y, m - 1, d);
                    offset          = Math.round((targetUTC - todayUTC) / 86400000);
                    DBG.info('Offset from target_date', { target_date: intent.target_date, offset });
                }

                if (offset !== null && !isNaN(offset)) {
                    params.set("day_offset", offset);
                }
            }

            url = `${API_BASE}/api/weather/daily?${params}`;
            DBG.api('GET /api/weather/daily', { url });
        }

        const response = await fetch(url);
        if (!response.ok) throw new Error(`Weather API error: ${response.status}`);
        const data = await response.json();
        typingIndicator.remove();

        const records = data['Forecast data'] ?? data['weather_data'] ?? [];
        DBG.data('Weather records received', {
            count:   records.length,
            keys:    Object.keys(data),
            first:   records[0] ?? null,
            message: data._message ?? null,
        });

        renderWeatherReport(data, label, intent, isHourly);  // ← now defined ✅

    } catch (error) {
        typingIndicator.remove();
        DBG.error('Weather fetch failed', { message: error.message });
        displayMessage(`Error fetching weather data: ${error.message}`, 'bot', true);
    }
}

function renderWeatherReport(data, location, intent, isHourly = false) {
    const records = data['Forecast data'] ?? data['weather_data'] ?? [];

    if (records.length === 0) {
        displayMessage(data._message || 'No weather data found for the requested period.', 'bot', true);
        triggerWeatherEffectFromData([]);
        return;
    }

    const metric  = intent?.metric ?? 'ALL';
    const isRange = records.length > 1;
    const w       = records[0];

    // ── Hourly table ───────────────────────────────────────────────
    if (isHourly) {
        const fmtHour = (raw) => {
            const d = new Date(raw);
            return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: true })
                 + ' · ' + d.toLocaleDateString('en-IN', { weekday: 'short', day: 'numeric', month: 'short' });
        };

        const HOURLY_COLS = metric === 'Tmin' || metric === 'Tmax' || metric === 'Tavg' ? [
            { head: 'Time',    cell: r => fmtHour(r.Date_time), cls: 'text-slate-500 whitespace-nowrap' },
            { head: 'Temp',    cell: r => `${Math.round(r.Tavg ?? r.Tmax ?? 0)}°C`, cls: 'font-bold text-primary' },
            { head: 'Feels',   cell: r => `${Math.round(r.Tmin ?? 0)}°–${Math.round(r.Tmax ?? 0)}°`, cls: 'text-slate-500' },
            { head: 'Humid',   cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'text-cyan-600' },
        ] : metric === 'Rainfall' ? [
            { head: 'Time',     cell: r => fmtHour(r.Date_time), cls: 'text-slate-500 whitespace-nowrap' },
            { head: 'Rain',     cell: r => `${(r.Rainfall ?? 0).toFixed(1)} mm`, cls: 'font-bold text-blue-600' },
            { head: 'Humidity', cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'text-slate-500' },
        ] : metric === 'Wind_Speed' ? [
            { head: 'Time',  cell: r => fmtHour(r.Date_time), cls: 'text-slate-500 whitespace-nowrap' },
            { head: 'Wind',  cell: r => `${Math.round(r.Wind_Speed ?? 0)} km/h`, cls: 'font-bold text-slate-700' },
            { head: 'Humid', cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'text-slate-500' },
        ] : [
            // ALL — show everything
            { head: 'Time', cell: r => fmtHour(r.Date_time), cls: 'text-slate-500 whitespace-nowrap' },
            { head: 'Temp', cell: r => `${Math.round(r.Tavg ?? 0)}°C`, cls: 'font-bold text-primary' },
            { head: 'Rain', cell: r => `${(r.Rainfall ?? 0).toFixed(1)}mm`, cls: 'text-blue-500' },
            { head: 'Humid',cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'text-cyan-600' },
            { head: 'Wind', cell: r => `${Math.round(r.Wind_Speed ?? 0)}km/h`, cls: 'text-slate-500' },
        ];

        const rows = records.map(r => `
            <tr class="border-t border-slate-100 hover:bg-slate-50 transition-colors">
                ${HOURLY_COLS.map(c => `<td class="py-2.5 pr-4 text-xs ${c.cls}">${c.cell(r)}</td>`).join('')}
            </tr>
        `).join('');

        const hourlyCard = `
            <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
                <div class="flex justify-between items-center mb-3">
                    <h3 class="text-base font-bold text-slate-800">📍 ${location}</h3>
                    <span class="text-xs text-slate-400">⏱ Next ${records.length} hours</span>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-left">
                        <thead>
                            <tr class="text-[9px] uppercase tracking-widest text-slate-400">
                                ${HOURLY_COLS.map(c => `<th class="pb-2 pr-4 font-bold">${c.head}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
            </div>
        `;

        displayMessage(`<div class="space-y-3">${hourlyCard}</div>`, 'bot', true);
        triggerWeatherEffectFromData(records);
        requestAnimationFrame(() => requestAnimationFrame(() => {
            chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
        }));
        return;  // ← exit early, don't fall through to daily renderer
    }

    // ── Date label ────────────────────────────────────────────────
    const fmtDate = (raw) => new Date(raw).toLocaleDateString('en-IN', {
        weekday: 'short', day: 'numeric', month: 'short'
    });
    const dateLabel = new Date(w.Date_time || w.Date).toLocaleDateString('en-IN', {
        weekday: 'long', day: 'numeric', month: 'short', year: 'numeric'
    });

    // ── Hero value — what the user actually asked about ───────────
    const HERO = {
        Tmax: { label: 'Max Temperature', value: () => `${Math.round(w.Tmax ?? 0)}°C`, icon: '🌡', color: '#f97316' },
        Tmin: { label: 'Min Temperature', value: () => `${Math.round(w.Tmin ?? 0)}°C`, icon: '🥶', color: '#3b82f6' },
        Tavg: { label: 'Temperature', value: () => `${Math.round(w.Tavg ?? 0)}°C`, icon: '🌡', color: '#2e7d32' },
        Rainfall: { label: 'Rainfall', value: () => `${(w.Rainfall ?? 0).toFixed(1)} mm`, icon: '🌧', color: '#2563eb' },
        RH: { label: 'Humidity', value: () => `${Math.round(w.RH ?? 0)}%`, icon: '💧', color: '#0891b2' },
        Wind_Speed: { label: 'Wind Speed', value: () => `${Math.round(w.Wind_Speed ?? 0)} km/h`, icon: '💨', color: '#475569' },
    };
    const hero = HERO[metric] ?? null;

    // ── Condition description ──────────────────────────────────────
    const conditionText = (() => {
        const rain = w.Rainfall ?? 0;
        const tmax = w.Tmax ?? 0;
        const tmin = w.Tmin ?? 0;
        const wind = w.Wind_Speed ?? 0;
        const rh = w.RH ?? 0;
        if (rain > 10) return '⛈ Heavy Rain';
        if (rain > 2) return '🌧 Moderate Rain';
        if (rain > 0.5) return '🌦 Light Showers';
        if (tmax > 40) return '🔥 Extreme Heat';
        if (tmax > 36) return '☀️ Very Hot';
        if (tmin < 10) return '🥶 Very Cold';
        if (tmin < 15) return '❄️ Cold';
        if (wind > 40) return '🌪 Strong Winds';
        if (wind > 25) return '💨 Windy';
        if (rh > 85) return '😮‍💨 Very Humid';
        return '☀️ Clear';
    })();

    // ── All metrics as small stat tiles ────────────────────────────
    const allStats = [
        { label: 'Max Temp', val: `${Math.round(w.Tmax ?? 0)}°C`, show: metric !== 'Tmax' },
        { label: 'Min Temp', val: `${Math.round(w.Tmin ?? 0)}°C`, show: metric !== 'Tmin' },
        { label: 'Humidity', val: `${Math.round(w.RH ?? 0)}%`, show: metric !== 'RH' },
        { label: 'Rainfall', val: `${(w.Rainfall ?? 0).toFixed(1)} mm`, show: metric !== 'Rainfall' },
        { label: 'Wind Speed', val: `${Math.round(w.Wind_Speed ?? 0)} km/h`, show: metric !== 'Wind_Speed' },
    ].filter(s => s.show);    // hide the one already shown as hero

    // When user asked "ALL" show every stat, no hero section
    const showHero = hero !== null && metric !== 'ALL';

    // ── Single-day card ────────────────────────────────────────────
    const gridCols = metric === 'ALL' ? 3 : 2;
    const statItems = metric === 'ALL'
        ? [
            { label: 'Max', val: `${Math.round(w.Tmax ?? 0)}°C` },
            { label: 'Min', val: `${Math.round(w.Tmin ?? 0)}°C` },
            { label: 'Humidity', val: `${Math.round(w.RH ?? 0)}%` },
            { label: 'Rainfall', val: `${(w.Rainfall ?? 0).toFixed(1)} mm` },
            { label: 'Wind', val: `${Math.round(w.Wind_Speed ?? 0)} km/h` },
        ]
        : allStats;   // already excludes the hero metric

    const singleCard = `
            <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
                <!-- Header -->
                <div class="flex justify-between items-start">
                    <div>
                        <p class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">${dateLabel}</p>
                        <h3 class="text-base font-bold text-slate-800 mt-0.5">📍 ${location}</h3>
                    </div>
                    <span class="text-xs font-semibold px-2.5 py-1 bg-slate-100 text-slate-600 rounded-full">${conditionText}</span>
                </div>

                ${showHero ? `
                <!-- Hero metric (specific query) -->
                <div class="mt-4 py-4 border-y border-slate-100 flex items-center gap-4">
                    <span style="font-size:2.25rem;line-height:1">${hero.icon}</span>
                    <div>
                        <p class="text-[10px] text-slate-400 uppercase font-bold tracking-wider">${hero.label}</p>
                        <p style="font-size:2.5rem;font-weight:900;line-height:1;margin-top:0.25rem;color:${hero.color}">${hero.value()}</p>
                    </div>
                </div>` : `
                <!-- ALL: avg temp prominent -->
                <div class="mt-4 flex items-end gap-2">
                    <span class="font-black text-primary leading-none" style="font-size:3rem">${Math.round(w.Tavg ?? 0)}°C</span>
                    <span class="text-sm text-slate-400 mb-1">avg</span>
                </div>`}

                <!-- Stat tiles — inline grid so Tailwind CDN doesn't need to generate dynamic class -->
                <div style="display:grid;grid-template-columns:repeat(${gridCols},1fr);gap:0.5rem;margin-top:1rem">
                    ${statItems.map(s => `
                        <div style="padding:0.625rem;background:#f8fafc;border:1px solid #f1f5f9;border-radius:0.75rem">
                            <p style="font-size:0.6rem;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.06em">${s.label}</p>
                            <p style="font-size:0.875rem;font-weight:700;color:#334155;margin-top:0.125rem">${s.val}</p>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

    // ── Multi-day range table — columns driven by metric ──────────
    const rangeCard = (() => {
        // Decide which columns to show
        const cols = metric === 'Wind_Speed' ? [
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Wind Speed', cell: r => `${Math.round(r.Wind_Speed ?? 0)} km/h`, cls: 'font-bold text-slate-700' },
            { head: 'Condition', cell: r => (r.Rainfall ?? 0) > 0.5 ? '🌧' : (r.Wind_Speed ?? 0) > 25 ? '💨' : '☀️', cls: '' },
        ] : metric === 'Rainfall' ? [
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Rainfall', cell: r => `${(r.Rainfall ?? 0).toFixed(1)} mm`, cls: 'font-bold text-blue-600' },
            { head: 'Humidity', cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'text-slate-600' },
        ] : metric === 'RH' ? [
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Humidity', cell: r => `${Math.round(r.RH ?? 0)}%`, cls: 'font-bold text-cyan-600' },
            { head: 'Rainfall', cell: r => `${(r.Rainfall ?? 0).toFixed(1)} mm`, cls: 'text-slate-600' },
        ] : metric === 'Tmax' ? [
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Max Temp', cell: r => `${Math.round(r.Tmax ?? 0)}°C`, cls: 'font-bold text-orange-500' },
            { head: 'Min Temp', cell: r => `${Math.round(r.Tmin ?? 0)}°C`, cls: 'text-slate-500' },
        ] : metric === 'Tmin' ? [
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Min Temp', cell: r => `${Math.round(r.Tmin ?? 0)}°C`, cls: 'font-bold text-blue-500' },
            { head: 'Max Temp', cell: r => `${Math.round(r.Tmax ?? 0)}°C`, cls: 'text-slate-500' },
        ] : [
            // ALL / Tavg — full table
            { head: 'Date', cell: r => fmtDate(r.Date_time || r.Date), cls: 'text-slate-500' },
            { head: 'Avg', cell: r => `${Math.round(r.Tavg ?? 0)}°C`, cls: 'font-bold' },
            { head: 'Max', cell: r => `${Math.round(r.Tmax ?? 0)}°C`, cls: 'text-orange-500' },
            { head: 'Rain', cell: r => `${(r.Rainfall ?? 0).toFixed(1)}mm`, cls: 'text-blue-500' },
            { head: 'Wind', cell: r => `${Math.round(r.Wind_Speed ?? 0)}km/h`, cls: 'text-slate-500' },
        ];

        const rows = records.map(r => `
                <tr class="border-t border-slate-100 hover:bg-slate-50 transition-colors">
                    ${cols.map(c => `<td class="py-2.5 pr-3 text-xs ${c.cls}">${c.cell(r)}</td>`).join('')}
                </tr>
            `).join('');

        return `
                <div class="bg-white border border-slate-200 rounded-2xl p-5 shadow-sm">
                    <div class="flex justify-between items-center mb-3">
                        <h3 class="text-base font-bold text-slate-800">📍 ${location}</h3>
                        <span class="text-xs text-slate-400">${records.length}-day forecast</span>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left">
                            <thead>
                                <tr class="text-[9px] uppercase tracking-widest text-slate-400">
                                    ${cols.map(c => `<th class="pb-2 pr-3 font-bold">${c.head}</th>`).join('')}
                                </tr>
                            </thead>
                            <tbody>${rows}</tbody>
                        </table>
                    </div>
                </div>
            `;
    })();

    const msgHtml = `
            <div class="space-y-3">
                ${data._message ? `<div class="p-3 bg-emerald-50 border-l-4 border-emerald-400 rounded-r-lg text-sm text-emerald-800">${data._message}</div>` : ''}
                ${isRange ? rangeCard : singleCard}
            </div>
        `;

    displayMessage(msgHtml, 'bot', true);
    DBG.info('Weather card rendered', { metric, isRange, showHero, conditionText });
    triggerWeatherEffectFromData(records);

    // Force scroll after card renders (cards can be tall)
    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            chatScrollContainer.scrollTop = chatScrollContainer.scrollHeight;
        });
    });
}

// ──────────────────────────────────────────────
// Pest fetch + render
// ──────────────────────────────────────────────
async function fetchPestData(location, intent) {
    // 1. Check for missing slots BEFORE doing anything else
    if (intent.is_pest && intent.missing_slots && intent.missing_slots.length > 0) {
        displayMessage(`I'd love to help with pest info! Could you please tell me your ${intent.missing_slots.join(" and ")}?`);
        return;
    }

    // 2. Only if slots are filled, proceed to fetch
    const typingIndicator = displayTypingIndicator();
    const lat = location._best_lat;
    const lon = location._best_lon;
    const state = location.state?.[0];
    const isNextWeek = intent.query_type === 'range_week' || (intent.day_offset ?? 0) >= 7;

    try {
        const params = new URLSearchParams({ lat, lon, is_next_week: String(isNextWeek) });

        // Use the extracted intent data if available, otherwise fallback to defaults
        const body = {
            sowing_date: intent.sowing_date ?? PEST_DEFAULTS.sowing_date,
            crop_slug: intent.crop_slug ?? PEST_DEFAULTS.crop_slug,
            state_name: state,
        };

        const url = `${API_BASE}/api/pest/infestation?${params}`;

        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail ?? `API error ${response.status}`);
        }

        const data = await response.json();
        typingIndicator.remove();
        renderPestReport(data, state, isNextWeek);

    } catch (error) {
        typingIndicator.remove();
        displayMessage(`Error fetching pest infestation data: ${error.message}`, 'bot', true);
    }
}

function renderPestReport(data, state, isNextWeek) {
    if (!data.status || !Array.isArray(data.data) || data.data.length === 0) {
        DBG.warn('Pest data empty or invalid', { status: data.status, dataLen: data.data?.length });
        displayMessage('No pest infestation data available for this region.', 'bot', true);
        return;
    }

    const weekKey = isNextWeek ? 'next_week' : 'current_week';
    const weekLabel = isNextWeek ? 'Next Week' : 'This Week';

    let activeRisks, clearCount;
    if (data._summary) {
        activeRisks = data._summary.active_pests;
        clearCount = data._summary.clear_count;
        DBG.data('Using backend _summary', { active: activeRisks.length, clear: clearCount });
    } else {
        activeRisks = data.data
            .map(r => ({
                name: r.infestation_name,
                probability: (r.chances_percentage ?? {})[weekKey] ?? 0,
                risk_level: riskLabel((r.chances_percentage ?? {})[weekKey] ?? 0),
            }))
            .filter(r => r.probability > 0)
            .sort((a, b) => b.probability - a.probability);
        clearCount = data.data.length - activeRisks.length;
    }

    const RISK_STYLE = {
        High: 'bg-red-100 text-red-700',
        Medium: 'bg-amber-100 text-amber-700',
        Low: 'bg-yellow-100 text-yellow-700',
    };

    let listHtml = '';
    if (activeRisks.length > 0) {
        listHtml = activeRisks.map(r => `
                <div class="flex items-center justify-between p-3 bg-slate-50 rounded-lg border border-slate-100">
                    <span class="text-sm font-semibold text-slate-700">${r.name}</span>
                    <div class="flex items-center gap-2">
                        <div class="w-20 bg-slate-200 rounded-full h-1.5">
                            <div class="h-1.5 rounded-full ${r.risk_level === 'High' ? 'bg-red-500' : r.risk_level === 'Medium' ? 'bg-amber-500' : 'bg-yellow-400'}"
                                style="width:${Math.min(r.probability, 100)}%"></div>
                        </div>
                        <span class="text-xs font-bold px-2 py-1 rounded-full ${RISK_STYLE[r.risk_level] ?? 'bg-slate-100 text-slate-600'}">
                            ${r.probability}% · ${r.risk_level}
                        </span>
                    </div>
                </div>
            `).join('');
    } else {
        listHtml = `<p class="text-sm text-slate-500 italic">✅ No significant infestation risks detected ${weekLabel.toLowerCase()}.</p>`;
    }

    const metaHtml = data.request_location
        ? `<p class="text-xs text-slate-400 mt-1">${data.request_location.district ?? ''}, ${data.request_location.state ?? ''}</p>`
        : '';

    const cardHtml = `
            <div class="space-y-4">
                <div class="bg-white border border-slate-200 rounded-xl p-6 shadow-sm">
                    <div class="flex items-start justify-between mb-4">
                        <div class="flex items-center gap-2">
                            <span class="material-symbols-outlined text-amber-500">bug_report</span>
                            <div>
                                <h3 class="font-bold text-lg leading-tight">Pest Report · ${state}</h3>
                                ${metaHtml}
                            </div>
                        </div>
                        <span class="text-xs font-semibold px-2 py-1 bg-slate-100 text-slate-600 rounded-lg">${weekLabel}</span>
                    </div>

                    <div class="space-y-2">${listHtml}</div>

                    ${activeRisks.length > 0 ? `
                    <div class="mt-4 p-4 bg-amber-50 border-l-4 border-amber-400 rounded-r-lg text-xs text-amber-800">
                        <strong>⚠ Precaution:</strong> High risk levels indicate immediate action may be required. 
                        Consult your local agricultural advisor.
                    </div>` : ''}

                    <div class="mt-3 text-[10px] text-slate-400 text-right">
                        ${clearCount} pest${clearCount !== 1 ? 's' : ''} at 0% risk · Crop: ${PEST_DEFAULTS.crop_slug}
                    </div>
                </div>
            </div>
        `;
    displayMessage(cardHtml, 'bot', true);
    clearWeatherEffect();   // pest report = no weather effect
}

function riskLabel(probability) {
    if (probability >= 75) return 'High';
    if (probability >= 40) return 'Medium';
    if (probability > 0) return 'Low';
    return 'None';
}

// Expose for clearChat() in HTML
function showGreeting() {
    displayMessage(
        "Hello! I'm <strong>Niruthi Bot</strong> 🌱<br>" +
        "<span class='text-sm text-slate-500 font-normal'>Ask me about weather forecasts or pest infestation risks for any region in India.</span>",
        'bot', true
    );
}

window.onload = showGreeting;