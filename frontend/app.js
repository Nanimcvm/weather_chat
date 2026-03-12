const chatContainer = document.getElementById('chat-container');
const chatForm      = document.getElementById('chat-form');
const userInput     = document.getElementById('user-input');

const API_BASE_URL = 'http://localhost:8010/api';   // ← single place to change port

// ─────────────────────────────────────────────
// Submit handler
// ─────────────────────────────────────────────
chatForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const query = userInput.value.trim();
    if (!query) return;

    appendMessage('user', query);
    userInput.value = '';

    const loadingId = appendLoading();

    try {
        const searchData = await searchLocation(query);
        removeLoading(loadingId);

        // intent now includes: { location, metric, query_type, day_offset, target_date, range_days, condition }
        const intent = searchData.intent || {};

        // Apply defaults — ALL fields must be safe to use downstream
        intent.metric      = intent.metric      || 'ALL';
        intent.query_type  = intent.query_type  || 'single';
        intent.day_offset  = intent.day_offset  ?? 0;
        intent.target_date = intent.target_date || todayIST();
        intent.range_days  = intent.range_days  || null;
        intent.condition   = intent.condition   || null;

        // Log the full intent so we can debug routing issues

        if (searchData.response && searchData.response.docs.length > 0) {
            const docs = searchData.response.docs;
            if (docs.length === 1) {
                handleLocationSelection(docs[0], intent);
            } else {
                handleMultipleLocations(docs, intent);
            }
        } else {
            appendMessage('bot', `Sorry, I couldn't find any location matching "${query}". Please try a different city or district name.`);
        }
    } catch (error) {
        console.error('[SEARCH ERROR]', error);
        removeLoading(loadingId);
        appendMessage('bot', "Oops! Something went wrong while searching. Please try again.");
    }
});

// ─────────────────────────────────────────────
// API calls
// ─────────────────────────────────────────────
async function searchLocation(query) {
    const res = await fetch(`${API_BASE_URL}/search?q=${encodeURIComponent(query)}`);
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || 'Search failed');
    }
    return res.json();
}

async function fetchWeather(lat, lon, intent) {
    const params = new URLSearchParams({ lat, lon });

    const qt = intent.query_type || 'single';
    params.set('query_type', qt);


    if (qt === 'single') {
        // Single day — must send target_date and day_offset
        params.set('target_date', intent.target_date);
        params.set('day_offset',  intent.day_offset ?? 0);

    } else if (['range', 'range_few', 'range_week'].includes(qt)) {
        // Range — send number of days
        if (intent.range_days) params.set('range_days', intent.range_days);

    } else if (qt === 'conditional_rain') {
        // Always send target_date + day_offset so backend can resolve "tomorrow" etc.
        params.set('target_date', intent.target_date);
        params.set('day_offset',  intent.day_offset ?? 0);
        // range_days=1 means "specific day", larger means "next N days window"
        if (intent.range_days) params.set('range_days', intent.range_days);

    } else if (qt === 'conditional_condition') {
        if (intent.condition)  params.set('condition',  intent.condition);
        if (intent.range_days) params.set('range_days', intent.range_days);
    }

    const res = await fetch(`${API_BASE_URL}/weather/daily?${params}`);
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || 'Daily weather fetch failed');
    }
    return res.json();
}

// ─────────────────────────────────────────────
// Location selection
// ─────────────────────────────────────────────
async function handleLocationSelection(doc, intent) {
    // ✅ Use pre-resolved coords from backend (most precise available: village > district > state)
    const lat = doc._best_lat;
    const lon = doc._best_lon;

    if (!lat || !lon) {
        appendMessage('bot', "Sorry, I couldn't determine coordinates for that location. The location data may be incomplete.");
        console.warn('[COORDS] Missing _best_lat/_best_lon in doc:', doc);
        return;
    }


    const loadingId = appendLoading();

    try {
        const weatherData = await fetchWeather(lat, lon, intent);
        removeLoading(loadingId);

        // Backend returns { "Forecast data": [...], _message?: "..." }
        const records  = weatherData["Forecast data"] ?? [];
        const msgNote  = weatherData["_message"];        // set when past date requested

        if (msgNote) {
            appendMessage('bot', `ℹ️ ${msgNote}`);
            return;
        }

        if (records.length === 0) {
            appendMessage('bot', `No forecast data found for the requested period.`);
            return;
        }

        // Route to correct display based on query type
        const qt = intent.query_type || 'single';
        if (qt === 'single') {
            displayWeatherCard(doc, records[0], intent);
        } else if (['range','range_few','range_week'].includes(qt)) {
            displayMultiDayForecast(doc, records, intent);
        } else if (qt === 'conditional_rain') {
            displayRainyDays(doc, records, intent);
        } else if (qt === 'conditional_condition') {
            displayConditionDays(doc, records, intent);
        } else {
            displayWeatherCard(doc, records[0], intent);
        }

    } catch (error) {
        console.error('[WEATHER ERROR]', error);
        removeLoading(loadingId);
        appendMessage('bot', `Failed to fetch weather: ${error.message}`);
    }
}

function handleMultipleLocations(docs, intent) {
    const content = document.createElement('div');
    content.innerHTML = `<p>I found multiple locations. Which one do you mean?</p>`;

    const chips = document.createElement('div');
    chips.className = 'suggestion-chips';

    docs.forEach(doc => {
        const chip  = document.createElement('div');
        chip.className = 'chip';
        const label = buildLocationLabel(doc);
        chip.textContent = label;
        chip.onclick = () => {
            appendMessage('user', label);
            handleLocationSelection(doc, intent);
        };
        chips.appendChild(chip);
    });

    const msg = appendMessage('bot', '', true);
    msg.querySelector('.message-content').appendChild(content);
    msg.querySelector('.message-content').appendChild(chips);
}

// ─────────────────────────────────────────────
// Multi-day / conditional display functions
// ─────────────────────────────────────────────

function displayMultiDayForecast(doc, records, intent) {
    const locationName = buildLocationLabel(doc);
    const n = records.length;
    const metric = intent.metric;

    const rows = records.map(r => {
        const d     = new Date(r.Date_time);
        const label = d.toLocaleDateString('en-IN', { weekday:'short', day:'numeric', month:'short' });
        const cond  = weatherCondition(r);
        const highlighted = (metric && metric !== 'ALL') ? `<strong>${metricMeta(metric).label}: ${formatVal(r[metric], metric)}${metricMeta(metric).unit}</strong>` : '';
        return `
            <tr>
                <td>${label}</td>
                <td>${Math.round(r.Tmax)}° / ${Math.round(r.Tmin)}°</td>
                <td>${r.Rainfall.toFixed(1)} mm</td>
                <td>${Math.round(r.RH)}%</td>
                <td>${cond}</td>
                ${highlighted ? `<td>${highlighted}</td>` : ''}
            </tr>`;
    }).join('');

    const html = `
        <div class="weather-card">
            <div class="weather-header">
                <div class="location-name">${locationName}</div>
                <div class="date">${n}-Day Forecast</div>
            </div>
            <table class="forecast-table">
                <thead>
                    <tr>
                        <th>Date</th><th>High / Low</th><th>Rain</th><th>Humidity</th><th>Condition</th>
                        ${(metric && metric !== 'ALL') ? `<th>${metricMeta(metric).label}</th>` : ''}
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>`;
    appendMessage('bot', html, true);
}

function displayRainyDays(doc, records, intent) {
    const locationName = buildLocationLabel(doc);
    const isSingleDay  = intent.range_days === 1;

    if (records.length === 0) {
        // This path is hit from _message handling, but keep as fallback
        appendMessage('bot', `☀️ No rainfall expected in ${locationName}.`);
        return;
    }

    // Single-day rain check: "will it rain tomorrow in delhi?"
    if (isSingleDay) {
        const r         = records[0];
        const d         = new Date(r.Date_time);
        const dateLabel = formatDate(r.Date_time, intent);
        const rainfall  = r.Rainfall ? r.Rainfall.toFixed(1) : '0.0';
        const html = `
            <div class="weather-card">
                <div class="weather-header">
                    <div class="location-name">${locationName}</div>
                    <div class="date">${dateLabel}</div>
                </div>
                <div class="weather-main" style="padding:16px 20px;">
                    <div style="font-size:2rem;margin-bottom:8px;">🌧️</div>
                    <div style="font-size:1.1rem;font-weight:600;color:#2563eb;margin-bottom:4px;">
                        Yes — rainfall expected
                    </div>
                    <div style="font-size:0.95rem;color:#555;">
                        Estimated rainfall: <strong>${rainfall} mm</strong> &nbsp;|&nbsp; Humidity: <strong>${Math.round(r.RH)}%</strong>
                    </div>
                </div>
            </div>`;
        appendMessage('bot', html, true);
        return;
    }

    // Multi-day: show table of all rainy days
    const rows = records.map(r => {
        const label = new Date(r.Date_time).toLocaleDateString('en-IN', { weekday:'long', day:'numeric', month:'short' });
        return `<tr><td>${label}</td><td>${r.Rainfall.toFixed(1)} mm</td><td>${Math.round(r.RH)}%</td><td>${weatherCondition(r)}</td></tr>`;
    }).join('');

    const windowDesc = intent.range_days ? `Next ${intent.range_days} Days` : 'Forecast Period';
    const html = `
        <div class="weather-card">
            <div class="weather-header">
                <div class="location-name">${locationName}</div>
                <div class="date">🌧 Rainy Days — ${windowDesc}</div>
            </div>
            <p style="padding:8px 12px;color:#555;">Found <strong>${records.length}</strong> day(s) with expected rainfall:</p>
            <table class="forecast-table">
                <thead><tr><th>Date</th><th>Rainfall</th><th>Humidity</th><th>Condition</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>`;
    appendMessage('bot', html, true);
}

function displayConditionDays(doc, records, intent) {
    const locationName = buildLocationLabel(doc);
    const cond = intent.condition || 'matching';

    if (records.length === 0) {
        appendMessage('bot', `No '${cond}' weather expected in ${locationName} in the coming days.`);
        return;
    }

    const rows = records.map(r => {
        const d     = new Date(r.Date_time);
        const label = d.toLocaleDateString('en-IN', { weekday:'long', day:'numeric', month:'short' });
        return `<tr><td>${label}</td><td>${Math.round(r.Tmax)}° / ${Math.round(r.Tmin)}°</td><td>${r.Wind_Speed.toFixed(1)} km/h</td><td>${Math.round(r.RH)}%</td></tr>`;
    }).join('');

    const html = `
        <div class="weather-card">
            <div class="weather-header">
                <div class="location-name">${locationName}</div>
                <div class="date">Days with '${cond}' conditions</div>
            </div>
            <table class="forecast-table">
                <thead><tr><th>Date</th><th>High / Low</th><th>Wind</th><th>Humidity</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>`;
    appendMessage('bot', html, true);
}

// ─────────────────────────────────────────────
// Weather card rendering
// ─────────────────────────────────────────────
function displayWeatherCard(doc, data, intent) {
    const locationName = buildLocationLabel(doc);
    const metric       = intent.metric;   // "ALL", "Tmax", "Tmin", "Tavg", "RH", "Wind_Speed", "Rainfall"
    const dateLabel    = formatDate(data.Date_time, intent);

    // ✅ FIX: metric is "ALL" (not null) when user didn't specify one
    let highlightText = '';
    if (metric && metric !== 'ALL') {
        const { label, unit } = metricMeta(metric);
        const val = data[metric];
        if (val !== undefined) {
            highlightText = `<p class="highlight-answer">The ${label} in <strong>${doc.district?.[0] ?? locationName}</strong> on ${dateLabel} is <strong>${formatVal(val, metric)}${unit}</strong>.</p>`;
        }
    }

    const cardHtml = `
        <div class="weather-card">
            <div class="weather-header">
                <div class="location-name">${locationName}</div>
                <div class="date">${dateLabel}</div>
            </div>
            ${highlightText}
            <div class="weather-main">
                <div class="temp-large ${metric === 'Tmax' ? 'highlight' : ''}">${Math.round(data.Tmax)}°C</div>
                <div class="weather-summary">
                    <p>H: ${Math.round(data.Tmax)}°  L: ${Math.round(data.Tmin)}°</p>
                    <p>${weatherCondition(data)}</p>
                </div>
            </div>
            <div class="weather-grid">
                <div class="weather-item ${metric === 'Tavg' ? 'highlight' : ''}">
                    <span class="item-label">Avg Temp</span>
                    <span class="item-value">${Math.round(data.Tavg)}°C</span>
                </div>
                <div class="weather-item ${metric === 'RH' ? 'highlight' : ''}">
                    <span class="item-label">Humidity</span>
                    <span class="item-value">${Math.round(data.RH)}%</span>
                </div>
                <div class="weather-item ${metric === 'Wind_Speed' ? 'highlight' : ''}">
                    <span class="item-label">Wind</span>
                    <span class="item-value">${data.Wind_Speed.toFixed(1)} km/h</span>
                </div>
                <div class="weather-item ${metric === 'Rainfall' ? 'highlight' : ''}">
                    <span class="item-label">Rainfall</span>
                    <span class="item-value">${data.Rainfall.toFixed(1)} mm</span>
                </div>
            </div>
        </div>
    `;

    appendMessage('bot', cardHtml, true);
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

/** Build a human-readable location label from a Solr doc */
function buildLocationLabel(doc) {
    const parts = [];
    if (doc.village?.[0])  parts.push(doc.village[0]);
    if (doc.district?.[0]) parts.push(doc.district[0]);
    if (doc.state?.[0])    parts.push(doc.state[0]);
    return parts.join(', ');
}

/** Format a Date_time string nicely, with a relative label if today/tomorrow */
function formatDate(dateTimeStr, intent) {
    const d      = new Date(dateTimeStr);
    const offset = intent.day_offset ?? 0;

    const base = d.toLocaleDateString('en-IN', {
        weekday: 'short', day: 'numeric', month: 'short', year: 'numeric'
    });

    if (offset === 0)  return `Today (${base})`;
    if (offset === 1)  return `Tomorrow (${base})`;
    if (offset === -1) return `Yesterday (${base})`;
    return base;
}

/** Return today's date as YYYY-MM-DD in IST */
function todayIST() {
    const now = new Date();
    // IST = UTC + 5:30
    const ist = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
    return ist.toISOString().slice(0, 10);
}

/** Human-readable label + unit for a metric key */
function metricMeta(metric) {
    const map = {
        Tmax:       { label: 'Maximum Temperature', unit: '°C' },
        Tmin:       { label: 'Minimum Temperature', unit: '°C' },
        Tavg:       { label: 'Average Temperature', unit: '°C' },
        RH:         { label: 'Humidity',             unit: '%'  },
        Wind_Speed: { label: 'Wind Speed',           unit: ' km/h' },
        Rainfall:   { label: 'Rainfall',             unit: ' mm'   },
    };
    return map[metric] || { label: metric, unit: '' };
}

/** Format a numeric value sensibly */
function formatVal(val, metric) {
    if (['Tmax','Tmin','Tavg'].includes(metric)) return Math.round(val);
    return parseFloat(val).toFixed(1);
}

/** Simple condition string based on data */
function weatherCondition(data) {
    if (data.Rainfall > 10) return '🌧 Heavy Rain';
    if (data.Rainfall > 0)  return '🌦 Light Rain';
    if (data.RH > 80)       return '🌫 Humid';
    if (data.Tmax > 40)     return '☀️ Very Hot';
    if (data.Tmax > 30)     return '🌤 Warm';
    return '🌥 Partly Cloudy';
}

// ─────────────────────────────────────────────
// DOM utilities
// ─────────────────────────────────────────────
function appendMessage(role, content, isHtml = false) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${role}-message`;

    const avatar = document.createElement('div');
    avatar.className = 'avatar';
    avatar.textContent = role === 'bot' ? '🤖' : '👤';

    const messageContent = document.createElement('div');
    messageContent.className = 'message-content';

    if (isHtml) {
        messageContent.innerHTML = content;
    } else {
        messageContent.textContent = content;
    }

    messageDiv.appendChild(avatar);
    messageDiv.appendChild(messageContent);
    chatContainer.appendChild(messageDiv);

    scrollToBottom();
    return messageDiv;
}

function appendLoading() {
    const id = 'loading-' + Date.now();
    const loadingDiv = document.createElement('div');
    loadingDiv.className = 'message bot-message';
    loadingDiv.id = id;
    loadingDiv.innerHTML = `
        <div class="avatar">🤖</div>
        <div class="message-content">
            <div class="typing">
                <span></span><span></span><span></span>
            </div>
        </div>
    `;
    chatContainer.appendChild(loadingDiv);
    scrollToBottom();
    return id;
}

function removeLoading(id) {
    const el = document.getElementById(id);
    if (el) el.remove();
}

function scrollToBottom() {
    chatContainer.scrollTop = chatContainer.scrollHeight;
}