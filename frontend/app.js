const chatContainer = document.getElementById('chat-container');
const chatForm = document.getElementById('chat-form');
const userInput = document.getElementById('user-input');

const API_BASE_URL = 'http://localhost:8010/api';

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

        const intent = searchData.intent || { metric: null };

        if (searchData.response && searchData.response.docs.length > 0) {
            const docs = searchData.response.docs;
            if (docs.length === 1) {
                handleLocationSelection(docs[0], intent);
            } else {
                handleMultipleLocations(docs, intent);
            }
        } else {
            appendMessage('bot', `Sorry, I couldn't find any location matching "${query}". Please try again.`);
        }
    } catch (error) {
        console.error(error);
        removeLoading(loadingId);
        appendMessage('bot', "Oops! Something went wrong while searching for the location. Please try again later.");
    }
});

async function searchLocation(query) {
    const response = await fetch(`${API_BASE_URL}/search?q=${encodeURIComponent(query)}`);
    if (!response.ok) throw new Error('Search failed');
    return await response.json();
}

async function fetchWeather(lat, lon) {
    const response = await fetch(`${API_BASE_URL}/weather/daily?lat=${lat}&lon=${lon}`);
    if (!response.ok) throw new Error('Daily weather fetch failed');
    return await response.json();
}

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

function handleMultipleLocations(docs, intent) {
    const content = document.createElement('div');
    content.innerHTML = `<p>I found multiple locations. Which one do you mean?</p>`;
    
    const suggestionChips = document.createElement('div');
    suggestionChips.className = 'suggestion-chips';
    
    docs.forEach(doc => {
        const chip = document.createElement('div');
        chip.className = 'chip';
        const label = `${doc.village ? doc.village[0] + ', ' : ''}${doc.district[0]}, ${doc.state[0]}`;
        chip.textContent = label;
        chip.onclick = () => {
            appendMessage('user', label);
            handleLocationSelection(doc, intent);
        };
        suggestionChips.appendChild(chip);
    });
    
    const msg = appendMessage('bot', '', true);
    msg.querySelector('.message-content').appendChild(content);
    msg.querySelector('.message-content').appendChild(suggestionChips);
}

async function handleLocationSelection(doc, intent) {
    const lat = doc.village_latitude ? doc.village_latitude[0] : 0;
    const lon = doc.village_longitude ? doc.village_longitude[0] : 0;
    
    const loadingId = appendLoading();
    
    try {
        const weatherData = await fetchWeather(lat, lon);
        removeLoading(loadingId);
        
        if (weatherData && weatherData["Forecast data"] && weatherData["Forecast data"].length > 0) {
            displayWeatherCard(doc, weatherData["Forecast data"][0], intent);
        } else {
            appendMessage('bot', "I couldn't retrieve the weather data for that location.");
        }
    } catch (error) {
        console.error(error);
        removeLoading(loadingId);
        appendMessage('bot', "Failed to fetch weather data. Please try again.");
    }
}

function displayWeatherCard(doc, data, intent) {
    const locationName = `${doc.village ? doc.village[0] + ', ' : ''}${doc.district[0]}, ${doc.state[0]}`;
    const metric = intent.metric;
    
    let highlightText = "";
    if (metric) {
        const val = data[metric];
        const label = metric === 'RH' ? 'Humidity' : metric === 'Wind_Speed' ? 'Wind Speed' : metric === 'Rainfall' ? 'Rainfall' : 'Temperature';
        const unit = metric === 'RH' ? '%' : metric === 'Wind_Speed' ? ' km/h' : metric === 'Rainfall' ? ' mm' : '°C';
        highlightText = `<p class="highlight-answer">The ${label} in ${doc.district[0]} is <strong>${val}${unit}</strong>.</p>`;
    }

    const cardHtml = `
        <div class="weather-card">
            <div class="weather-header">
                <div class="location-name">${locationName}</div>
                <div class="date">${new Date(data.Date_time).toLocaleDateString()}</div>
            </div>
            ${highlightText}
            <div class="weather-main">
                <div class="temp-large ${metric === 'Tmax' ? 'highlight' : ''}">${Math.round(data.Tmax)}°C</div>
                <div class="weather-summary">
                    <p>H: ${Math.round(data.Tmax)}° L: ${Math.round(data.Tmin)}°</p>
                    <p>${data.Rainfall > 0 ? 'Rainy' : 'Clear'}</p>
                </div>
            </div>
            <div class="weather-grid">
                <div class="weather-item ${metric === 'RH' ? 'highlight' : ''}">
                    <span class="item-label">Humidity</span>
                    <span class="item-value">${Math.round(data.RH)}%</span>
                </div>
                <div class="weather-item ${metric === 'Wind_Speed' ? 'highlight' : ''}">
                    <span class="item-label">Wind</span>
                    <span class="item-value">${data.Wind_Speed} km/h</span>
                </div>
                <div class="weather-item ${metric === 'Rainfall' ? 'highlight' : ''}">
                    <span class="item-label">Rainfall</span>
                    <span class="item-value">${data.Rainfall} mm</span>
                </div>
                <div class="weather-item ${metric === 'Tavg' ? 'highlight' : ''}">
                    <span class="item-label">Avg Temp</span>
                    <span class="item-value">${Math.round(data.Tavg)}°C</span>
                </div>
            </div>
        </div>
    `;
    
    appendMessage('bot', cardHtml, true);
}
