document.addEventListener("DOMContentLoaded", () => {
    const input = document.querySelector("#url-input");
    const button = document.querySelector("#scrape-btn");
    const statusDiv = document.querySelector("#status");
    const resultsContainer = document.querySelector("#results");
    const historyContainer = document.querySelector("#history-list");

    // 1. Load History on Page Start
    loadHistory();

    button.addEventListener("click", async () => {
        const url = input.value.trim();
        
        if (!url) {
            alert("Please enter a URL");
            return;
        }

        // Reset UI
        statusDiv.textContent = "Scraping deep data... (this may take a few seconds)";
        statusDiv.className = "";
        resultsContainer.innerHTML = '<div style="text-align:center; color:#666;">Scanning target...</div>';
        button.disabled = true;

        try {
            const response = await fetch("/scrape", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url: url })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || "Unknown server error");
            }

            statusDiv.textContent = "Success! Data retrieved.";
            statusDiv.className = "success";
            
            // 2. Render the Main Result
            renderResultHTML(data);

            // 3. Save to History (Local Storage)
            saveToHistory(data);

        } catch (error) {
            console.error("Frontend Error:", error);
            statusDiv.textContent = "Failed: " + error.message;
            statusDiv.className = "error";
            resultsContainer.innerHTML = '';
        } finally {
            button.disabled = false;
        }
    });

    // --- Helper Functions ---

    function renderResultHTML(data) {
        // 1. Extract ALL data points, including content_summary
        const { metadata, contact, security, content_summary } = data;

        // Helper: Create HTML tags for arrays (emails, phones, etc.)
        const makeTags = (items, isDanger = false) => {
            if (!items || items.length === 0) return '<span class="tag empty">None found</span>';
            return items.map(i => `<span class="tag ${isDanger ? 'danger' : ''}">${i}</span>`).join('');
        };

        // Helper: Format API Keys
        let apiKeysList = [];
        for (const [provider, keys] of Object.entries(security.possible_api_keys || {})) {
            keys.forEach(k => apiKeysList.push(`<b>${provider}:</b> ${k}`));
        }

        // Helper: Safely get counts (default to 0 if undefined)
        const headings = content_summary?.headings_count || 0;
        const links = content_summary?.links_count || 0;

        // 2. Build the HTML
        const html = `
            <div class="result-card">
                
                <!-- Metadata Section -->
                <div class="meta-header">
                    ${metadata.image ? `<img src="${metadata.image}" style="max-width:100%; height:auto; border-radius:8px; margin-bottom:15px; border:1px solid #eee;" alt="Site Preview">` : ''}
                    <h2>${metadata.title || 'No Title Found'}</h2>
                    <a href="${metadata.url}" target="_blank">${metadata.url}</a>
                    <p style="margin-top:0.5rem;">${metadata.description || 'No description available.'}</p>
                </div>

                <!-- Content Stats Section (NEW) -->
                <div class="data-section">
                    <div class="section-label">Content Overview</div>
                    <div class="tags">
                        <span class="tag">Headings found: <b>${headings}</b></span>
                        <span class="tag">Links found: <b>${links}</b></span>
                    </div>
                </div>

                <!-- Contact Info -->
                <div class="data-section">
                    <div class="section-label">Contact Emails</div>
                    <div class="tags">${makeTags(contact.emails)}</div>
                </div>

                <div class="data-section">
                    <div class="section-label">Phone Numbers</div>
                    <div class="tags">${makeTags(contact.phones)}</div>
                </div>

                <div class="data-section">
                    <div class="section-label">Social Media</div>
                    <div class="tags">${makeTags(contact.socials)}</div>
                </div>

                <!-- Security Section -->
                <div class="data-section">
                    <div class="section-label" style="color:${apiKeysList.length ? 'red' : '#6b7280'}">
                        Security / API Keys
                    </div>
                    <div class="tags">${makeTags(apiKeysList, true)}</div>
                </div>
            </div>
        `;

        resultsContainer.innerHTML = html;
    }

    function saveToHistory(data) {
        let history = JSON.parse(localStorage.getItem("scrapeHistory") || "[]");
        
        data.timestamp = new Date().toLocaleString();
        
        // Add to top of list
        history.unshift(data);

        // Limit to 10 items
        if (history.length > 10) history = history.slice(0, 10);

        localStorage.setItem("scrapeHistory", JSON.stringify(history));
        loadHistory();
    }

    function loadHistory() {
        if (!historyContainer) return;
        
        const history = JSON.parse(localStorage.getItem("scrapeHistory") || "[]");
        historyContainer.innerHTML = "";

        if (history.length === 0) {
            historyContainer.innerHTML = '<div style="color:#999; font-style:italic;">No recent searches.</div>';
            return;
        }

        history.forEach(item => {
            const div = document.createElement("div");
            div.className = "history-item";
            
            // Get stats for history preview
            const emailCount = item.contact?.emails?.length || 0;
            const linkCount = item.content_summary?.links_count || 0;

            div.innerHTML = `
                <div>
                    <div class="history-url">${item.metadata.url}</div>
                    <div class="history-time">${item.timestamp}</div>
                </div>
                <div style="font-size:0.8rem; color:#666;">
                    ${emailCount} emails • ${linkCount} links
                </div>
            `;
            
            div.addEventListener("click", () => {
                renderResultHTML(item);
                statusDiv.textContent = "Loaded from history";
                statusDiv.className = "";
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });

            historyContainer.appendChild(div);
        });
    }
});