document.addEventListener("DOMContentLoaded", () => {
    const input = document.querySelector("#url-input");
    const scrapeBtn = document.querySelector("#scrape-btn");
    const dbBtn = document.querySelector("#db-btn"); // New Button
    const refreshBtn = document.querySelector("#refresh-btn"); // Refresh Button
    const statusDiv = document.querySelector("#status");
    const resultsContainer = document.querySelector("#results");
    const historyContainer = document.querySelector("#history-list");

    // 1. Load DB History on Page Start
    fetchHistoryFromDB();

    // --- BUTTON 1: SCRAPE NEW ---
    scrapeBtn.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL");

        setLoading(true, "Scraping deep data...");

        try {
            const response = await fetch("/scrape", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url: url })
            });

            const data = await response.json();
            if (!response.ok) throw new Error(data.error || "Server error");

            statusDiv.textContent = "Success! Scraped and saved to DB.";
            statusDiv.className = "success";
            
            renderResultHTML(data);
            fetchHistoryFromDB(); // Refresh list to show new item

        } catch (error) {
            handleError(error);
        } finally {
            setLoading(false);
        }
    });

    // --- BUTTON 2: GET FROM DB (PREFIX SEARCH) ---
    dbBtn.addEventListener("click", async () => {
        const prefix = input.value.trim(); 
        // If input is empty, it will fetch everything (defaults to 'http' in backend)
        
        setLoading(true, "Searching Database...");

        try {
            const response = await fetch("/get", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ prefix: prefix })
            });

            const data = await response.json();
            if (!response.ok) throw new Error(data.error || "DB Error");

            if (data.length === 0) {
                statusDiv.textContent = "No records found in database.";
                statusDiv.className = "error";
                resultsContainer.innerHTML = "";
            } else if (data.length === 1) {
                // If only 1 result, show it fully
                statusDiv.textContent = "Found 1 record.";
                statusDiv.className = "success";
                renderResultHTML(data[0]);
            } else {
                // If multiple, show distinct message
                statusDiv.textContent = `Found ${data.length} records. Check history list below.`;
                statusDiv.className = "success";
                resultsContainer.innerHTML = '<div style="text-align:center; color:#666;">Select a result from the list below</div>';
            }

            // Update the history sidebar with exactly what we found
            renderHistoryList(data);

        } catch (error) {
            handleError(error);
        } finally {
            setLoading(false);
        }
    });

    // --- BUTTON 3: REFRESH LIST ---
    refreshBtn.addEventListener("click", fetchHistoryFromDB);

    // --- FUNCTIONS ---

    async function fetchHistoryFromDB() {
        try {
            const response = await fetch("/get-data", {
                method: "POST", 
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ prefix: "" }) // Empty prefix = get all
            });
            const data = await response.json();
            renderHistoryList(data);
        } catch (e) {
            console.error("Failed to load history", e);
        }
    }

    function renderHistoryList(items) {
        historyContainer.innerHTML = "";
        if (!items || items.length === 0) {
            historyContainer.innerHTML = '<div style="color:#999; font-style:italic;">Database is empty.</div>';
            return;
        }

        items.forEach(item => {
            const div = document.createElement("div");
            div.className = "history-item";
            
            const emailCount = item.contact?.emails?.length || 0;
            const linkCount = item.content_summary?.links_count || 0;
            const time = item.timestamp || "N/A";

            div.innerHTML = `
                <div>
                    <div class="history-url">${item.metadata.url}</div>
                    <div class="history-time">${time}</div>
                </div>
                <div style="font-size:0.8rem; color:#666;">
                    ${emailCount} emails • ${linkCount} links
                </div>
            `;
            
            div.addEventListener("click", () => {
                renderResultHTML(item);
                statusDiv.textContent = "Loaded from Database";
                statusDiv.className = "";
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });

            historyContainer.appendChild(div);
        });
    }

    function renderResultHTML(data) {
        const { metadata, contact, security, content_summary } = data;

        const makeTags = (items, isDanger = false) => {
            if (!items || items.length === 0) return '<span class="tag empty">None found</span>';
            return items.map(i => `<span class="tag ${isDanger ? 'danger' : ''}">${i}</span>`).join('');
        };

        let apiKeysList = [];
        for (const [provider, keys] of Object.entries(security.possible_api_keys || {})) {
            keys.forEach(k => apiKeysList.push(`<b>${provider}:</b> ${k}`));
        }

        const headings = content_summary?.headings_count || 0;
        const links = content_summary?.links_count || 0;

        const html = `
            <div class="result-card">
                <div class="meta-header">
                    ${metadata.image ? `<img src="${metadata.image}" style="max-width:100%; height:auto; border-radius:8px; margin-bottom:15px; border:1px solid #eee;" alt="Site Preview">` : ''}
                    <h2>${metadata.title || 'No Title'}</h2>
                    <a href="${metadata.url}" target="_blank">${metadata.url}</a>
                    <p>${metadata.description || ''}</p>
                </div>
                <div class="data-section">
                    <div class="section-label">Content Overview</div>
                    <div class="tags">
                        <span class="tag">Headings: <b>${headings}</b></span>
                        <span class="tag">Links: <b>${links}</b></span>
                    </div>
                </div>
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
                <div class="data-section">
                    <div class="section-label" style="color:${apiKeysList.length ? 'red' : '#6b7280'}">Security / API Keys</div>
                    <div class="tags">${makeTags(apiKeysList, true)}</div>
                </div>
            </div>
        `;
        resultsContainer.innerHTML = html;
    }

    function setLoading(isLoading, text) {
        if (isLoading) {
            statusDiv.textContent = text;
            statusDiv.className = "";
            resultsContainer.innerHTML = '<div style="text-align:center; color:#666;">Processing...</div>';
            scrapeBtn.disabled = true;
            dbBtn.disabled = true;
        } else {
            scrapeBtn.disabled = false;
            dbBtn.disabled = false;
        }
    }

    function handleError(error) {
        console.error("App Error:", error);
        statusDiv.textContent = "Error: " + error.message;
        statusDiv.className = "error";
        resultsContainer.innerHTML = "";
    }
});