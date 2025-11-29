document.addEventListener("DOMContentLoaded", () => {
    // UI Refs
    const urlList = document.querySelector("#url-list");
    const resultDisplay = document.querySelector("#result-display");
    const statusDiv = document.querySelector("#status");
    const amountSelect = document.querySelector("#amount-select");
    
    // Buttons
    const btnNext = document.querySelector("#btn-next");
    const btnPrev = document.querySelector("#btn-prev");
    const btnScrape = document.querySelector("#scrape-btn");
    const btnDb = document.querySelector("#db-btn"); // <--- restored
    const input = document.querySelector("#url-input");

    // State
    let currentAmount = 10;
    
    // Initial Load
    fetchBatch('get_next');

    // --- Event Listeners ---

    // 1. Sidebar Navigation
    btnNext.addEventListener("click", () => fetchBatch('get_next'));
    btnPrev.addEventListener("click", () => fetchBatch('get_prev'));

    amountSelect.addEventListener("change", (e) => {
        currentAmount = parseInt(e.target.value);
        fetchBatch('get_next');
    });

    // 2. Scrape New
    btnScrape.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL");
        
        updateStatus("Scraping target...", "blue");
        
        try {
            const res = await fetch("/scrape", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url })
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);

            updateStatus("Scraped successfully.", "green");
            renderDetail(data); 
            fetchBatch('get_next'); // Refresh list

        } catch (e) {
            updateStatus("Error: " + e.message, "red");
        }
    });

    // 3. Search DB (Restored)
    btnDb.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL key to search");

        updateStatus("Searching database...", "blue");

        try {
            const res = await fetch("/get", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url })
            });
            const data = await res.json();
            
            if (!res.ok) throw new Error(data.error || "Not found");

            updateStatus("Record found in DB.", "green");
            renderDetail(data);

        } catch (e) {
            updateStatus("Not found in database.", "red");
        }
    });

    // --- Core Logic ---

    async function fetchBatch(command) {
        urlList.style.opacity = "0.5";
        
        try {
            const res = await fetch("/get_nextprev", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ 
                    amount: currentAmount, 
                    command: command 
                })
            });

            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "DB Error");

            renderList(data);

        } catch (e) {
            console.error(e);
            updateStatus("List fetch error: " + e.message, "red");
        } finally {
            urlList.style.opacity = "1";
        }
    }

    function renderList(items) {
        urlList.innerHTML = "";

        if (!items || items.length === 0) {
            urlList.innerHTML = '<div style="padding:15px; color:#999; text-align:center;">No records found.</div>';
            return;
        }

        items.forEach(item => {
            const div = document.createElement("div");
            div.className = "list-item";
            
            const finalItem = typeof item === 'string' ? JSON.parse(item) : item;
            
            div.textContent = finalItem.metadata.url;
            div.title = finalItem.metadata.url;

            div.addEventListener("click", () => {
                document.querySelectorAll(".list-item").forEach(el => el.classList.remove("active"));
                div.classList.add("active");
                renderDetail(finalItem);
            });

            urlList.appendChild(div);
        });
    }

    function renderDetail(data) {
        const { metadata, contact, security, content_summary } = data;
        
        const makeTags = (arr, danger) => arr && arr.length 
            ? arr.map(x => `<span class="tag ${danger?'danger':''}">${x}</span>`).join('') 
            : '<span style="color:#999;font-style:italic">None</span>';

        let keys = [];
        for (let [k, v] of Object.entries(security.possible_api_keys || {})) {
            v.forEach(val => keys.push(`<b>${k}:</b> ${val}`));
        }

        const imageHtml = metadata.image 
            ? `<img src="${metadata.image}" class="meta-img" alt="Site Preview">` 
            : '';

        resultDisplay.innerHTML = `
            <div class="result-card">
                
                ${imageHtml}

                <div style="margin-bottom: 20px; border-bottom:1px solid #eee; padding-bottom:15px;">
                    <h2 style="margin:0 0 5px 0; font-size:1.8rem;">${metadata.title || 'No Title'}</h2>
                    <a href="${metadata.url}" target="_blank" style="color:var(--primary); font-size:1.1rem;">${metadata.url}</a>
                    <p style="color:#666; margin-top:10px;">${metadata.description || 'No description available.'}</p>
                </div>

                <div class="section-label">Content Overview</div>
                <div class="tags">
                    <span class="tag">Headings: ${content_summary?.headings_count || 0}</span>
                    <span class="tag">Links: ${content_summary?.links_count || 0}</span>
                </div>

                <div class="section-label">Contact Emails</div>
                <div class="tags">${makeTags(contact.emails)}</div>

                <div class="section-label">Phone Numbers</div>
                <div class="tags">${makeTags(contact.phones)}</div>

                <div class="section-label" style="color:${keys.length?'red':'inherit'}">Security Keys</div>
                <div class="tags">${makeTags(keys, true)}</div>
            </div>
        `;
    }

    function updateStatus(msg, color) {
        statusDiv.textContent = msg;
        statusDiv.style.color = color === "red" ? "#ef4444" : (color === "green" ? "#10b981" : "#4f46e5");
    }
});