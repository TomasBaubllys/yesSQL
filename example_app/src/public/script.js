document.addEventListener("DOMContentLoaded", () => {
    // UI Refs
    const urlList = document.querySelector("#url-list");
    const resultDisplay = document.querySelector("#result-display");
    const statusDiv = document.querySelector("#status");
    const amountSelect = document.querySelector("#amount-select");
    
    // New Refs for Prefix Sidebar
    const btnPrefixSearch = document.querySelector("#btn-prefix-search");
    const prefixInput = document.querySelector("#prefix-input");
    const prefixList = document.querySelector("#prefix-list");

    // Buttons
    const btnNext = document.querySelector("#btn-next");
    const btnPrev = document.querySelector("#btn-prev");
    const btnScrape = document.querySelector("#scrape-btn");
    const btnDb = document.querySelector("#db-btn");
    const input = document.querySelector("#url-input");

        // 1. Generate a UUID (or random string) when the page loads
    const sessionId = localStorage.getItem('site_session_id') || crypto.randomUUID();
    localStorage.setItem('site_session_id', sessionId);

    // 3. Optional: Cleanup when window closes
    window.addEventListener('beforeunload', () => {
        navigator.sendBeacon('/cleanup_cursor', JSON.stringify({})); 
        // Note: sendBeacon doesn't easily support custom headers, 
        // so for cleanup you might want to pass ID in body or use keepalive fetch.
    });




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
        //fetchBatch('get_next');
    });

    // 2. Scrape New
    btnScrape.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL");
        
        updateStatus("Scraping target...", "blue");
        
        try {
            const res = await fetch("/scrape", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                 },
                body: JSON.stringify({ url })
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);

            updateStatus("Scraped successfully.", "green");
            renderDetail(data); 
            //fetchBatch('get_next'); // Refresh list

        } catch (e) {
            updateStatus("Error: " + e.message, "red");
        }
    });

    // 3. Search DB
    btnDb.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL key to search");

        updateStatus("Searching database...", "blue");

        try {
            const res = await fetch("/get", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                },
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

    // 4. Prefix Search (New Functionality)
    btnPrefixSearch.addEventListener("click", async () => {
        const prefix = prefixInput.value.trim();
        if(!prefix) return alert("Please enter a prefix");

        updateStatus("Searching prefix...", "blue");
        prefixList.style.opacity = "0.5";

        try {
            const res = await fetch("/get_prefix", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                },
                body: JSON.stringify({
                    amount: currentAmount,
                    prefix: prefix
                })
            });

            console.log("RES::")
            console.log(res);

            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Error fetching prefix");

            console.log("DATA::")
            console.log(data);
            // Reuse logic but render to prefixList
            renderPrefixList(data, prefixList);
            updateStatus(`Found records for prefix '${prefix}'`, "green");

        } catch (e) {
            console.error(e);
            updateStatus("Prefix Error: " + e.message, "red");
            prefixList.innerHTML = `<div style="padding:15px; color:red;">Error: ${e.message}</div>`;
        } finally {
            prefixList.style.opacity = "1";
        }
    });

    // --- Core Logic ---

   async function fetchBatch(command) {
        console.log(`Fetching batch: ${command} with amount: ${currentAmount}`);
        urlList.style.opacity = "0.5";
        
        try {
            const res = await fetch("/get_nextprev", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId 
                },
                body: JSON.stringify({ 
                    amount: currentAmount, 
                    command: command 
                })
            });

            const data = await res.json();
            
            if (!res.ok) {
                throw new Error(data.error || `Server Error (${res.status})`);
            }

            if (Array.isArray(data)) {
                renderList(data, urlList); // Pass target container
            } else {
                updateStatus("Received invalid data format", "red");
            }

        } catch (e) {
            console.error("Fetch Error:", e);
            updateStatus("Error: " + e.message, "red");
        } finally {
            urlList.style.opacity = "1";
        }
    }

    function renderPrefixList(keys, target) {
        target.innerHTML = "";

        if (!keys || keys.length === 0) {
            target.innerHTML = `<div style="padding:15px; text-align:center; color:#999;">No records found for prefix</div>`;
            return;
        }

        keys.forEach(key => {
            // Use 'list-item' class to match the style of the main list
            const div = document.createElement("div");
            div.className = "list-item"; 
            div.textContent = key; // Display the URL key
            div.title = key;

            div.addEventListener("click", () => {
                // 1. Visual: Remove 'active' class from all other items (in both lists)
                document.querySelectorAll(".list-item").forEach(el => el.classList.remove("active"));
                
                // 2. Visual: Highlight this specific item
                div.classList.add("active");

                // 3. Data: Fetch the full details for this key
                loadRecordByKey(key);
            });

            target.appendChild(div);
        });
    }

    async function loadRecordByKey(key) {
        updateStatus("Loading record details...", "blue");

        try {
            // IMPORTANT: The backend /get endpoint automatically adds "https://" 
            // via: const prefix = protocol || "https://";
            // Since 'key' from get_prefix already has "https://", we must strip it 
            // here, otherwise the backend looks for "https://https://..."
            
            const cleanUrl = key.replace(/^https?:\/\//, '');

            const res = await fetch("/get", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                },
                body: JSON.stringify({ 
                    url: cleanUrl, 
                    protocol: "https://" // Explicitly match backend logic
                })
            });

            const data = await res.json();
            
            if (!res.ok) throw new Error(data.error || "Record not found in DB");

            updateStatus("Record loaded.", "green");
            
            // Render the fetched details into the view
            renderDetail(data);

        } catch (e) {
            console.error(e);
            updateStatus("Error: " + e.message, "red");
        }
    }


    // Modified to accept a container argument
    function renderList(items, container) {
        container.innerHTML = "";

        if (!items || items.length === 0) {
            container.innerHTML = '<div style="padding:15px; color:#999; text-align:center;">No records found.</div>';
            return;
        }

        items.forEach(item => {
            const div = document.createElement("div");
            div.className = "list-item";
            
            let finalItem = typeof item === 'string' ? JSON.parse(item) : item;

            if (finalItem && !finalItem.metadata && finalItem.value) {
                if (typeof finalItem.value === 'string') {
                    try {
                        finalItem = JSON.parse(finalItem.value);
                    } catch (e) {
                        console.error("Could not parse inner value JSON:", finalItem.value);
                    }
                } else {
                    finalItem = finalItem.value;
                }
            }

            if (!finalItem || !finalItem.metadata) {
                console.warn("Skipping item with missing metadata:", item);
                return; 
            }
            
            div.textContent = finalItem.metadata.url;
            div.title = finalItem.metadata.url;

            div.addEventListener("click", () => {
                // Remove active class from ALL list items in document
                document.querySelectorAll(".list-item").forEach(el => el.classList.remove("active"));
                div.classList.add("active");
                renderDetail(finalItem);
            });

            container.appendChild(div);
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
                <!-- Delete Button -->
                <button id="btn-delete-record" class="delete-btn" title="Remove from Database">
                     &#128465; Delete
                </button>

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

        // Attach Delete Logic
        document.getElementById("btn-delete-record").addEventListener("click", async () => {
            if(!confirm(`Are you sure you want to delete ${metadata.url} from the database?`)) return;

            // Server expects split URL and Protocol. 
            // We need to try and parse the existing URL.
            let fullUrl = metadata.url;
            let protocol = "";
            let urlPart = fullUrl;

            // Simple split if present
            if(fullUrl.includes("://")) {
                const parts = fullUrl.split("://");
                protocol = parts[0] + "://";
                urlPart = parts[1];
            } else {
                // If stored without protocol, assume https or empty
                // Based on server logic "prefix = protocol || https://"
                // We send empty protocol so server adds default, or specific if known.
                // However, the DB keys usually store the full URL including protocol.
                // The server '/remove' does: full_url = prefix + url. 
                // So we must ensure we send exactly what reconstructs the key.
                
                // If the key in DB is "https://google.com"
                // And we send protocol="https://" url="google.com" -> "https://google.com" (Correct)
                
                // If key is "http://..."
                // We must detect http.
            }

            try {
                const res = await fetch("/remove", {
                    method: "POST",
                    headers: { 
                        "Content-Type": "application/json",
                        'x-session-id': sessionId
                    },
                    body: JSON.stringify({ 
                        url: urlPart,
                        protocol: protocol 
                    })
                });

                if(!res.ok) {
                    const errData = await res.json();
                    throw new Error(errData.error || "Removal failed");
                }

                updateStatus("Record deleted successfully.", "green");
                resultDisplay.innerHTML = `<div class="placeholder"><p>Record deleted.</p></div>`;
                
                // Optional: refresh lists
                // fetchBatch('get_next'); 

            } catch(e) {
                console.error(e);
                alert("Failed to delete: " + e.message);
            }
        });
    }

    function updateStatus(msg, color) {
        statusDiv.textContent = msg;
        statusDiv.style.color = color === "red" ? "#ef4444" : (color === "green" ? "#10b981" : "#4f46e5");
    }
});

