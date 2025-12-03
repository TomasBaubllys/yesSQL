document.addEventListener("DOMContentLoaded", () => {
    const urlList = document.querySelector("#url-list");
    const resultDisplay = document.querySelector("#result-display");
    const statusDiv = document.querySelector("#status");
    const amountSelect = document.querySelector("#amount-select");
    
    const btnPrefixSearch = document.querySelector("#btn-prefix-search");
    const prefixInput = document.querySelector("#prefix-input");
    const prefixList = document.querySelector("#prefix-list");

    const btnNext = document.querySelector("#btn-next");
    const btnPrev = document.querySelector("#btn-prev");
    const btnScrape = document.querySelector("#scrape-btn");
    const btnCrawl = document.querySelector("#crawl-btn");
    const btnDb = document.querySelector("#db-btn");
    const input = document.querySelector("#url-input");

    // Crawl settings
    const crawlMaxDepth = document.querySelector("#crawl-max-depth");
    const crawlMaxPages = document.querySelector("#crawl-max-pages");

    const sessionId = localStorage.getItem('site_session_id') || crypto.randomUUID();
    localStorage.setItem('site_session_id', sessionId);

    window.addEventListener('beforeunload', () => {
        navigator.sendBeacon('/cleanup_cursor', JSON.stringify({})); 
    });

    let currentAmount = 10;
    
    fetchBatch('get_next');

    btnNext.addEventListener("click", () => fetchBatch('get_next'));
    btnPrev.addEventListener("click", () => fetchBatch('get_prev'));

    amountSelect.addEventListener("change", (e) => {
        currentAmount = parseInt(e.target.value);
    });

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

        } catch (e) {
            updateStatus("Error: " + e.message, "red");
        }
    });

    btnCrawl.addEventListener("click", async () => {
        const url = input.value.trim();
        if (!url) return alert("Please enter a URL");
        
        const maxDepth = parseInt(crawlMaxDepth.value) || 3;
        const maxPages = parseInt(crawlMaxPages.value) || 50;

        updateStatus(`Starting crawl (depth: ${maxDepth}, max pages: ${maxPages})...`, "blue");
        
        try {
            const res = await fetch("/crawl", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                },
                body: JSON.stringify({ url, maxDepth, maxPages })
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error);

            updateStatus(`Crawl complete! Scraped ${data.pagesScraped} pages.`, "green");
            renderCrawlResults(data);

        } catch (e) {
            updateStatus("Crawl Error: " + e.message, "red");
        }
    });

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

            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Error fetching prefix");

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
                renderList(data, urlList); 
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

    function renderCrawlResults(data) {
        const { startUrl, pagesScraped, results, errors, limitReached } = data;
        
        const successList = results.map(r => 
            `<div class="crawl-item success">
                <div class="crawl-url">${r.url}</div>
                <div class="crawl-meta">Depth: ${r.depth} | ${r.title}</div>
            </div>`
        ).join('');

        const errorList = errors.length > 0 ? `
            <div class="section-label" style="color: red; margin-top: 20px;">Errors (${errors.length})</div>
            ${errors.map(e => 
                `<div class="crawl-item error">
                    <div class="crawl-url">${e.url}</div>
                    <div class="crawl-meta">Depth: ${e.depth} | ${e.error}</div>
                </div>`
            ).join('')}
        ` : '';

        const limitWarning = limitReached ? 
            `<div class="warning-box">⚠️ Page limit reached. Some pages may not have been crawled.</div>` : '';

        resultDisplay.innerHTML = `
            <div class="result-card">
                <h2>Crawl Complete</h2>
                <div style="background: #f0fdf4; border-left: 4px solid #10b981; padding: 15px; margin: 20px 0; border-radius: 6px;">
                    <div><strong>Start URL:</strong> ${startUrl}</div>
                    <div><strong>Pages Scraped:</strong> ${pagesScraped}</div>
                    <div><strong>Successful:</strong> ${results.length}</div>
                    <div><strong>Failed:</strong> ${errors.length}</div>
                </div>
                
                ${limitWarning}

                <div class="section-label">Scraped Pages (${results.length})</div>
                <div class="crawl-results">
                    ${successList}
                </div>

                ${errorList}
            </div>
        `;
    }

    function renderPrefixList(keys, target) {
        target.innerHTML = "";

        if (!keys || keys.length === 0) {
            target.innerHTML = `<div style="padding:15px; text-align:center; color:#999;">No records found for prefix</div>`;
            return;
        }

        keys.forEach(key => {
            const div = document.createElement("div");
            div.className = "list-item"; 
            div.textContent = key; 
            div.title = key;

            div.addEventListener("click", () => {
                document.querySelectorAll(".list-item").forEach(el => el.classList.remove("active"));
                
                div.classList.add("active");

                loadRecordByKey(key);
            });

            target.appendChild(div);
        });
    }

    async function loadRecordByKey(key) {
        updateStatus("Loading record details...", "blue");

        try {
            const cleanUrl = key.replace(/^https?:\/\//, '');

            const res = await fetch("/get", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    'x-session-id': sessionId
                },
                body: JSON.stringify({ 
                    url: cleanUrl, 
                    protocol: "https://" 
                })
            });

            const data = await res.json();
            
            if (!res.ok) throw new Error(data.error || "Record not found in DB");

            updateStatus("Record loaded.", "green");
            
            renderDetail(data);

        } catch (e) {
            console.error(e);
            updateStatus("Error: " + e.message, "red");
        }
    }

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
                document.querySelectorAll(".list-item").forEach(el => el.classList.remove("active"));
                div.classList.add("active");
                renderDetail(finalItem);
            });

            container.appendChild(div);
        });
    }

    function renderDetail(data) {
        const { metadata, contact, security, content_summary, internal_links } = data;
        
        const makeTags = (arr, danger) => arr && arr.length 
            ? arr.map(x => `<span class="tag ${danger?'danger':''}">${x}</span>`).join('') 
            : '<span style="color:#999;font-style:italic">None</span>';

        const makeLinkTags = (arr) => arr && arr.length
            ? arr.map(url => `<a href="${url}" target="_blank" class="tag" style="text-decoration:none; color:#2563eb; border-color:#bfdbfe;">${url}</a>`).join('')
            : '<span style="color:#999;font-style:italic">None</span>';

        let keys = [];
        for (let [k, v] of Object.entries(security.possible_api_keys || {})) {
            v.forEach(val => keys.push(`<b>${k}:</b> ${val}`));
        }

        const imageHtml = metadata.image 
            ? `<img src="${metadata.image}" class="meta-img" alt="Site Preview">` 
            : '';

        // Internal links section
        const internalLinksHtml = internal_links && internal_links.length > 0 ? `
            <div class="section-label">Internal Links Found (${internal_links.length})</div>
            <div class="internal-links-box">
                ${internal_links.slice(0, 20).map(link => 
                    `<div class="internal-link-item">${link}</div>`
                ).join('')}
                ${internal_links.length > 20 ? `<div style="color:#999; font-style:italic; margin-top:10px;">...and ${internal_links.length - 20} more</div>` : ''}
            </div>
        ` : '';

        resultDisplay.innerHTML = `
            <div class="result-card">
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

                <div class="section-label">Social Media</div>
                <div class="tags">${makeLinkTags(contact.socials)}</div>

                <div class="section-label" style="color:${keys.length?'red':'inherit'}">Security Keys</div>
                <div class="tags">${makeTags(keys, true)}</div>

                ${internalLinksHtml}
            </div>
        `;

        document.getElementById("btn-delete-record").addEventListener("click", async () => {
            if(!confirm(`Are you sure you want to delete ${metadata.url} from the database?`)) return;

            let fullUrl = metadata.url;
            let protocol = "";
            let urlPart = fullUrl;

            if(fullUrl.includes("://")) {
                const parts = fullUrl.split("://");
                protocol = parts[0] + "://";
                urlPart = parts[1];
            }

            try {
                const res = await fetch("/remove", {
                    method: "POST",
                    headers: { 
                        "Content-Type": "application/json",
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

                resultDisplay.innerHTML = `<div style="padding:20px; text-align:center; color:#666;">Record deleted.</div>`;
                
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