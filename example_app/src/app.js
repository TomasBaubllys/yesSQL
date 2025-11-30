import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import * as cheerio from 'cheerio';
import DatabaseClient from '../js_api/index.js';
import { timeStamp } from 'console';

const DB_URL = process.env.DB_URL || 'http://host.docker.internal:8000';
const DEF_CURSOR = "_client_cursor"
const db = new DatabaseClient({ url: DB_URL });
let cursor_exist = await db.createCursor(DEF_CURSOR);

const app = express();
const PORT = 8080;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const REGEX = {
    email: /([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)/gi,
    phone: /(?:\+?\d{1,3}[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}/g,
    apiKeys: {
        google: /AIza[0-9A-Za-z-_]{35}/g,
        stripe: /sk_live_[0-9a-zA-Z]{24}/g,
        aws: /AKIA[0-9A-Z]{16}/g,
        facebook: /EAAB[0-9a-zA-Z]+/g
    }
};

app.post('/scrape', async (req, res) => {
    let { url } = req.body;

    if (!url) return res.status(400).json({ error: "Missing URL" });
    if (!/^https?:\/\//i.test(url)) url = 'https://' + url;

    console.log(`Scraping: ${url}`);

    try {
        const response = await axios.get(url, {
            headers: { 
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36" 
            },
            timeout: 8000
        });

        const html = response.data;
        const $ = cheerio.load(html);

        const title = $("title").text() || "No Title";
        const description = $('meta[name="description"]').attr('content') || 
                            $('meta[property="og:description"]').attr('content') || "";
        const image = $('meta[property="og:image"]').attr('content') || "";

        const bodyText = $('body').text(); 

        const emailsFound = (bodyText.match(REGEX.email) || []);
        $('a[href^="mailto:"]').each((_, el) => {
            const href = $(el).attr('href');
            if(href) emailsFound.push(href.replace('mailto:', ''));
        });
        const uniqueEmails = [...new Set(emailsFound)];

        const phonesFound = (bodyText.match(REGEX.phone) || []);
        const uniquePhones = [...new Set(phonesFound)].map(p => p.trim());

        const foundKeys = {};
        for (const [provider, regex] of Object.entries(REGEX.apiKeys)) {
            const matches = html.match(regex);
            if (matches) foundKeys[provider] = [...new Set(matches)];
        }

        const socialDomains = ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'github.com', 'youtube.com'];
        const socialLinks = [];
        $("a").each((_, el) => {
            const href = $(el).attr("href");
            if (href && socialDomains.some(domain => href.includes(domain))) {
                socialLinks.push(href);
            }
        });

        const result = {
            metadata: { url, title, description, image },
            contact: { emails: uniqueEmails, phones: uniquePhones, socials: [...new Set(socialLinks)] },
            security: { possible_api_keys: foundKeys },
            content_summary: {
                headings_count: $("h1, h2, h3").length,
                links_count: $("a").length
            },
            timestamp: new Date().toLocaleString()
        };

        // Save to Database
        await db.set(url, JSON.stringify(result));
        
        res.json(result);

    } catch (err) {
        console.error("Scraping Error:", err.message);
        const errorMessage = err.response ? `Target site returned ${err.response.status}` : err.message;
        res.status(500).json({ error: errorMessage });
    }
});

app.post('/get_nextprev', async(req, res) => {
    try {
        const { amount, command } = req.body;
        if(!cursor_exist) {
            cursor_exist = await db.createCursor(DEF_CURSOR);
            if(!cursor_exist) {
                return res.status(500).json({error: "Cursor error"});
            }
        }

        if(!amount || !command) {
            return res.status(400).json({ error: "Missing amount or command" });
        }

        let dbData;
        if(command === 'get_next') {
            dbData = await db.getFF(DEF_CURSOR, amount);
        }
        else {
            dbData = await db.getFB(DEF_CURSOR, amount);
        }

        let result = dbData;
        if(typeof dbData === 'string') {
            result = JSON.parse(dbData);
        }

        res.json(result);

    } catch (err) {
        console.log(err);
        res.status(500).json({error: "Failed from the database"});
    }
});

app.post('/get_prefix', async(req, res) => {
    try {
        const { amount, prefix } = req.body;
        
        const tempCursor = 'cursor_' + Math.random().toString(36);
        const is_success = await db.createCursor(tempCursor);
        if(!is_success) {
            return res.status(500).json({error: "Cursor error"});
        }

        if(!amount || !prefix) {
            return res.status(400).json({ error: "Missing amount or prefix" });
        }

        const dbData = await db.getKeysPrefix(tempCursor, prefix, amount);

        let result = dbData;
        if(typeof dbData === 'string') {
            result = JSON.parse(dbData);
        }

        db.deleteCursor(tempCursor).catch(err => {console.log("Cleanup failed: ", err)});

        res.json(result);

    } catch (err) {
        console.log(err);
        res.status(500).json({error: "Failed from the database"});
    }
});

app.post('/get', async (req, res) => {
    try {
        const { url, protocol } = req.body;
        const prefix = protocol || "https://";

        if (!url) {
            return res.status(400).json({ error: "Missing URL key" });
        }

        const full_url = prefix + url;
        console.log(`Fetching specific URL from DB: ${full_url}`);

        const dbData = await db.get(full_url);

        if (!dbData) {
            return res.status(404).json({ error: "Record not found" });
        }

        let result = dbData;
        if (typeof dbData === 'string') {
            result = JSON.parse(dbData);
        }

        res.json(result);

    } catch (err) {
        console.error("DB Fetch Error:", err);
        res.status(500).json({ error: "Failed to fetch from database" });
    }
});

app.post('/remove', async (req, res) => {
    try {
        const { url, protocol } = req.body;
        const prefix = protocol || "https://";

        if (!url) {
            return res.status(400).json({ error: "Missing URL key" });
        }

        const full_url = prefix + url;
        console.log(`Fetching specific URL from DB: ${full_url}`);

        const dbData = await db.remove(full_url);

        if (!dbData) {
            return res.status(404).json({ error: "Record not found" });
        }

        let result = dbData;
        if (typeof dbData === 'string') {
            result = JSON.parse(dbData);
        }

        res.json(result);

    } catch (err) {
        console.error("DB Fetch Error:", err);
        res.status(500).json({ error: "Failed to fetch from database" });
    }
});

app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});