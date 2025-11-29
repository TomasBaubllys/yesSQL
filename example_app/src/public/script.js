document.addEventListener("DOMContentLoaded", () => {
    const input = document.querySelector("#url-input");
    const button = document.querySelector("#scrape-btn");
    const statusDiv = document.querySelector("#status");
    const resultsPre = document.querySelector("#results");

    button.addEventListener("click", async () => {
        const url = input.value.trim();
        
        if (!url) {
            alert("Please enter a URL");
            return;
        }

        statusDiv.textContent = "Scraping deep data... (this may take a few seconds)";
        statusDiv.className = "";
        resultsPre.textContent = "";
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

            statusDiv.textContent = "Success! Found contact info & keys.";
            statusDiv.style.color = "green";
            
            resultsPre.textContent = JSON.stringify(data, null, 4);

        } catch (error) {
            console.error("Frontend Error:", error);
            statusDiv.textContent = "Failed: " + error.message;
            statusDiv.className = "error";
        } finally {
            button.disabled = false;
        }
    });
});