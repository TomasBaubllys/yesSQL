// Make HTTP requests to: http://127.0.0.1:8000
// GET /get/{key}
// POST /set/{key}/{value}  
// DELETE /delete/{key}

// client/connection.js

class Connection {
  constructor(config) {
    this.baseUrl = config.url || 'http://127.0.0.1:8000';
    this.timeout = config.timeout || 30000;
  }

  async request(method, path) {
    const url = `${this.baseUrl}${path}`;
    
    const options = {
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    try {
      const response = await fetch(url, options);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data;
      
    } catch (error) {
      throw error;
    }
  }
}

module.exports = Connection;