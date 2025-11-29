// client/connection.js

class Connection {
  constructor(config) {
    this.baseUrl = config.url || 'http://127.0.0.1:8000';
    this.timeout = config.timeout || 30000;
    this.sessionId = config.sessionId || this.generateSessionId();
  }

  generateSessionId() {
    return 'session_' + Date.now() + '_' + Math.random().toString(36).substring(7);
  }

  async request(method, path, body = null) {
    const url = `${this.baseUrl}${path}`;
    
    const options = {
      method: method,
      headers: {
        'Content-Type': 'application/json',
        'X-Session-Id': this.sessionId
      }
    };

    // Add body if provided
    if (body) {
      options.body = JSON.stringify(body);
    }

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

  getSessionId() {
    return this.sessionId;
  }

  setSessionId(sessionId) {
    this.sessionId = sessionId;
  }
}

export default Connection;
// module.exports = Connection;