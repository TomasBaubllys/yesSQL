// client/commands.js

class Commands {
  constructor(connection) {
    this.connection = connection;
  }

  // Existing commands
  async get(key) {
    const path = `/get/${key}`;
    const response = await this.connection.request('GET', path);
    return response;
  }

  async set(key, value) {
    const path = `/set/${key}/${value}`;
    const response = await this.connection.request('POST', path);
    return response;
  }

  async remove(key) {
    const path = `/delete/${key}`;
    const response = await this.connection.request('DELETE', path);
    return response;
  }

  // NEW: Cursor operations
  async createCursor(name, key = '0') {
    const path = key === '0' 
      ? `/create_cursor/${name}` 
      : `/create_cursor/${name}/${key}`;
    const response = await this.connection.request('POST', path);
    return response;
  }

  async deleteCursor(name) {
    const path = `/delete_cursor/${name}`;
    const response = await this.connection.request('DELETE', path);
    return response;
  }

  async getFF(name, amount) {
    const path = `/get_ff/${name}/${amount}`;
    const response = await this.connection.request('GET', path);
    return response;
  }

  async getFB(name, amount) {
    const path = `/get_fb/${name}/${amount}`;
    const response = await this.connection.request('GET', path);
    return response;
  }

  async getKeys(name, amount) {
    const path = `/get_keys/${name}/${amount}`;
    const response = await this.connection.request('GET', path);
    return response;
  }

  async getKeysPrefix(name, prefix, amount) {
    const path = `/get_keys_prefix/${name}/${prefix}/${amount}`;
    const response = await this.connection.request('GET', path);
    return response;
  }
}

module.exports = Commands;