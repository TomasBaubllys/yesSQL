// client/commands.js

class Commands {
  constructor(connection) {
    this.connection = connection;
  }

  async get(key) {
    const response = await this.connection.request('POST', '/get', { key });
    return response;
  }

  async set(key, value) {
    const response = await this.connection.request('POST', '/set', { key, value });
    return response;
  }

  async remove(key) {
    const response = await this.connection.request('DELETE', '/delete', { key });
    return response;
  }

  async createCursor(name, key = '0') {
    const response = await this.connection.request('POST', '/create_cursor', { name, key });
    return response;
  }

  async deleteCursor(name) {
    const response = await this.connection.request('DELETE', '/delete_cursor', { key: name });
    return response;
  }

  async getFF(name, amount) {
    const response = await this.connection.request('POST', '/get_ff', { name, amount });
    return response;
  }

  async getFB(name, amount) {
    const response = await this.connection.request('POST', '/get_fb', { name, amount });
    return response;
  }

  async getKeys(name, amount) {
    const response = await this.connection.request('POST', '/get_keys', { name, amount });
    return response;
  }

  async getKeysPrefix(name, prefix, amount) {
    const response = await this.connection.request('POST', '/get_keys_prefix', { name, prefix, amount });
    return response;
  }
}

export default Commands;
// module.exports = Commands;