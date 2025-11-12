// Import all other modules
// Create instances of them
// Expose a simple API to users
// index.js

const Connection = require('./client/connection');
const Commands = require('./client/commands');
const Protocol = require('./client/protocol');
const logger = require('./utils/logger');

class DatabaseClient {
  constructor(config = {}) {
    this.connection = new Connection(config);
    this.protocol = new Protocol();
    this.commands = new Commands(this.connection);
    this.logger = logger;
  }

  async get(key) {
    this.logger.debug('GET request for key:', key);
    const response = await this.commands.get(key);
    const value = this.protocol.parse(response);
    this.logger.debug('GET response:', value);
    return value;
  }

  async set(key, value) {
    this.logger.debug('SET request:', key, value);
    const response = await this.commands.set(key, value);
    const result = this.protocol.parse(response);
    this.logger.debug('SET response:', result);
    return result;
  }

  async remove(key) {
    this.logger.debug('REMOVE request for key:', key);
    const response = await this.commands.remove(key);
    const result = this.protocol.parse(response);
    this.logger.debug('REMOVE response:', result);
    return result;
  }
}

module.exports = DatabaseClient;