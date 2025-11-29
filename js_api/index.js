// index.js

/*const Connection = require('./client/connection');
const Commands = require('./client/commands');
const Protocol = require('./client/protocol');
const logger = require('./utils/logger');*/

import Connection from './client/connection.js';
import Commands from './client/commands.js';
import Protocol from './client/protocol.js';
import logger from './utils/logger.js';

class DatabaseClient {
  constructor(config = {}) {
    this.connection = new Connection(config);
    this.protocol = new Protocol();
    this.commands = new Commands(this.connection);
    this.logger = logger;
  }

  // Existing methods
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

  // NEW: Cursor methods
  async createCursor(name, key = '') {
    this.logger.debug('CREATE_CURSOR request:', name, key);
    const response = await this.commands.createCursor(name, key);
    const result = this.protocol.parse(response);
    this.logger.debug('CREATE_CURSOR response:', result);
    return result;
  }

  async deleteCursor(name) {
    this.logger.debug('DELETE_CURSOR request:', name);
    const response = await this.commands.deleteCursor(name);
    const result = this.protocol.parse(response);
    this.logger.debug('DELETE_CURSOR response:', result);
    return result;
  }

  async getFF(name, amount) {
    this.logger.debug('GET_FF request:', name, amount);
    const response = await this.commands.getFF(name, amount);
    const result = this.protocol.parseMultiple(response);
    this.logger.debug('GET_FF response:', result);
    return result;
  }

  async getFB(name, amount) {
    this.logger.debug('GET_FB request:', name, amount);
    const response = await this.commands.getFB(name, amount);
    const result = this.protocol.parseMultiple(response);
    this.logger.debug('GET_FB response:', result);
    return result;
  }

  async getKeys(name, amount) {
    this.logger.debug('GET_KEYS request:', name, amount);
    const response = await this.commands.getKeys(name, amount);
    const result = this.protocol.parseKeys(response);
    this.logger.debug('GET_KEYS response:', result);
    return result;
  }

  async getKeysPrefix(name, prefix, amount) {
    this.logger.debug('GET_KEYS_PREFIX request:', name, prefix, amount);
    const response = await this.commands.getKeysPrefix(name, prefix, amount);
    const result = this.protocol.parseKeys(response);
    this.logger.debug('GET_KEYS_PREFIX response:', result);
    return result;
  }

  // Session management
  getSessionId() {
    return this.connection.getSessionId();
  }

  setSessionId(sessionId) {
    this.connection.setSessionId(sessionId);
  }
}

export default DatabaseClient;
// module.exports = DatabaseClient;


