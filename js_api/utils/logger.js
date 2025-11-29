// Log what's happening
// utils/logger.js

const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3
};

class Logger {
  constructor(level = 'INFO') {
    this.level = LOG_LEVELS[level] || LOG_LEVELS.INFO;
  }

  debug(message, ...args) {
    if (this.level <= LOG_LEVELS.DEBUG) {
      console.debug(`[DEBUG] ${new Date().toISOString()}`, message, ...args);
    }
  }

  info(message, ...args) {
    if (this.level <= LOG_LEVELS.INFO) {
      console.info(`[INFO] ${new Date().toISOString()}`, message, ...args);
    }
  }

  warn(message, ...args) {
    if (this.level <= LOG_LEVELS.WARN) {
      console.warn(`[WARN] ${new Date().toISOString()}`, message, ...args);
    }
  }

  error(message, ...args) {
    if (this.level <= LOG_LEVELS.ERROR) {
      console.error(`[ERROR] ${new Date().toISOString()}`, message, ...args);
    }
  }
}

export default new Logger(process.env.LOG_LEVEL || 'INFO');
// module.exports = new Logger(process.env.LOG_LEVEL || 'INFO');