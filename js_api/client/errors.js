// client/errors.js

class DatabaseError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DatabaseError';
  }
}

class ConnectionError extends DatabaseError {
  constructor(message) {
    super(message);
    this.name = 'ConnectionError';
  }
}

class QueryError extends DatabaseError {
  constructor(message) {
    super(message);
    this.name = 'QueryError';
  }
}

class NotFoundError extends DatabaseError {
  constructor(message) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export default {
  DatabaseError,
  ConnectionError,
  QueryError,
  NotFoundError
}

/*module.exports = {
  DatabaseError,
  ConnectionError,
  QueryError,
  NotFoundError
};*/