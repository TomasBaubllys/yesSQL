// client/protocol.js

class Protocol {
  parse(response) {
    // Handle successful response
    if (response.status === 'OK') {
      // Extract the value from the data object
      const values = Object.values(response.data);
      return values.length > 0 ? values[0] : null;
    }

    // Handle data not found
    if (response.status === 'DATA NOT FOUND') {
      return null;
    }

    // Handle error
    if (response.status === 'ERROR') {
      throw new Error(`Error code: ${response.code || 'Unknown'}`);
    }

    // Unknown status
    throw new Error(`Unknown status: ${response.status}`);
  }

  // NEW: For commands that return multiple key-value pairs
  parseMultiple(response) {
    if (response.status === 'OK') {
      return response.data; // Returns the full object
    }

    if (response.status === 'DATA NOT FOUND') {
      return {};
    }

    if (response.status === 'ERROR') {
      throw new Error(`Error code: ${response.code || 'Unknown'}`);
    }

    throw new Error(`Unknown status: ${response.status}`);
  }

  // NEW: For GET_KEYS commands that return only keys
  parseKeys(response) {
    if (response.status === 'OK') {
      return Object.keys(response.data); // Returns array of keys
    }

    if (response.status === 'DATA NOT FOUND') {
      return [];
    }

    if (response.status === 'ERROR') {
      throw new Error(`Error code: ${response.code || 'Unknown'}`);
    }

    throw new Error(`Unknown status: ${response.status}`);
  }
}

module.exports = Protocol;