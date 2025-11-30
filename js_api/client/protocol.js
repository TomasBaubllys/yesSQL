// client/protocol.js

class Protocol {
  parse(response) {
    // Handle successful response
    if (response.status === 'OK') {
      // Data is now an array: [{"key": "mykey", "value": "myvalue"}]
      if (response.data && response.data.length > 0) {
        // Return the first item's value
        return response.data[0].value || {};
      }
      return {};
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

  // For commands that return multiple key-value pairs
  parseMultiple(response) {
    if (response.status === 'OK') {
      return response.data; // Return the array directly
    }

    if (response.status === 'DATA NOT FOUND') {
      return [];
    }

    if (response.status === 'ERROR') {
      throw new Error(`Error code: ${response.code || 'Unknown'}`);
    }

    throw new Error(`Unknown status: ${response.status}`);
  }

  // For GET_KEYS commands that return only keys
  parseKeys(response) {
    if (response.status === 'OK') {
      // Data is array: [{"key": "k1"}, {"key": "k2"}]
      // Return just the keys: ["k1", "k2"]
      if (response.data && Array.isArray(response.data)) {
        return response.data.map(item => item.key);
      }
      return [];
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

export default Protocol;
// module.exports = Protocol;