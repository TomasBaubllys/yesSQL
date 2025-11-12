// client/protocol.js

class Protocol {
  parse(response) {
    // Handle successful response
    if (response.status === 'OK') {
      // Extract the value from the data object
      // response.data is like {"mykey": "myvalue"}
      const values = Object.values(response.data);
      return values.length > 0 ? values[0] : null;
    }

    // Handle data not found
    if (response.status === 'DATA NOT FOUND') {
      return null;
    }

    // Handle error
    if (response.status === 'ERROR') {
      throw new Error(response.message || 'Unknown error');
    }

    // Unknown status
    throw new Error(`Unknown status: ${response.status}`);
  }
}

module.exports = Protocol;