// User calls: commands.get("mykey")
// This calls: connection.request("GET", "/get/mykey")
// Returns: { status: "OK", data: { mykey: "myvalue" } }

class Commands {
  constructor(connection) {
    this.connection = connection;
  }

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
}

module.exports = Commands;