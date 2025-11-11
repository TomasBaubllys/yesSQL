// User calls: commands.get("mykey")
// This calls: connection.request("GET", "/get/mykey")
// Returns: { status: "OK", data: { mykey: "myvalue" } }