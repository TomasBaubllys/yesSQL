package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

const (
	HOST = "127.0.0.1"
	PORT = 8080
)

// ===============================================
// Command Codes
// ===============================================
const (
	CMD_OK              = 0
	CMD_ERR             = 1
	CMD_GET             = 2
	CMD_SET             = 3
	CMD_GET_KEYS        = 4
	CMD_GET_KEYS_PREFIX = 5
	CMD_GET_FF          = 6
	CMD_GET_FB          = 7
	CMD_REMOVE          = 8
	CMD_CREATE_CURSOR   = 9
	CMD_DELETE_CURSOR   = 10
	CMD_DATA_NOT_FOUND  = 11
	CMD_INVALID_COMMAND = 12
)

var COMMAND_NAMES = map[uint16]string{
	CMD_OK:              "OK",
	CMD_ERR:             "ERR",
	CMD_GET:             "GET",
	CMD_SET:             "SET",
	CMD_GET_KEYS:        "GET_KEYS",
	CMD_GET_KEYS_PREFIX: "GET_KEYS_PREFIX",
	CMD_GET_FF:          "GET_FF",
	CMD_GET_FB:          "GET_FB",
	CMD_REMOVE:          "REMOVE",
	CMD_CREATE_CURSOR:   "CREATE_CURSOR",
	CMD_DELETE_CURSOR:   "DELETE_CURSOR",
	CMD_DATA_NOT_FOUND:  "DATA_NOT_FOUND",
	CMD_INVALID_COMMAND: "INVALID_COMMAND",
}

// ===============================================
// Colors
// ===============================================
var (
	ColorRed   = "\033[91m"
	ColorGreen = "\033[92m"
	ColorReset = "\033[0m"
)

// ===============================================
// Helpers
// ===============================================
func randomKey() string {
	n := rand.Intn(112) + 16
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func randomValue() string {
	n := rand.Intn(1024) + 1
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ===============================================
// Command Builders
// ===============================================
func buildSetCommand(key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)

	totalLen := uint64(8 + 8 + 2 + 2 + len(keyBytes) + 4 + len(valBytes))
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_SET))
	binary.Write(buf, binary.BigEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)
	binary.Write(buf, binary.BigEndian, uint32(len(valBytes)))
	buf.Write(valBytes)
	return buf.Bytes()
}

func buildGetCommand(key string) []byte {
	keyBytes := []byte(key)
	totalLen := uint64(8 + 8 + 2 + 2 + len(keyBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET))
	binary.Write(buf, binary.BigEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)
	return buf.Bytes()
}

func buildRemoveCommand(key string) []byte {
	keyBytes := []byte(key)
	totalLen := uint64(8 + 8 + 2 + 2 + len(keyBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_REMOVE))
	binary.Write(buf, binary.BigEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)
	return buf.Bytes()
}

// CREATE_CURSOR ---------------------------------------
func buildCreateCursorCommand(cursorName string, key string) []byte {
	cursorBytes := []byte(cursorName)
	keyBytes := []byte(key)

	totalLen := uint64(8 + 8 + 2 + 1 + len(cursorBytes) + 2 + len(keyBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_CREATE_CURSOR))
	binary.Write(buf, binary.BigEndian, uint8(len(cursorBytes)))
	buf.Write(cursorBytes)
	binary.Write(buf, binary.BigEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)
	return buf.Bytes()
}

// DELETE_CURSOR ---------------------------------------
func buildDeleteCursorCommand(name string) []byte {
	nameBytes := []byte(name)
	totalLen := uint64(8 + 8 + 2 + 1 + len(nameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_DELETE_CURSOR))
	binary.Write(buf, binary.BigEndian, uint8(len(nameBytes)))
	buf.Write(nameBytes)
	return buf.Bytes()
}

// =====================================================
// NEW: GET_FF + GET_FB builder functions
// =====================================================
/*
GET_FF format:
[uint64 msg_len]
[6 bytes 0]
[2 bytes: number of entries requested]
[2 bytes command code]
[1 byte cursor name length]
[cursor name]
*/
func buildGetFFCommand(cursor string, count uint16) []byte {
	nameBytes := []byte(cursor)
	totalLen := uint64(8 + 6 + 2 + 2 + 1 + len(nameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)

	// 6 bytes zero
	buf.Write([]byte{0, 0, 0, 0, 0, 0})

	binary.Write(buf, binary.BigEndian, count)
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET_FF))
	binary.Write(buf, binary.BigEndian, uint8(len(nameBytes)))
	buf.Write(nameBytes)

	return buf.Bytes()
}

func buildGetFBCommand(cursor string, count uint16) []byte {
	nameBytes := []byte(cursor)
	totalLen := uint64(8 + 6 + 2 + 2 + 1 + len(nameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)

	buf.Write([]byte{0, 0, 0, 0, 0, 0})
	binary.Write(buf, binary.BigEndian, count)
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET_FB))
	binary.Write(buf, binary.BigEndian, uint8(len(nameBytes)))
	buf.Write(nameBytes)

	return buf.Bytes()
}

func buildGetKeysCommand(cursor string, count uint16) []byte{
	nameBytes := []byte(cursor)
	totalLen := uint64(8 + 6 + 2 + 2 + 1 + len(nameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)

	buf.Write([]byte{0, 0, 0, 0, 0, 0})
	binary.Write(buf, binary.BigEndian, count)
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET_KEYS))
	binary.Write(buf, binary.BigEndian, uint8(len(nameBytes)))
	buf.Write(nameBytes)

	return buf.Bytes()


}

// =====================================================
// Response Parsing
// =====================================================
func recvExact(conn net.Conn) ([]byte, error) {
	header := make([]byte, 8)
	_, err := conn.Read(header)
	if err != nil {
		return nil, err
	}

	totalLen := binary.BigEndian.Uint64(header)
	data := make([]byte, totalLen)
	copy(data, header)

	read := 8
	for read < int(totalLen) {
		n, err := conn.Read(data[read:])
		if err != nil {
			return nil, err
		}
		read += n
	}

	return data, nil
}

type KV struct {
	Key string
	Val string
}

func parseResponse(data []byte) (string, []KV) {
	if len(data) < 18 {
		return "INVALID", nil
	}

	numElems := binary.BigEndian.Uint64(data[8:16])
	cmdCode := binary.BigEndian.Uint16(data[16:18])

	cmdName := COMMAND_NAMES[cmdCode]
	pos := 18

	results := make([]KV, 0, numElems)

	for i := uint64(0); i < numElems; i++ {
		if pos+2 > len(data) {
			break
		}

		keyLen := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2

		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		if pos+4 > len(data) {
			results = append(results, KV{Key: key, Val: ""})
			break
		}

		valLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		val := string(data[pos : pos+int(valLen)])
		pos += int(valLen)

		results = append(results, KV{Key: key, Val: val})
	}

	return cmdName, results
}

/*
func parseResponse(data []byte) (string, map[string]string) {
	if len(data) < 18 {
		return "INVALID", nil
	}

	numElems := binary.BigEndian.Uint64(data[8:16])
	cmdCode := binary.BigEndian.Uint16(data[16:18])

	cmdName := COMMAND_NAMES[cmdCode]
	pos := 18
	results := make(map[string]string)

	for i := uint64(0); i < numElems; i++ {
		if pos+2 > len(data) {
			break
		}

		keyLen := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2

		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		if pos+4 > len(data) {
			results[key] = ""
			break
		}

		valLen := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		val := string(data[pos : pos+int(valLen)])
		pos += int(valLen)

		results[key] = val
	}

	return cmdName, results
}
	*/

// =====================================================
// Main
// =====================================================
func main() {
	rand.Seed(time.Now().UnixNano())

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Connected.")
	fmt.Println("Commands:")
	fmt.Println("  SET <key> <value>")
	fmt.Println("  GET <key>")
	fmt.Println("  REMOVE <key>")
	fmt.Println("  CREATE_CURSOR <name> <key>")
	fmt.Println("  DELETE_CURSOR <name>")
	fmt.Println("  GET_FF <cursor> <count>")
	fmt.Println("  GET_FB <cursor> <count>")
	fmt.Println("  GET_KEYS <cursor> <count>")
	fmt.Println("  exit")

	// ===========================================================
	// Preload exactly 20 keys: "a", "aa", "aaa", ...
	// ===========================================================
	preload := []string{
		"!",
		"!!",
		"!!!",
		"a",
		"aa",
		"aaa",
		"aaaa",
		"aaaaa",
		"aaaaaa",
		"aaaaaaa",
		"aaaaaaaa",
		"aaaaaaaaa",
		"aaaaaaaaaa",
		"aaaaaaaaaaa",
		"aaaaaaaaaaaa",
		"aaaaaaaaaaaaa",
		"aaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaa",
		"aaaaaaaaaaaaaaaaaaaa",
		"ė",
		"ėė",
		"ėėė",
		"ėėėė",
		"ėėėėė",
	}

	fmt.Println("Preloading test keys...")
	for _, k := range preload {
		conn.Write(buildSetCommand(k, "1"))
		data, _ := recvExact(conn)
		resp, _ := parseResponse(data)
		fmt.Println("  SET", k, "=>", resp)
	}
	fmt.Println("Preload complete.")
	fmt.Println("================================")

	fmt.Println("Commands:")
	fmt.Println("  SET <key> <value>")



















	for {
		fmt.Print("yessql> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}
		if line == "exit" {
			break
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {

		case "SET":
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			conn.Write(buildSetCommand(key, value))
			data, _ := recvExact(conn)
			resp, _ := parseResponse(data)
			fmt.Println("Response:", resp)

		case "GET":
			key := parts[1]
			conn.Write(buildGetCommand(key))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data)
			fmt.Println("Response:", resp)
			fmt.Println(res)

		case "REMOVE":
			key := parts[1]
			conn.Write(buildRemoveCommand(key))
			data, _ := recvExact(conn)
			resp, _ := parseResponse(data)
			fmt.Println("Response:", resp)

		case "CREATE_CURSOR":
			name := parts[1]
			key := ""
			if len(parts) > 2 {
				key = parts[2]
			}
			conn.Write(buildCreateCursorCommand(name, key))
			data, _ := recvExact(conn)
			resp, _ := parseResponse(data)
			fmt.Println("Response:", resp)

		case "DELETE_CURSOR":
			name := parts[1]
			conn.Write(buildDeleteCursorCommand(name))
			data, _ := recvExact(conn)
			resp, _ := parseResponse(data)
			fmt.Println("Response:", resp)

		// ==================================================
		// NEW GET_FF
		// ==================================================
		case "GET_FF":
			if len(parts) != 3 {
				fmt.Println("Usage: GET_FF <cursor> <count>")
				continue
			}
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetFFCommand(cursor, count))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data)

			fmt.Println("Response:", resp)

			for _, kv := range res {
				fmt.Println(kv.Key, "=>", kv.Val)
			}



		// ==================================================
		// NEW GET_FB
		// ==================================================
		case "GET_FB":
			if len(parts) != 3 {
				fmt.Println("Usage: GET_FB <cursor> <count>")
				continue
			}
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetFBCommand(cursor, count))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data)

			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key, "=>", kv.Val)
			}

		case "GET_KEYS":
			if len(parts) != 3 {
				fmt.Println("Usage: GET_KEYS <cursor> <count>")
				continue
			}
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetKeysCommand(cursor, count))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data)

			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key)
			}


		default:
			fmt.Println("Unknown command.")
		}
	}
}
