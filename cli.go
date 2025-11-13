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

// ==================================================
// Command Codes
// ==================================================
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

// ==================================================
// Colors
// ==================================================
var (
	ColorRed   = "\033[91m"
	ColorGreen = "\033[92m"
	ColorReset = "\033[0m"
)

// ==================================================
// Utilities
// ==================================================
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

// ==================================================
// Command Builders
// ==================================================
func buildSetCommand(key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	keyLen := len(keyBytes)
	valLen := len(valBytes)
	totalLen := uint64(8 + 8 + 2 + 2 + keyLen + 4 + valLen)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_SET))
	binary.Write(buf, binary.BigEndian, uint16(keyLen))
	buf.Write(keyBytes)
	binary.Write(buf, binary.BigEndian, uint32(valLen))
	buf.Write(valBytes)
	return buf.Bytes()
}

func buildGetCommand(key string) []byte {
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	totalLen := uint64(8 + 8 + 2 + 2 + keyLen)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET))
	binary.Write(buf, binary.BigEndian, uint16(keyLen))
	buf.Write(keyBytes)
	return buf.Bytes()
}

func buildRemoveCommand(key string) []byte {
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	totalLen := uint64(8 + 8 + 2 + 2 + keyLen)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_REMOVE))
	binary.Write(buf, binary.BigEndian, uint16(keyLen))
	buf.Write(keyBytes)
	return buf.Bytes()
}

// CREATE_CURSOR -> [msg_len][array_size][cmd_code][cursor_name_len][cursor_name][cursor_capacity][key_len][key_str]
func buildCreateCursorCommand(cursorName string, key string) []byte {
	cursorNameBytes := []byte(cursorName)
	cursorNameLen := uint8(len(cursorNameBytes))
	keyBytes := []byte(key)
	keyLen := uint16(len(keyBytes))

	totalLen := uint64(8 + 8 + 2 + 1 + len(cursorNameBytes) + 2 + len(keyBytes))
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1)) // array size
	binary.Write(buf, binary.BigEndian, uint16(CMD_CREATE_CURSOR))
	binary.Write(buf, binary.BigEndian, cursorNameLen)
	buf.Write(cursorNameBytes)
	binary.Write(buf, binary.BigEndian, keyLen)
	buf.Write(keyBytes)
	return buf.Bytes()
}

// DELETE_CURSOR -> [msg_len][array_size][cmd_code][cursor_name_len][cursor_name]
func buildDeleteCursorCommand(cursorName string) []byte {
	cursorNameBytes := []byte(cursorName)
	cursorNameLen := uint8(len(cursorNameBytes))
	totalLen := uint64(8 + 8 + 2 + 1 + len(cursorNameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, uint16(CMD_DELETE_CURSOR))
	binary.Write(buf, binary.BigEndian, cursorNameLen)
	buf.Write(cursorNameBytes)
	return buf.Bytes()
}

// ==================================================
// Response Parser
// ==================================================
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

func parseResponse(data []byte) (string, map[string]string) {
	if len(data) < 18 {
		return "INVALID", nil
	}
	numElems := binary.BigEndian.Uint64(data[8:16])
	cmdCode := binary.BigEndian.Uint16(data[16:18])
	cmdName := COMMAND_NAMES[cmdCode]
	pos := 18

	results := make(map[string]string)
	for i := uint64(0); i < numElems && pos+2 <= len(data); i++ {
		keyLen := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)
		if pos >= len(data) {
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

// ==================================================
// Main
// ==================================================
func main() {
	rand.Seed(time.Now().UnixNano())
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Connected to server.")
	fmt.Println("Commands:")
	fmt.Println("  SET <key> <value>")
	fmt.Println("  GET <key>")
	fmt.Println("  REMOVE <key>")
	fmt.Println("  CREATE_CURSOR <name> <capacity> [key]")
	fmt.Println("  DELETE_CURSOR <name>")
	fmt.Println("  exit")

	for {
		fmt.Print("yessql> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.ToLower(line) == "exit" {
			break
		}

		parts := strings.Fields(line)
		op := strings.ToUpper(parts[0])

		switch op {
		case "SET":
			if len(parts) < 3 {
				fmt.Println("Usage: SET <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			conn.Write(buildSetCommand(key, value))
			data, _ := recvExact(conn)
			cmd, _ := parseResponse(data)
			fmt.Println("Response:", cmd)

		case "GET":
			if len(parts) != 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			key := parts[1]
			conn.Write(buildGetCommand(key))
			data, _ := recvExact(conn)
			cmd, res := parseResponse(data)
			if val, ok := res[key]; ok {
				fmt.Printf("Response: %s\nValue: %s\n", cmd, val)
			} else {
				fmt.Println("Response:", cmd)
			}

		case "REMOVE":
			if len(parts) != 2 {
				fmt.Println("Usage: REMOVE <key>")
				continue
			}
			key := parts[1]
			conn.Write(buildRemoveCommand(key))
			data, _ := recvExact(conn)
			cmd, _ := parseResponse(data)
			fmt.Println("Response:", cmd)

		case "CREATE_CURSOR":
			if len(parts) < 2 {
				fmt.Println("Usage: CREATE_CURSOR <name> <capacity> [key]")
				continue
			}
			name := parts[1]
			key := ""
			if len(parts) > 2 {
				key = parts[3]
			}
			conn.Write(buildCreateCursorCommand(name, key))
			data, _ := recvExact(conn)
			cmd, _ := parseResponse(data)
			fmt.Println("Response:", cmd)

		case "DELETE_CURSOR":
			if len(parts) != 2 {
				fmt.Println("Usage: DELETE_CURSOR <name>")
				continue
			}
			name := parts[1]
			conn.Write(buildDeleteCursorCommand(name))
			data, _ := recvExact(conn)
			cmd, _ := parseResponse(data)
			fmt.Println("Response:", cmd)

		default:
			fmt.Println("Unknown command. Try: SET, GET, REMOVE, CREATE_CURSOR, DELETE_CURSOR, exit")
		}
	}
}
