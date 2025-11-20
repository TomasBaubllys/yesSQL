package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	HOST = "127.0.0.1"
	PORT = 9000
)

const TOTAL_KEYS = 100000

func preloadKeys(conn net.Conn) {
	fmt.Printf("Preloading %d keys...\n", TOTAL_KEYS)
	for i := 1; i <= TOTAL_KEYS; i++ {
		key := fmt.Sprintf("%d", i)
		val := fmt.Sprintf("val-%d", i)

		conn.Write(buildSetCommand(key, val))
		data, _ := recvExact(conn)
		resp, _ := parseResponse(data, false)

		if i%10000 == 0 || i == TOTAL_KEYS {
			fmt.Printf("[%d/%d] Last response: %s\n", i, TOTAL_KEYS, resp)
		}
	}
	fmt.Println("Preload complete.")
}

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

const (
	UNKNOWN          = 0
	PARTITION_DIED   = 1
	CURSOR_NOT_FOUND = 2
	MSG_TOO_SHORT    = 3
)

var ERROR_NAMES = map[uint16]string{
	UNKNOWN:          "UNKNOWN",
	PARTITION_DIED:   "PARTITION CANNOT BE REACHED",
	CURSOR_NOT_FOUND: "CURSOR NOT FOUND",
	MSG_TOO_SHORT:    "MESSAGE WAS TOO SHORT",
}

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

var (
	ColorRed   = "\033[91m"
	ColorGreen = "\033[92m"
	ColorReset = "\033[0m"
)

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

func buildGetFFCommand(cursor string, count uint16) []byte {
	nameBytes := []byte(cursor)
	totalLen := uint64(8 + 6 + 2 + 2 + 1 + len(nameBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)

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

func buildGetKeysCommand(cursor string, count uint16) []byte {
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

func buildGetKeysPrefixCommand(cursor string, count uint16, prefix string) []byte {
	nameBytes := []byte(cursor)
	prefixBytes := []byte(prefix)

	totalLen := uint64(8 + 6 + 2 + 2 + 1 + len(nameBytes) + 2 + len(prefixBytes))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, totalLen)

	buf.Write([]byte{0, 0, 0, 0, 0, 0})
	binary.Write(buf, binary.BigEndian, count)
	binary.Write(buf, binary.BigEndian, uint16(CMD_GET_KEYS_PREFIX))
	binary.Write(buf, binary.BigEndian, uint8(len(nameBytes)))
	buf.Write(nameBytes)

	binary.Write(buf, binary.BigEndian, uint16(len(prefixBytes)))
	buf.Write(prefixBytes)

	return buf.Bytes()
}

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

func parseResponse(data []byte, keysOnly bool) (string, []KV) {
	if len(data) < 18 {
		return "INVALID", nil
	}

	numElems := binary.BigEndian.Uint64(data[8:16])
	cmdCode := binary.BigEndian.Uint16(data[16:18])
	cmdName := COMMAND_NAMES[cmdCode]
	pos := 18

	if cmdCode == CMD_ERR {
		errCode := binary.BigEndian.Uint16(data[18:20])
		return cmdName, []KV{{Key: "ERROR", Val: fmt.Sprint(ERROR_NAMES[errCode])}}
	}

	results := make([]KV, 0, numElems)

	for i := uint64(0); i < numElems; i++ {
		if pos+2 > len(data) {
			break
		}
		keyLen := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		if !keysOnly {
			if pos+4 > len(data) {
				results = append(results, KV{Key: key, Val: ""})
				break
			}
			valLen := binary.BigEndian.Uint32(data[pos : pos+4])
			pos += 4
			val := string(data[pos : pos+int(valLen)])
			pos += int(valLen)
			results = append(results, KV{Key: key, Val: val})
		} else {
			results = append(results, KV{Key: key})
		}
	}

	return cmdName, results
}

func main() {
	conn, err := net.Dial("tcp4", fmt.Sprintf("%s:%d", HOST, PORT))
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
	fmt.Println("  GET_KEYS_PREFIX <cursor> <count> <prefix>")
	fmt.Println("  exit")

	preloadKeys(conn)

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
			resp, respCode := parseResponse(data, false)
			fmt.Println("Response:", resp, "Code: ", respCode)

		case "GET":
			key := parts[1]
			conn.Write(buildGetCommand(key))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data, false)
			fmt.Println("Response:", resp)
			fmt.Println(res)

		case "REMOVE":
			key := parts[1]
			conn.Write(buildRemoveCommand(key))
			data, _ := recvExact(conn)
			resp, respCode := parseResponse(data, false)
			fmt.Println("Response:", resp, "Code: ", respCode)

		case "CREATE_CURSOR":
			name := parts[1]
			key := ""
			if len(parts) > 2 {
				key = parts[2]
			}
			conn.Write(buildCreateCursorCommand(name, key))
			data, _ := recvExact(conn)
			resp, respCode := parseResponse(data, false)
			fmt.Println("Response:", resp, "Code: ", respCode)

		case "DELETE_CURSOR":
			name := parts[1]
			conn.Write(buildDeleteCursorCommand(name))
			data, _ := recvExact(conn)
			resp, respCode := parseResponse(data, false)
			fmt.Println("Response:", resp, "Code: ", respCode)

		case "GET_FF":
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetFFCommand(cursor, count))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data, false)

			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key, "=>", kv.Val)
			}

		case "GET_FB":
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetFBCommand(cursor, count))
			data, _ := recvExact(conn)
			resp, res := parseResponse(data, false)

			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key, "=>", kv.Val)
			}

		case "GET_KEYS":
			cursor := parts[1]
			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			conn.Write(buildGetKeysCommand(cursor, count))
			data, _ := recvExact(conn)

			resp, res := parseResponse(data, true)
			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key)
			}

		case "GET_KEYS_PREFIX":
			if len(parts) < 4 {
				fmt.Println("Usage: GET_KEYS_PREFIX <cursor> <count> <prefix>")
				continue
			}
			cursor := parts[1]

			var count uint16
			fmt.Sscanf(parts[2], "%d", &count)

			prefix := parts[3]

			conn.Write(buildGetKeysPrefixCommand(cursor, count, prefix))
			data, _ := recvExact(conn)

			resp, res := parseResponse(data, true)

			fmt.Println("Response:", resp)
			for _, kv := range res {
				fmt.Println(kv.Key)
			}

		default:
			fmt.Println("Unknown command.")
		}
	}
}
