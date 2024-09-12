package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	header := make([]byte, 100)
	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}
	// now, for the number of tables
	pageHeader := make([]byte, 12)
	_, err = databaseFile.ReadAt(pageHeader, 100)
	if err != nil {
		log.Fatal(err)
	}

	var numTables uint16
	if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &numTables); err != nil {
		fmt.Println("Failed to read integer:\n", err)
		return
	}

	switch command {
		case ".dbinfo":
			// Uncomment this to pass the first stage
			fmt.Printf("database page size: %v\n", pageSize)
			fmt.Printf("number of tables: %v\n", numTables)
		case ".tables":
			// let's get the table names
			page := make([]byte, pageSize-112)
			_, err = databaseFile.ReadAt(page,112)
			if err != nil {
				fmt.Println("Failed to read page:", err)
				return
			}

			for i := uint16(0); i < numTables; i++ {
				var cellPointer uint16
				offset := 2*i // 2 bytes per cell pointer
				if err := binary.Read(bytes.NewReader(page[offset:offset+2]), binary.BigEndian, &cellPointer); err != nil {
					fmt.Println("Failed to read integer:", err)
					return
				}
				cell := page[cellPointer:]
				tableName := getTableName(cell)
				fmt.Println(tableName + "\n")
			}
		default:
			fmt.Println("Unknown command", command)
			os.Exit(1)
	}
}

func readVarint(data []byte) (int, int) {
	var result int
	var bytesRead int
	for i := 0; i < len(data); i++ {
		// 0x7f is a bitmask (01111111) that clears the most significant bit of the byte
		// because in a varint only the lower 7 bits are used to store the value
		// |= is a bitwise OR assignment operator. this adds the bits from the current byte to the
		// correct position in `result`
		result |= int(data[i]&0x7f) << uint(7*i)
		bytesRead++
		// if the most significant bit is 0, then this is the last byte of the varint
		// here, &0x80 is an AND bitmask that returns the most significant bit of the byte
		if data[i]&0x80 == 0 {
			break
		}
	}
	return result, bytesRead
}

func getTableName(cell []byte) string {
	// skipping rowid
	_, bytesRead := readVarint(cell)
	// skip header
	_, offset := readVarint(cell[bytesRead:])
	bytesRead += offset
	// skip the type column
	_, offset = readVarint(cell[bytesRead:])
	bytesRead += offset

	// finally, get the table name
	tableNameLength, nameLengthBytes := readVarint(cell[bytesRead:])
	bytesRead += nameLengthBytes
	// here, we convert the bytes from the current offset (at start of tablename) to offset+tablenameLength
	tableName := string(cell[bytesRead : bytesRead+tableNameLength])

	return tableName
}
