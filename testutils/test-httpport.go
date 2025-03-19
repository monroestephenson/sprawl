package testutils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
)

type NodeInfo struct {
	ID       string `json:"id"`
	HTTPPort int    `json:"http_port"`
}

func RunTest() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("HTTP port test starting")

	// Log environment variable
	envPort := os.Getenv("SPRAWL_HTTP_PORT")
	log.Printf("SPRAWL_HTTP_PORT environment variable: %q", envPort)

	// Parse the environment variable
	var httpPort int
	var err error
	if envPort != "" {
		httpPort, err = strconv.Atoi(envPort)
		if err != nil {
			log.Printf("Error parsing SPRAWL_HTTP_PORT: %v", err)
			httpPort = 0
		}
	}
	log.Printf("Parsed HTTP port: %d", httpPort)

	// Test JSON encoding and decoding
	testJSON := fmt.Sprintf(`{"id":"test-node", "http_port":%d}`, httpPort)
	log.Printf("Test JSON: %s", testJSON)

	var nodeInfo NodeInfo
	err = json.Unmarshal([]byte(testJSON), &nodeInfo)
	if err != nil {
		log.Printf("Error unmarshaling: %v", err)
	}
	log.Printf("Unmarshaled HTTP port: %d", nodeInfo.HTTPPort)

	// Convert an integer to JSON
	meta := map[string]interface{}{
		"id":        "test-node",
		"http_port": httpPort,
	}
	jsonBytes, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Error marshaling: %v", err)
	}
	log.Printf("Marshaled JSON: %s", string(jsonBytes))

	// Now try to unmarshal it back
	var parsedMeta map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsedMeta)
	if err != nil {
		log.Printf("Error unmarshaling map: %v", err)
	}

	// Check the type of http_port
	httpPortValue := parsedMeta["http_port"]
	log.Printf("HTTP port value type: %T, value: %v", httpPortValue, httpPortValue)

	// Extract HTTP port with proper type handling
	extractedPort := 0
	switch v := httpPortValue.(type) {
	case float64: // JSON numbers are unmarshaled as float64
		extractedPort = int(v)
		log.Printf("Extracted HTTP port as float64: %d", extractedPort)
	case json.Number:
		portNum, err := v.Int64()
		if err == nil {
			extractedPort = int(portNum)
			log.Printf("Extracted HTTP port as json.Number: %d", extractedPort)
		}
	case string:
		portNum, err := strconv.Atoi(v)
		if err == nil {
			extractedPort = portNum
			log.Printf("Extracted HTTP port as string: %d", extractedPort)
		}
	case int:
		extractedPort = v
		log.Printf("Extracted HTTP port as int: %d", extractedPort)
	case int64:
		extractedPort = int(v)
		log.Printf("Extracted HTTP port as int64: %d", extractedPort)
	default:
		log.Printf("HTTP port has unexpected type %T: %v", httpPortValue, httpPortValue)
	}

	log.Printf("Final extracted port: %d", extractedPort)
}
