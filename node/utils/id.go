package utils

// TruncateID safely truncates a node ID to 8 characters for logging
func TruncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
