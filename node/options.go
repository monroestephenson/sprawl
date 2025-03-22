package node

// Options struct for node configuration
type Options struct {
	BindAddress           string
	AdvertiseAddr         string
	BindPort              int
	HTTPPort              int
	GossipPort            int
	MaxConcurrentRequests int
	StoragePath           string
	Debug                 bool
	EnableAI              bool
	ReplicationFactor     int
	AutoPortAssign        bool // Add auto port assignment option
	PortRangeStart        int  // Starting port when auto-assigning
	PortRangeEnd          int  // Ending port when auto-assigning
	// Storage-related fields
	StorageType     string
	DiskStoragePath string
	S3Bucket        string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	MemoryMaxSize   uint64
	MemoryToDiskAge int64
	DiskToCloudAge  int64
}

// DefaultOptions returns default node options
func DefaultOptions() *Options {
	return &Options{
		BindAddress:           "0.0.0.0:8080",
		HTTPPort:              8080,
		GossipPort:            7946,
		MaxConcurrentRequests: 1000, // Changed back to 1000 from 5
		StoragePath:           "./data",
		Debug:                 false,
		EnableAI:              true,
		AutoPortAssign:        true, // Enable auto port assignment by default
		PortRangeStart:        8080, // Default starting port
		PortRangeEnd:          8180, // Default ending port
		StorageType:           "memory",
		DiskStoragePath:       "./data/disk",
		MemoryMaxSize:         104857600, // 100MB default
		MemoryToDiskAge:       3600,      // 1 hour default
		DiskToCloudAge:        86400,     // 24 hours default
	}
}
