To improve storage configuration documentation:
Add detailed storage configuration guides in documentation
Create usage examples for different storage tiers (memory, RocksDB, S3/MinIO)
Document storage migration and backup strategies
To implement tiered storage command-line flags:
Add command-line flags for storage type selection (-storageType=memory|disk|s3)
Implement storage path configuration via CLI (-storagePath, -s3Bucket, etc.)
Create configuration presets for common storage scenarios
To improve observability for partitioning and rebalancing:
Add specific monitoring endpoints for topic partitioning status
Implement visualization for partition distribution
Create real-time metrics for rebalancing operations
Add logging for partition events and decisions