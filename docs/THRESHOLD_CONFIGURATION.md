# Threshold Configuration System

This document provides detailed information about Sprawl's threshold configuration system, which is used for automatic scaling decisions, resource monitoring, and performance optimization.

## Overview

The threshold configuration system allows you to customize how Sprawl makes decisions about resource utilization and scaling. By configuring appropriate thresholds, you can:

- Automate scaling decisions based on actual resource usage
- Prevent resource exhaustion in your cluster
- Optimize cost by scaling down when resources are underutilized
- Fine-tune the system's behavior for your specific workload patterns

## Configuration Methods

Threshold configurations can be set using any of the following methods, listed in order of precedence:

1. **API** - Runtime updates through the API endpoints
2. **Configuration Files** - JSON configuration files
3. **Environment Variables** - System or container environment variables
4. **Default Values** - Built-in reasonable defaults

## Available Thresholds

The following thresholds can be configured:

| Threshold | Description | Default | Valid Range |
|-----------|-------------|---------|------------|
| `cpu_scale_up_threshold` | CPU usage percentage that triggers scale-up recommendations | 80.0 | 0.0-100.0 |
| `cpu_scale_down_threshold` | CPU usage percentage below which scale-down is recommended | 20.0 | 0.0-100.0 |
| `mem_scale_up_threshold` | Memory usage percentage that triggers scale-up recommendations | 85.0 | 0.0-100.0 |
| `mem_scale_down_threshold` | Memory usage percentage below which scale-down is recommended | 30.0 | 0.0-100.0 |
| `msg_rate_scale_up_threshold` | Message rate (msgs/sec) that triggers scale-up recommendations | 5000.0 | ≥ 0.0 |
| `msg_rate_scale_down_threshold` | Message rate below which scale-down is recommended | 500.0 | ≥ 0.0 |
| `min_confidence_threshold` | Minimum confidence required for a scaling recommendation | 0.7 | 0.0-1.0 |
| `enable_dynamic_adjustment` | Whether to automatically adjust thresholds based on system behavior | true | true/false |
| `adjustment_factor` | How aggressively to adjust thresholds (0.0-1.0) | 0.5 | 0.0-1.0 |
| `learning_rate` | How quickly to learn from system behavior | 0.1 | 0.0-1.0 |
| `hysteresis_percent` | Buffer percentage to prevent oscillation | 10.0 | 0.0-50.0 |
| `bad_threshold_detection_enabled` | Whether to detect and adjust inappropriate thresholds | true | true/false |
| `bad_threshold_adjustment_factor` | How much to adjust inappropriate thresholds | 0.3 | 0.0-1.0 |

## Environment Variables

Thresholds can be set using environment variables with the `SPRAWL_` prefix:

```bash
# CPU thresholds
export SPRAWL_CPU_SCALE_UP_THRESHOLD=75.0
export SPRAWL_CPU_SCALE_DOWN_THRESHOLD=25.0

# Memory thresholds
export SPRAWL_MEM_SCALE_UP_THRESHOLD=80.0
export SPRAWL_MEM_SCALE_DOWN_THRESHOLD=30.0

# Message rate thresholds
export SPRAWL_MSG_RATE_SCALE_UP_THRESHOLD=4000.0
export SPRAWL_MSG_RATE_SCALE_DOWN_THRESHOLD=400.0

# Configuration parameters
export SPRAWL_ENABLE_DYNAMIC_ADJUSTMENT=true
export SPRAWL_MIN_CONFIDENCE_THRESHOLD=0.75
export SPRAWL_CONFIG_FILE=/path/to/thresholds.json
export SPRAWL_CONFIG_RELOAD_INTERVAL=5m
```

## Configuration File Format

Thresholds can be defined in a JSON configuration file:

```json
{
  "cpu_scale_up_threshold": 75.0,
  "cpu_scale_down_threshold": 25.0,
  "mem_scale_up_threshold": 80.0,
  "mem_scale_down_threshold": 30.0,
  "msg_rate_scale_up_threshold": 4000.0,
  "msg_rate_scale_down_threshold": 400.0,
  "min_confidence_threshold": 0.75,
  "enable_dynamic_adjustment": true,
  "adjustment_factor": 0.4,
  "learning_rate": 0.1,
  "hysteresis_percent": 10.0,
  "bad_threshold_detection_enabled": true,
  "bad_threshold_adjustment_factor": 0.3
}
```

To specify the configuration file location:

```bash
export SPRAWL_CONFIG_FILE=/path/to/thresholds.json
```

## Dynamic Threshold Adjustment

The system can automatically adjust thresholds based on observed system behavior:

### Inappropriate Threshold Detection

When `bad_threshold_detection_enabled` is true, the system monitors for scenarios where:

- CPU or memory usage consistently exceeds configured thresholds
- Message processing is throttled despite resources being available
- Thresholds that cause frequent oscillation between scaling up and down

When inappropriate thresholds are detected, they are automatically adjusted by the `bad_threshold_adjustment_factor`.

### Learning From System Behavior

When `enable_dynamic_adjustment` is true, the system gradually adjusts thresholds based on:

- Observed peak and baseline resource usage patterns
- Workload seasonality and trends
- Success rate of previous scaling decisions

## API Endpoints for Runtime Configuration

The following HTTP endpoints are available for runtime threshold management:

### Get Current Thresholds

```
GET /api/v1/ai/thresholds
```

Example response:

```json
{
  "cpu_scale_up_threshold": 75.0,
  "cpu_scale_down_threshold": 25.0,
  "mem_scale_up_threshold": 80.0,
  "mem_scale_down_threshold": 30.0,
  "msg_rate_scale_up_threshold": 4000.0,
  "msg_rate_scale_down_threshold": 400.0,
  "min_confidence_threshold": 0.75,
  "last_config_source": "file",
  "last_updated": "2023-08-15T14:30:45Z"
}
```

### Update Thresholds

```
PUT /api/v1/ai/thresholds
```

Request body (partial updates supported):

```json
{
  "cpu_scale_up_threshold": 70.0,
  "mem_scale_up_threshold": 80.0
}
```

### Reload Thresholds from File

```
POST /api/v1/ai/thresholds/reload
```

Reloads thresholds from the configured file path.

## Best Practices

### Determining Appropriate Thresholds

1. **Start with defaults** for a new system
2. **Monitor system behavior** for at least a week
3. **Analyze peak usage patterns** during high-load periods
4. **Set scale-up thresholds** at 70-80% of typical peak
5. **Set scale-down thresholds** at 20-30% of typical peak

### Avoiding Oscillation

Oscillation occurs when a system rapidly scales up and down. To avoid this:

1. Ensure adequate hysteresis (gap between scale-up and scale-down thresholds)
2. Use the `hysteresis_percent` setting (10-15% recommended)
3. Monitor the oscillation history in logs
4. Increase the confidence threshold if oscillation persists

### Error Handling and Troubleshooting

Common errors and solutions:

1. **Invalid threshold values**: Ensure values are within valid ranges
2. **Configuration file not found**: Check file path and permissions
3. **Thresholds not taking effect**: Check precedence order (API > File > Environment)
4. **Unexpected scaling behavior**: Review logs for threshold adjustments and confidence scores

## Observability and Logging

The system outputs detailed logs about threshold configuration and adjustments:

```
2023-08-15 14:30:45 INFO Loaded thresholds from file /path/to/thresholds.json
2023-08-15 14:35:12 INFO Detected inappropriate CPU threshold (current: 80.0, usage: 95.0). Adjusting to 88.0.
2023-08-15 14:40:30 WARN Scaling oscillation detected. Consider increasing hysteresis (current: 5.0%).
```

Metrics available through the `/metrics` endpoint:

- `sprawl_threshold_adjustments_total`: Count of automatic threshold adjustments
- `sprawl_threshold_values`: Current threshold values
- `sprawl_scaling_recommendations_total`: Count of scaling recommendations by type
- `sprawl_scaling_oscillations_total`: Count of detected scaling oscillations

## Example Scenarios

### High-Traffic Web Service

```json
{
  "cpu_scale_up_threshold": 70.0,
  "cpu_scale_down_threshold": 30.0,
  "mem_scale_up_threshold": 80.0,
  "mem_scale_down_threshold": 40.0,
  "msg_rate_scale_up_threshold": 10000.0,
  "msg_rate_scale_down_threshold": 1000.0,
  "hysteresis_percent": 15.0
}
```

### Batch Processing System

```json
{
  "cpu_scale_up_threshold": 85.0,
  "cpu_scale_down_threshold": 10.0,
  "mem_scale_up_threshold": 80.0,
  "mem_scale_down_threshold": 20.0,
  "msg_rate_scale_up_threshold": 2000.0,
  "msg_rate_scale_down_threshold": 100.0,
  "hysteresis_percent": 20.0
}
``` 