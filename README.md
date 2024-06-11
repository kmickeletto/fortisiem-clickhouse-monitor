# FortiSIEM ClickHouse Monitor

FortiSIEM ClickHouse Monitor is a Python tool designed to extract and monitor ClickHouse information in FortiSIEM environments. It supports daemon mode and can be automatically installed as a systemd service.

## Features

- Extracts data health and replication health of ClickHouse nodes.
- Monitors ClickHouseKeeper nodes for health and statistics.
- Supports automatic module installation.
- Can be run as a daemon or installed as a systemd service.
- Configurable with various JSON output color themes.

## Requirements

- Python 3.9+
- FortiSIEM version 7.0.0 or higher
- Systemd (for service installation)

## Installation

### Clone the Repository

```bash
git clone https://github.com/yourusername/fortisiem-clickhouse-monitor.git
cd fortisiem-clickhouse-monitor
```

### Run the Script

```bash
chStats.py
```

### Install as a Systemd Service

Run the script with the `--install` flag to install it as a systemd service:

```bash
chStats.py --install
```

## Usage

### Command-Line Arguments

- `-m`: Mode of operation (job name).
- `-U`: Upload events to Supervisor instead of displaying output.
- `--theme`: JSON output color theme (default: `github-dark`).
- `-i`: JSON output indentation level (default: `2`).
- `--install`: Install as a systemd service.
- `--daemon`: Run as a daemon.

### Example Usage

Extract data health and display it:

```bash
./chStats.py -m getDataHealth
```
```json
{
  "DATA_NODE_HEALTH": [
    {
      "shard": 1,
      "nodes": [
        {
          "deviceName": "forti-super.home.mickeletto.local",
          "deviceIp": "192.168.128.222",
          "replicas": [
            {
              "dbName": "fsiem",
              "dbTable": "events_replicated",
              "readonly": 0,
              "sessionExpired": 0,
              "queueSize": 1,
              "insertsInQueue": 1,
              "mergesInQueue": 0,
              "partMutationsInQueue": 0,
              "lastQueueUpdate": "2024-06-11 10:28:04",
              "absoluteDelay": 1782019,
              "replicasOnlinePct": 100,
              "queueOldestTime": "2024-05-21 19:27:53",
              "insertsOldestTime": "2024-05-21 19:27:53",
              "oldestPartToGetTime": "18250-20240521_6839_6884_9",
              "activeReplicas": "1"
            },
            {
              "dbName": "fsiem",
              "dbTable": "summary",
              "readonly": 0,
              "sessionExpired": 0,
              "queueSize": 0,
              "insertsInQueue": 0,
              "mergesInQueue": 0,
              "partMutationsInQueue": 0,
              "lastQueueUpdate": "2024-06-11 10:25:49",
              "absoluteDelay": 0,
              "replicasOnlinePct": 100,
              "activeReplicas": "1"
            }
          ]
        }
      ]
    }
  ]
}
```

Run as a daemon:

```bash
chStats.py --daemon
```
