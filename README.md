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
