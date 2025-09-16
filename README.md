# sdm2influx

A multithreaded Python application that reads energy measurements from Eastron SDM 120/230 Modbus
meters and stores them in InfluxDB, with optional ZeroMQ data publishing.

## Overview

This Python module continuously monitors one or more
[Eastron SDM120 / SDM230](http://www.eastrongroup.com/products/10.html) Modbus energy meters,
storing the collected data in an [InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/)
time-series database. Additionally, it can publish real-time data through
[ZeroMQ](https://zeromq.org) for integration with other systems.

It has been developed and is in real world usage on a Raspberry Pi 1 Model B revision 1 (older 256MB model).

## Requirements

### Runtime Dependencies

* [Python 3](https://www.python.org) (version 3.11 or higher)
* [influxdb](https://pypi.org/project/influxdb/) - InfluxDB client library
* [pymodbus](https://pypi.org/project/pymodbus/) - Modbus communication protocol
* [pyzmq](https://pypi.org/project/pyzmq/) - ZeroMQ messaging library
* [retrying](https://pypi.org/project/retrying/) - Retry logic

### System Requirements

* Access to Eastron SDM120/SDM230 energy meters via Modbus
* InfluxDB instance for data storage
* Optional: ZeroMQ-compatible systems for real-time data consumption

## Usage

To see all available command-line options and configuration parameters:

```bash
python sdm2influx.py -h
```

### Basic Example

```bash
# Monitor a single meter and store data in InfluxDB
python sdm2influx.py --influx-host localhost --influx-database energy_data
```

For detailed configuration options, refer to the help output.

## Development

1. Install the required development tools:
   * [uv](https://github.com/astral-sh/uv)
   * [pre-commit](https://pre-commit.com/)

2. Set up the development environment:

   ```bash
   pre-commit install
   make install
   ```

## Wishlist

* Publish package to [PyPI](https://pypi.org)
* Support for complex meter configurations like multi-branch monitoring

## Contributing

Contributions are welcome! Please ensure all code passes `make check` to maintain code quality.
