<!-- markdownlint-disable MD024 -->
# sdm2influx Changelog

All notable changes to this project will be documented in this section.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning v2.0.0](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2025-09-17

### Added

- added `CHANGELOG.md` based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
- add type hints
- add a lot of linters and formatters, managed through `pre-commit`
- add support for PV and storage meters
- debug logging

### Changed

- drop support for Raspbian OS <=11 and Python <=3.10
- drop `retrying` and switch to `tenacity`
- made more robust in case of ModBus errors
- switch project to `uv`, `ruff`, `ty`
- upgrade all dependencies

### Fixed

## [0.5.0] - 2021-06-21

### Added

- add support for an energy storage system behind a third energy meter
- retry failed writes to InfluxDB

### Fixed

- avoid hanging InfluxWriter thread if writing fails

## [0.4.0] - 2018-06-15

### Added

- publish available energy on ZeroMQ with higher temporal resolution
  (10 seconds instead of the previous 60 seconds)

## [0.3.1] - 2018-06-15

### Added

- logging to `stdout`
- multi-threaded implementation
- graceful shutdown on `SIGINT` / `SIGTERM`

### Changed

- align meters readings to whole minutes

## [0.3.0] - 2018-06-14

### Added

- publish energy data through [ZeroMQ](https://zeromq.org)

## [0.2.1] - 2018-06-14

### Fixed

- convert all values to float before sending them to InfluxDB, to avoid
  inconsistent data type problems

## [0.2.0] - 2018-06-14

### Added

- add support for an energy production system (ie. photovoltaic) behind a
  second energy meter

## [0.1.0] - 2018-05-03

### Added

- support for one energy meter on the connection to the energy provider
