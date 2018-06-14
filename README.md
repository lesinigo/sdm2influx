# sdm2influx
read Eastron SDM 120/230 measurements via ModBus and store them to InfluxDB, and also optionally
publish data on ZeroMQ

This is a Python script that reads data from [Eastron SDM120 / SDM230](http://www.eastrongroup.com/products/10.html) Modbus energy meters and writes
it to an [InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/) database.

It has been developed and is in use on a Raspberry Pi 1 Model B revision 1 (older 256MB model).

## requirements

* [Python 3](https://www.python.org) (tested on >= 3.5)
* [influxdb](https://pypi.org/project/influxdb/)
* [pymodbus](https://pypi.org/project/pymodbus/)

## usage

See `sdm2influx.py -h`

## whishlist

* add `setup.py` and other goodies needed to publish on [PyPi](https://pypi.org)
* add support for multiple energy meters
* add support for combined measurements
  * meter 1 on main line, meter 2 on sub-line X, meter 3 on sub-line Y, no meter on sub-line Z: 1-(2+3) = sub-line Z
  * meter 1 bidirectional flow to/from energy provider, meter 2 photovoltaic production: 1+2 = actual home power consumption
