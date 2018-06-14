#!/usr/bin/env python3

import argparse
import datetime
import struct
import sys
import time

from influxdb import InfluxDBClient
import pymodbus.client.sync

__version__ = '0.3.0'

class ModBus(object):
    def __init__(self, port='/dev/ttyUSB0', baudrate=2400, parity='N', stopbits=1, timeout=0.125):
        self.client = pymodbus.client.sync.ModbusSerialClient('rtu',
                                                              port=port,
                                                              baudrate=baudrate,
                                                              parity=parity,
                                                              stopbits=stopbits,
                                                              timeout=timeout)
        self.client.connect()

    def read_register(self, register, unit=1):
        res = self.client.read_input_registers(register, 2, unit=unit)
        value = struct.unpack('>f',struct.pack('>HH',*res.registers))[0]
        return value

    def close(self):
        self.client.close()

class Eastron_SDM(object):
    # http://www.eastrongroup.com/data/uploads/Eastron_SDM230-Modbus_protocol_V1_2.pdf
    registers = {   0: 'Voltage (V)',
                    6: 'Current (A)',
                    12: 'Active Power (W)',
                    18: 'Apparent Power (VA)',
                    24: 'Reactive Power (VAr)',
                    30: u'Power Factor (cosθ)',
                    36: 'Phase Angle (degrees)',
                    70: 'Frequency (Hz)',
                    72: 'Import Active Energy (kWh)',
                    74: 'Export Active Energy (kWh)',
                    76: 'Import Reactive Energy (kVARh)',
                    78: 'Export Reactive Energy (kVARh)',
                    84: 'Total system power demand (W)',
                    86: 'Maximum total system power demand (W)',
                    88: 'Current system positive power demand (W)',
                    90: 'Maximum system positive power demand (W)',
                    92: 'Current system reverse power demand (W)',
                    94: 'Maximum system reverse power demand (W)',
                    258: 'Current demand (A)',
                    264: 'Maximum current demand (A)',
                    342: 'Total Active Energy (kWh)',
                    344: 'Total Reactive Energy (kVARh)',
                    384: 'Current resettable total active energy (kWh)',
                    386: 'Current resettable total reactive energy (kVARh)',
                }

    def __init__(self, modbus, address=1):
        self.modbus = modbus
        self.address = address

    def read_register(self, register):
        return self.modbus.read_register(register=register, unit=self.address)

    def read_energy(self):
        return self.read_register(12)   # Active Power (W)

    def read_all(self):
        values = {}
        for reg in self.registers:
            values[reg] = self.read_register(reg)
        return values

def parse_arguments(command_line):
    """reads command line arguments"""
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument("-d", "--database", metavar='DB', type=str, default='energymon', help="InfluxDB database name")
    arg_parser.add_argument("-i", "--influxdb", metavar='HOST', type=str, default='127.0.0.1', help="InfluxDB host")
    arg_parser.add_argument("-p", "--production", action='store_true', help="enable 2 meters mode (mains & energy production)")
    arg_parser.add_argument("-s", "--serial", metavar='DEV', type=str, default='/dev/ttyUSB0', help="modbus serial device")
    arg_parser.add_argument("-t", "--timeout", metavar='TIME', type=float, default=0.125, help="modbus timeout", )
    arg_parser.add_argument("-v", "--version", action='version', version='%(prog)s ' + __version__)
    args = arg_parser.parse_args(args=command_line)
    return args


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    modbus = ModBus(port=args.serial, timeout=args.timeout)
    eastron = Eastron_SDM(modbus)
    if args.production:
        production = Eastron_SDM(modbus, address=2)
    else:
        production = None
    influx = InfluxDBClient(args.influxdb, 8086, database=args.database)

    while True:
        print(datetime.datetime.now())

        influx_data = {'measurement': 'energy',
                       'time': datetime.datetime.utcnow(),
                       'tags': { 'line': 'home_mains' },
                       'fields': { },
                       }

        values = eastron.read_all()         # read all registers

        # read production meter and derive net consumption
        if production:
            time.sleep(0.1)
            production_power = production.read_energy() * -1.0
            influx_data['fields']['production_power'] = float(production_power)
            print('%50s: %9.3f' % ('Production Power (W)', production_power))
            consumption_power = values[12] + production_power
            influx_data['fields']['consumption_power'] = float(consumption_power)
            print('%50s: %9.3f' % ('Consumed Power (W)', consumption_power))
            # determine self consumption
            if values[12] > 0:
                # importing additional energy -> 100% autoconsumption of produced power
                self_consumption_power = max(production_power, 0)
            else:
                # exporting additional energy -> 100% autoconsumption of consumed power
                self_consumption_power = max(consumption_power, 0)
            influx_data['fields']['self_consumption_power'] = float(self_consumption_power)
            print('%50s: %9.3f' % ('Self-Consumed Power (W)', self_consumption_power))

        for reg in values:                  # print all registers and prepare data for InfluxDB
            # register name and machine-friendly name
            name = eastron.registers[reg]
            uglyname = name.split('(', 1)[0].strip().lower().replace(' ', '_')
            # add value to InfluxDB measurement
            influx_data['fields'][uglyname] = float(values[reg])
            # print value
            output = '%50s: %9.3f' % (name, values[reg])
            print(output)

        influx.write_points([influx_data])  # send data to InfluxDB

        time.sleep(60)                      # sleep until next cycle

    modbus.close()
