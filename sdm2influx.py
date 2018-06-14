#!/usr/bin/env python3

import argparse
import datetime
import logging
import queue
import struct
import sys
import threading
import time

from influxdb import InfluxDBClient
import pymodbus.client.sync

__version__ = '0.3.1'

logger = logging.getLogger(__name__)

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
        values = { 12: self.read_register(12) } # Active Power (W)
        return values

    def read_all(self):
        values = {}
        for reg in self.registers:
            values[reg] = self.read_register(reg)
        return values

class InfluxWriter(threading.Thread):
    """InfluDB writer thread"""

    def __init__(self, commands, address, database, *args, **kwargs):
        self.address = address
        self.database = database
        self.commands = commands
        super().__init__(*args, **kwargs)

    def run(self):
        addr = '%s:8086/%s' % (self.address, self.database)
        logger.info('starting InfluxDB writer to %s', addr)
        influx = InfluxDBClient(self.address, 8086, database=self.database)
        running = True
        while running:
            (command, parameter) = self.commands.get()
            if command == 'WRITE':
                logger.info('writing data to InfluxDB')
                influx.write_points(parameter)
            else:
                running = False
            self.commands.task_done()


class ZeroPublisher(threading.Thread):
    """ZeroMQ publisher thread"""

    def __init__(self, commands, address='tcp://*:5556', *args, **kwargs):
        self.address = address
        self.commands = commands
        super().__init__(*args, **kwargs)

    def run(self):
        logger.info('starting ZeroMQ publisher at %s', self.address)
        import zmq
        zmq_context = zmq.Context()
        zmq_socket = zmq_context.socket(zmq.PUB)
        zmq_socket.bind(self.address)
        running = True
        while running:
            (command, parameter) = self.commands.get()
            if command == 'PUB':
                logger.info('ZeroMQ PUB: %s', repr(parameter))
                zmq_socket.send_string(parameter)
            else:
                running = False
            self.commands.task_done()

class Sdm2Influx(object):
    """main class"""

    def __init__(self):
        self.eastron = None
        self.production = None

    def init_meters(self, serial_port, timeout, production=False):
        modbus = ModBus(port=serial_port, timeout=timeout)
        self.eastron = Eastron_SDM(modbus)
        if production:
            self.production = Eastron_SDM(modbus, address=2)

    def main(self, args):
        # initialize energy meters
        self.init_meters(serial_port=args.serial, timeout=args.timeout, production=args.production)

        q_influxdb_writer = queue.Queue()
        influx_writer = InfluxWriter(commands=q_influxdb_writer, address=args.influxdb, database=args.database)
        influx_writer.name = 'InfluxWriter'
        influx_writer.start()

        if args.zeromq:
            q_zero_publisher = queue.Queue()
            zero_publisher = ZeroPublisher(q_zero_publisher)
            zero_publisher.name = 'ZeroPublisher'
            zero_publisher.start()

        while True:
            now = datetime.datetime.utcnow()
            data_fields = { }

            values = self.eastron.read_all()        # read all registers
            if self.production:                     # read production meter
                time.sleep(0.1)
                production_energy = self.production.read_energy()

            # derive net consumption
            if self.production:
                data_fields.update(self.calc_consumption(values, production_energy))
                logger.info('%50s: %9.3f', 'Production Power (W)', data_fields['production_power'])
                logger.info('%50s: %9.3f', 'Consumed Power (W)', data_fields['consumption_power'])
                logger.info('%50s: %9.3f', 'Self-Consumed Power (W)', data_fields['self_consumption_power'])

            for reg in values:                  # log all registers and prepare data for InfluxDB
                # register name and machine-friendly name
                name = self.eastron.registers[reg]
                uglyname = name.split('(', 1)[0].strip().lower().replace(' ', '_')
                # add value to InfluxDB measurement
                data_fields[uglyname] = float(values[reg])
                # log value
                output = '%50s: %9.3f' % (name, values[reg])
                logger.info(output)

            # send data to InfluxDB
            influx_data = {'measurement': 'energy',
                           'time': now,
                           'tags': { 'line': 'home_mains' },
                           'fields': data_fields,
                           }
            q_influxdb_writer.put(('WRITE', [influx_data])) # send data to InfluxDB

            # publish data on ZeroMQ
            if args.zeromq and args.production:
                zmq_pkt = 'energy %f %f' % (data_fields['consumption_power'], data_fields['production_power'])
                q_zero_publisher.put(('PUB', zmq_pkt))

            # sleep until next minute
            next_cycle = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
            nap = max((next_cycle - datetime.datetime.utcnow()).total_seconds(), 0)
            time.sleep(nap)

    @staticmethod
    def calc_consumption(values, production_energy):
        data = { }
        data['production_power'] = production_energy[12] * -1.0
        data['consumption_power'] = float(values[12] + data['production_power'])
        # determine self consumption
        if values[12] > 0:
            # importing additional energy -> 100% autoconsumption of produced power
            data['self_consumption_power'] = max(data['production_power'], 0.0)
        else:
            # exporting additional energy -> 100% autoconsumption of consumed power
            data['self_consumption_power'] = max(data['consumption_power'], 0.0)
        return data

    @staticmethod
    def init_logging():
        '''set up logging and return the main logger'''
        global logger
        log_formatter = logging.Formatter('%(asctime)s - %(threadName)13s - %(levelname)8s - %(message)s')
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(log_formatter)
        logger.addHandler(log_handler)
        logger.setLevel(logging.DEBUG)
        logger.info('This is sdm2influx %s', __version__)
        return logger

    @staticmethod
    def parse_arguments(command_line):
        """reads command line arguments"""
        arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        arg_parser.add_argument("-d", "--database", metavar='DB', type=str, default='energymon', help="InfluxDB database name")
        arg_parser.add_argument("-i", "--influxdb", metavar='HOST', type=str, default='127.0.0.1', help="InfluxDB host")
        arg_parser.add_argument("-p", "--production", action='store_true', help="enable 2 meters mode (mains & energy production)")
        arg_parser.add_argument("-s", "--serial", metavar='DEV', type=str, default='/dev/ttyUSB0', help="modbus serial device")
        arg_parser.add_argument("-t", "--timeout", metavar='TIME', type=float, default=0.125, help="modbus timeout", )
        arg_parser.add_argument("-v", "--version", action='version', version='%(prog)s ' + __version__)
        arg_parser.add_argument("-z", "--zeromq", action='store_true', help="enable publishing data on ZeroMQ")
        args = arg_parser.parse_args(args=command_line)
        return args


if __name__ == '__main__':
    sdm2influx = Sdm2Influx()
    sdm2influx.init_logging()
    args = sdm2influx.parse_arguments(sys.argv[1:])
    sdm2influx.main(args)
