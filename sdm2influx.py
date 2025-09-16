#!/usr/bin/env python3

# TBD: convert internal representations to Decimal

import argparse
import datetime
import logging
import queue
import signal
import struct
import sys
import threading
import time
import types
import typing as t


from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException
from pymodbus.pdu.register_message import ReadInputRegistersResponse
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError
from tenacity import retry, wait_fixed, stop_after_delay, retry_if_exception_type

__version__ = "0.6.0"
_REGISTERS_BLACKLIST = (342, 344, 384, 386)
logger = logging.getLogger(__name__)

InfluxData = list[dict[str, t.Any]]
QueueCommands = tuple[str, t.Any]
Registers = dict[int, float | None]  # a reading can be None in case of ModBus errors


def datetime_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def pretty_float(number: float | None, fmt: str = "%9.3f") -> str:
    if number is None:
        return "<None>"
    return fmt % number


class ModBus:
    def __init__(
        self,
        *,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 2400,
        parity: str = "N",
        stopbits: int = 1,
        timeout: float = 0.125,
        retries: int = 5,
    ) -> None:
        self.client = ModbusSerialClient(
            port=port, baudrate=baudrate, parity=parity, stopbits=stopbits, timeout=timeout, retries=retries
        )
        self.client.connect()

    def read_register(self, register: int, device_id: int = 1) -> float | None:
        # NB: this is not a generic register read but is actually a read of two consecutive 16bit register
        #     decoded to a single precision 32bit IEEE 754 floating point number
        # TBD: docs says "Each request for data must be restricted to 40 parameters or less", should investigate reading
        #      multiple registers at once to save on ModBus requests. The registers in a single read must be consecutive
        # logger.debug("ModBus.read_register(register=%s, device_id=%s)", register, device_id)
        res = self.client.read_input_registers(address=register, count=2, device_id=device_id)
        if not isinstance(res, ReadInputRegistersResponse):
            logger.error("got unexpected result type %s !!!", type(res))
            return None
        # this will decode natively to a float
        value = struct.unpack(">f", struct.pack(">HH", *res.registers))[0]
        assert isinstance(value, float)
        return value

    def close(self) -> None:
        self.client.close()  # type: ignore


class EastronSDM:
    # http://www.eastrongroup.com/data/uploads/Eastron_SDM230-Modbus_protocol_V1_2.pdf
    registers = {
        0: "Voltage (V)",
        6: "Current (A)",
        12: "Active Power (W)",
        18: "Apparent Power (VA)",
        24: "Reactive Power (VAr)",
        30: "Power Factor (cosθ)",
        36: "Phase Angle (degrees)",
        70: "Frequency (Hz)",
        72: "Import Active Energy (kWh)",
        74: "Export Active Energy (kWh)",
        76: "Import Reactive Energy (kVARh)",
        78: "Export Reactive Energy (kVARh)",
        84: "Total system power demand (W)",
        86: "Maximum total system power demand (W)",
        88: "Current system positive power demand (W)",
        90: "Maximum system positive power demand (W)",
        92: "Current system reverse power demand (W)",
        94: "Maximum system reverse power demand (W)",
        258: "Current demand (A)",
        264: "Maximum current demand (A)",
        342: "Total Active Energy (kWh)",
        344: "Total Reactive Energy (kVARh)",
        384: "Current resettable total active energy (kWh)",
        386: "Current resettable total reactive energy (kVARh)",
    }

    def __init__(self, modbus: ModBus, address: int = 1) -> None:
        self.modbus = modbus
        self.address = address

    def read_register(self, register: int) -> float | None:
        try:
            reg = self.modbus.read_register(register=register, device_id=self.address)
        except ModbusIOException:
            logger.warning("got ModbusIOException reading register %s from modbus address %s", register, self.address)
            return None
        return reg

    def read_energy(self) -> Registers:
        logger.debug("read_energy() from SDM230 at address %d", self.address)
        values = {
            _: self.read_register(_)
            for _ in (
                12,  # Active Power (W)
                72,  # Import Active Energy (kWh)
                74,  # Export Active Energy (kWh)
            )
        }
        return values

    def read_all(self) -> Registers:
        logger.debug("read_all() from SDM230 at address %d", self.address)
        values = {_: self.read_register(_) for _ in self.registers if _ not in _REGISTERS_BLACKLIST}
        return values


class InfluxWriter(threading.Thread):
    """InfluDB writer thread"""

    def __init__(
        self, commands: queue.Queue[QueueCommands], address: str, database: str, *args: t.Any, **kwargs: t.Any
    ) -> None:
        self.address = address
        self.database = database
        self.commands = commands
        self.influx = None
        super().__init__(*args, **kwargs)

    def run(self) -> None:
        if self.address != "":
            logger.info("starting InfluxDB writer to %s:8086/%s", self.address, self.database)
            self.influx = InfluxDBClient(self.address, 8086, database=self.database)
        else:
            logger.warning("InfluxDB writing disabled, will discard data")
        running = True
        while running:
            (command, parameter) = self.commands.get()
            if command == "WRITE":
                logger.info("writing data to InfluxDB")
                try:
                    self._write_points(parameter)
                except Exception:
                    logger.error(
                        "couldn't write data to InfluxDB, giving up on this datapoint",
                        exc_info=True,
                    )
            else:
                running = False
            self.commands.task_done()
        logger.info("closing thread")

    @retry(retry=retry_if_exception_type(InfluxDBServerError), wait=wait_fixed(0.1), stop=stop_after_delay(2))
    def _write_points(self, points: InfluxData) -> None:
        if self.influx is not None:
            self.influx.write_points(points)


class ZeroPublisher(threading.Thread):
    """ZeroMQ publisher thread"""

    def __init__(
        self, commands: queue.Queue[QueueCommands], address: str = "tcp://*:5556", *args: t.Any, **kwargs: t.Any
    ) -> None:
        self.address = address
        self.commands = commands
        super().__init__(*args, **kwargs)

    def run(self) -> None:
        logger.info("starting ZeroMQ publisher at %s", self.address)
        import zmq  # pylint:disable=import-outside-toplevel

        zmq_context = zmq.Context()
        zmq_socket = zmq_context.socket(zmq.PUB)
        zmq_socket.bind(self.address)
        running = True
        while running:
            (command, parameter) = self.commands.get()
            if command == "PUB":
                logger.info("ZeroMQ PUB: %s", repr(parameter))
                zmq_socket.send_string(parameter)
            else:
                running = False
            self.commands.task_done()
        logger.info("closing thread")


class Sdm2Influx:
    """main class"""

    def __init__(self) -> None:
        self.modbus: ModBus | None = None
        self.em_mains: EastronSDM | None = None
        self.em_production: EastronSDM | None = None
        self.em_storage: EastronSDM | None = None
        self.q_influxdb_writer: queue.Queue[QueueCommands] | None = None  # InfluxWriter queue
        self.t_influxdb_writer: threading.Thread | None = None  # InfluxWriter thread
        self.q_zero_publisher: queue.Queue[QueueCommands] | None = None  # ZeroPublisher queue
        self.t_zero_publisher: threading.Thread | None = None  # ZeroPublisher thread

    def init_meters(
        self,
        serial_port: str,
        timeout: float,
        production: bool = False,
        storage: bool = False,
    ) -> None:
        self.modbus = ModBus(port=serial_port, timeout=timeout)
        self.em_mains = EastronSDM(self.modbus, address=1)
        if production:
            self.em_production = EastronSDM(self.modbus, address=2)
        if storage:
            self.em_storage = EastronSDM(self.modbus, address=3)

    def main(self, args: argparse.Namespace) -> t.NoReturn:
        # initialize energy meters
        self.init_meters(
            serial_port=args.serial,
            timeout=args.timeout,
            production=args.production,
            storage=args.storage,
        )

        self.q_influxdb_writer = queue.Queue()
        self.t_influxdb_writer = InfluxWriter(
            commands=self.q_influxdb_writer,
            address=args.influxdb,
            database=args.database,
        )
        self.t_influxdb_writer.name = "InfluxWriter"
        self.t_influxdb_writer.start()

        if args.zeromq:
            self.q_zero_publisher = queue.Queue()
            self.t_zero_publisher = ZeroPublisher(self.q_zero_publisher)
            self.t_zero_publisher.name = "ZeroPublisher"
            self.t_zero_publisher.start()

        # set up signal handler for CTRL-C
        def quit_handler(signal: int, frame: types.FrameType | None) -> None:  # pylint:disable=redefined-outer-name,unused-argument
            logger.info("received signal %s", repr(signal))
            self.shutdown()

        signal.signal(signal.SIGINT, quit_handler)
        signal.signal(signal.SIGTERM, quit_handler)

        while True:
            # TBD: rework the cycle thing with two new parameters:
            # - time between each cycle (will do "quick" readings)
            # - number of iterations to do detailed readings
            for cycle in range(6):
                now = datetime.datetime.now()
                if cycle == 0:
                    # read complete data from mains meter and send it to InfluxDB and ZeroMQ
                    try:
                        self.do_main_readings()
                    except RuntimeError:
                        logger.error("got exception during do_main_readings()!")
                else:
                    # just read available energy and publish to ZeroMQ
                    try:
                        self.do_energy_readings()
                    except RuntimeError:
                        logger.error("got exception during do_energy_readings()!")
                # sleep until next cycle (10 seconds, rounded)
                next_cycle = now + datetime.timedelta(seconds=10)
                next_cycle = next_cycle.replace(second=(next_cycle.second // 10 * 10), microsecond=0)
                nap = max((next_cycle - datetime.datetime.now()).total_seconds(), 0)
                time.sleep(nap)

    def shutdown(self) -> None:
        # shutdown everything
        logger.info("shutting down everything")
        if self.q_influxdb_writer is not None:
            self.q_influxdb_writer.put(("QUIT", None))
        if self.t_influxdb_writer is not None:
            self.t_influxdb_writer.join()
        if self.q_zero_publisher is not None:
            self.q_zero_publisher.put(("QUIT", None))
        if self.t_zero_publisher is not None:
            self.t_zero_publisher.join()
        if self.modbus is not None:
            self.modbus.close()
            self.modbus = None
        logger.info("shutdown completed")
        sys.exit(0)

    def do_energy_readings(self) -> None:
        """
        - read registers about active power and import/export active energy
        - publish them on ZeroMQ
        """
        if self.em_mains is None:
            raise RuntimeError
        energy_mains = self.em_mains.read_energy()[12]
        if self.em_production:
            time.sleep(0.05)
            energy_production = self.em_production.read_energy()[12]
        else:
            energy_production = None
        if self.em_storage:
            time.sleep(0.05)
            energy_storage = self.em_storage.read_energy()[12]
        else:
            energy_storage = None
        # temporarily we just log the readings
        log_msg = f"mains: {energy_mains:5.0f} W"
        if energy_production is not None:
            log_msg += f", production: {energy_production:5.0f} W"
        if energy_storage is not None:
            log_msg += f", storage: {energy_storage:5.0f} W"
        logger.info(log_msg)
        # TBD TBD TBD: complete this
        # available_energy = self.em_production.read_energy()[12] * -1.0
        # if self.q_zero_publisher:
        #     zmq_pkt = "available_energy %f" % available_energy
        #     self.q_zero_publisher.put(("PUB", zmq_pkt))

    def do_main_readings(self) -> None:
        """
        - get all data from mains energy meter
        - if available, get just energy data from production meter
        - if available, get just energy data from storage meter
        - store full mains data in one table
        - store energy data only in another table
        """
        logger.debug("do_main_readings() started")
        timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        # read mains energy meter
        if self.em_mains is None:
            raise RuntimeError
        mains_data = self.em_mains.read_all()  # read all registers
        logger.info("%24s: %s", "Mains Power (W)", pretty_float(mains_data[12]))

        # read production energy meter
        production_energy = None
        if self.em_production:
            time.sleep(0.05)
            production_energy = self.em_production.read_energy()
            logger.info("%24s: %s", "Production Power (W)", pretty_float(production_energy[12]))

        # read storage energy meter
        storage_energy = None
        if self.em_storage:
            time.sleep(0.05)
            storage_energy = self.em_storage.read_energy()
            logger.info("%24s: %s", "Storage Power (W)", pretty_float(storage_energy[12]))

        # prepare mains meter data for InfluxDB
        influx_meter_data = {}
        for reg in mains_data:
            if mains_data[reg] is None:
                continue  # ModBus wasn't able to provide a value
            # register name and machine-friendly name
            name = self.em_mains.registers[reg]
            uglyname = name.split("(", 1)[0].strip().lower().replace(" ", "_")
            # add value to InfluxDB measurement
            # mypy thinks mains_data[reg] could be None... duh
            influx_meter_data[uglyname] = float(mains_data[reg])  # type: ignore
            # [DISABLED] log value
            # output = "%50s: %9.3f" % (name, mains_data[reg])
            # logger.info(output)

        # prepare power data for InfluxDB
        power_data = {}
        if mains_data[12] is not None:
            power_data["mains"] = float(mains_data[12])
        if self.em_production and production_energy is not None and production_energy[12] is not None:
            power_data["production"] = float(production_energy[12])
        if self.em_storage and storage_energy is not None and storage_energy[12] is not None:
            power_data["storage"] = float(storage_energy[12])
        if all(key in power_data for key in ["mains", "production", "storage"]) and (
            self.em_production or self.em_storage
        ):
            if power_data["mains"] < 0:
                power_data["self_consumption"] = ((power_data["production"] + power_data["storage"]) * -1) - abs(
                    power_data["mains"]
                )
            else:
                power_data["self_consumption"] = max(0.0, (power_data["production"] + power_data["storage"]) * -1)
            logger.info("%24s: %9.3f", "Self-consumed Power (W)", power_data["self_consumption"])

        # prepare energy data for InfluxDB
        energy_data = {}
        if mains_data[72] is not None:
            energy_data["mains_import"] = float(mains_data[72])
        if mains_data[74] is not None:
            energy_data["mains_export"] = float(mains_data[74])

        if self.em_production and production_energy is not None and production_energy[74] is not None:
            energy_data["production"] = float(production_energy[74])
        if self.em_storage and storage_energy is not None:
            if storage_energy[72] is not None:
                energy_data["storage_in"] = float(storage_energy[72])
            if storage_energy[74] is not None:
                energy_data["storage_out"] = float(storage_energy[74])

        # send data points to InfluxDB
        influx_data = []
        if influx_meter_data:
            influx_data.append(
                {
                    "measurement": "meter_data",
                    "time": timestamp,
                    "tags": {"line": "home_mains"},
                    "fields": influx_meter_data,
                }
            )
        if power_data:
            influx_data.append(
                {
                    "measurement": "power_data",
                    "time": timestamp,
                    "fields": power_data,
                }
            )
        if energy_data:
            influx_data.append(
                {
                    "measurement": "energy_data",
                    "time": timestamp,
                    "fields": energy_data,
                }
            )
        if influx_data and self.q_influxdb_writer is not None:
            self.q_influxdb_writer.put(("WRITE", influx_data))  # send data to InfluxDB

        # TBD TBD TBD
        # derive net consumption
        # if self.production:
        #     data_fields.update(
        #         self.calc_consumption(values, production_energy, storage_energy)
        #     )
        #     logger.info(
        #         "%50s: %9.3f", "Consumed Power (W)", data_fields["consumption_power"]
        #     )
        #     logger.info(
        #         "%50s: %9.3f",
        #         "Self-Consumed Power (W)",
        #         data_fields["self_consumption_power"],
        #     )

        # # publish data on ZeroMQ
        # if self.q_zero_publisher and self.production:
        #     zmq_pkt = "energy %f %f" % (
        #         data_fields["consumption_power"],
        #         data_fields["production_power"],
        #     )
        #     self.q_zero_publisher.put(("PUB", zmq_pkt))
        # TBD: send storage data over 0MQ ?

    @staticmethod
    def calc_consumption(
        values: Registers,
        production_energy: Registers | None,
        storage_energy: Registers | None,
    ) -> dict[str, float]:
        data = {}
        if production_energy is None:
            data["production_power"] = 0.0
        elif production_energy[12] is None:
            return data  # ModBus wasn't able to provide value, we can't calculate any more
        else:
            data["production_power"] = production_energy[12] * -1.0
        if storage_energy is None:
            data["storage_power"] = 0.0
        elif storage_energy[12] is None:
            return data  # ModBus wasn't able to provide value, we can't calculate any more
        else:
            data["storage_power"] = storage_energy[
                12
            ]  # TBD: check sign, we aim for positive = discharging towards household
        if values[12] is None:
            return data
        # calculate energy consumption
        data["consumption_power"] = float(values[12] + data["production_power"] + data["storage_power"])
        # determine self consumption
        if values[12] > 0:
            # importing additional energy -> 100% autoconsumption of produced and stored power
            data["self_consumption_power"] = max((data["production_power"] + data["storage_power"]), 0.0)
        else:
            # exporting additional energy -> 100% autoconsumption of consumed power
            data["self_consumption_power"] = max(data["consumption_power"], 0.0)
        return data

    @staticmethod
    def init_logging(debug: bool = False) -> logging.Logger:
        """set up logging and return the main logger"""
        log_formatter = logging.Formatter("%(asctime)s - %(threadName)13s - %(levelname)8s - %(message)s")
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(log_formatter)
        logger.addHandler(log_handler)
        logger.setLevel(logging.INFO)
        if debug:
            logger.setLevel(logging.DEBUG)
        logger.info("This is sdm2influx %s", __version__)
        return logger

    @staticmethod
    def parse_arguments(command_line: list[str]) -> argparse.Namespace:
        """reads command line arguments"""
        arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        arg_parser.add_argument("-D", "--debug", action="store_true", help="enable debug logging")
        arg_parser.add_argument(
            "-d",
            "--database",
            metavar="DB",
            type=str,
            default="energymeters",
            help="InfluxDB database name",
        )
        arg_parser.add_argument(
            "-i",
            "--influxdb",
            metavar="HOST",
            type=str,
            default="127.0.0.1",
            help="InfluxDB host (empty string disables writing to InfluxDB)",
        )
        arg_parser.add_argument(
            "-p",
            "--production",
            action="store_true",
            help="enable meter 2 for energy production",
        )
        arg_parser.add_argument(
            "-S",
            "--storage",
            action="store_true",
            help="enable meter 3 for energy storage",
        )
        arg_parser.add_argument(
            "-s",
            "--serial",
            metavar="DEV",
            type=str,
            default="/dev/ttyUSB0",
            help="modbus serial device",
        )
        arg_parser.add_argument(
            "-t",
            "--timeout",
            metavar="TIME",
            type=float,
            default=0.5,
            help="modbus timeout",
        )
        arg_parser.add_argument("-v", "--version", action="version", version="%(prog)s " + __version__)
        arg_parser.add_argument(
            "-z",
            "--zeromq",
            action="store_true",
            help="enable publishing data on ZeroMQ",
        )
        args = arg_parser.parse_args(args=command_line)
        return args


def main() -> None:
    sdm2influx = Sdm2Influx()
    args = sdm2influx.parse_arguments(sys.argv[1:])
    sdm2influx.init_logging(debug=args.debug)
    sdm2influx.main(args)


if __name__ == "__main__":
    main()
