#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import re
import sched
import time
from copy import copy
from random import choice
from string import ascii_lowercase
from threading import Thread, Event

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.constants import *

from thingsboard_gateway.connectors.j1939.bytes_j1939_downlink_converter import BytesJ1939DownlinkConverter
from thingsboard_gateway.connectors.j1939.bytes_j1939_uplink_converter import BytesJ1939UplinkConverter
from thingsboard_gateway.connectors.connector import Connector, log

import j1939


class J1939Connector(Connector, Thread):
    VALUE_REGEX = r"^(\d{1,2}):((?:-1)?|\d{1,2}):?(big|little)?:(bool|boolean|int|long|float|double|string|raw):?([0-9A-Za-z-_]+)?$"

    DEFAULT_RECONNECT_PERIOD = 30.0
    DEFAULT_POLL_PERIOD = 1.0

    DEFAULT_SEND_IF_CHANGED = False
    DEFAULT_RECONNECT_STATE = True

    DEFAULT_EXTENDED_ID_FLAG = False
    DEFAULT_FD_FLAG = False
    DEFAULT_BITRATE_SWITCH_FLAG = False

    DEFAULT_BYTEORDER = "big"
    DEFAULT_ENCODING = "ascii"

    DEFAULT_ENABLE_UNKNOWN_RPC = False
    DEFAULT_OVERRIDE_RPC_PARAMS = False
    DEFAULT_STRICT_EVAL_FLAG = True

    DEFAULT_SIGNED_FLAG = False

    DEFAULT_RPC_RESPONSE_SEND_FLAG = False

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.setName(config.get("name", 'J1939 Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__config = config
        self.__bus_conf = {}
        self.__reconnect_count = 0
        self.__reconnect_conf = {}
        self.__devices = {}
        self.__nodes = {}
        self.__commands = {}
        self.__polling_messages = []
        self.__rpc_calls = {}
        self.__shared_attributes = {}
        self.__converters = {}
        self.__bus_error = None
        self.__connected = False
        self.__stopped = True
        self.daemon = True
        self.__parse_config(config)

        self.__ecu = j1939.ElectronicControlUnit()
        self.__ca = None
        self.__scheduler = sched.scheduler(time.time, time.sleep)
        self.__reset_event = Event()

        self.__ecu.subscribe(self.__process_message)

    def open(self):
        log.info("[%s] Starting...", self.get_name())
        self.__stopped = False
        self.start()

    def close(self):
        if not self.__stopped:
            self.__stopped = True
            for event in self.__scheduler.queue:
                self.__scheduler.cancel(event)
            self.__reset_event.set()
            log.debug("[%s] Stopping", self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def run(self):
        first = True
        while not self.__stopped:
            self.__connected = False

            if not first:
                if not self.__is_reconnect_enabled():
                    break

                retry_period = self.__reconnect_conf["period"]
                log.info("[%s] Next attempt to connect will be in %f seconds (%s attempt left)",
                         self.get_name(), retry_period,
                         "infinite" if self.__reconnect_conf["maxCount"] is None
                         else self.__reconnect_conf["maxCount"] - self.__reconnect_count + 1)
                time.sleep(retry_period)

            first = False

            try:
                self.__ecu.connect(**self.__bus_conf)
            except Exception as e:
                log.error("[%s] Error connecting to J1939 bus: %s", self.get_name(), str(e))
                continue

            log.info("[%s] Connected to J1939 bus", self.get_name())

            # Initialize the connected flag and reconnect count only after bus creation and sending poll messages.
            # It is expected that after these operations most likely the bus is up.
            self.__connected = True
            self.__reconnect_count = 0

            if self.__ca is not None:
                for polling_config in self.__polling_messages:
                    self.__poll(polling_config)

            self.__scheduler.run()
            self.__reset_event.wait()
            self.__reset_event.clear()

            log.info("[%s] Disconnecting from J1939 bus", self.get_name())
            
            self.__ecu.disconnect()

        log.info("[%s] Stopped", self.get_name())

    def is_stopped(self):
        return self.__stopped

    def __is_reconnect_enabled(self):
        if self.__reconnect_conf["enabled"]:
            if self.__reconnect_conf["maxCount"] is None:
                return True
            self.__reconnect_count += 1
            return self.__reconnect_conf["maxCount"] >= self.__reconnect_count
        else:
            return False

    def __poll(self, config):
        ca_up = self.__ca.state == j1939.ControllerApplication.State.NORMAL
        if ca_up:
            try:
                self.__ca.send_request(config["data_page"], config["pgn"], j1939.ParameterGroupName.Address.GLOBAL)
            except CanError as e:
                log.error("[%s] Failed to send J1939 message: %s", self.get_name(), str(e))
                self.close()

        if not ca_up or config["type"] == "always":
            self.__scheduler.enter(config["period"], 1, self.__poll, argument=(config,))

    def __process_message(self, priority, pgn, sa, timestamp, data):
        if pgn not in self.__nodes:
            # Too lot log messages in case of high message generation frequency
            log.debug("[%s] Ignoring J1939 PDU. Unknown PGN %d", self.get_name(), pgn)
            return

        log.debug("[%s] Processing J1939 message (PGN=%d)", self.get_name(), pgn)

        parsing_conf = self.__nodes[pgn]
        data = self.__converters[parsing_conf["deviceName"]]["uplink"].convert(parsing_conf["configs"], data)
        if data is None or not data.get("attributes", []) and not data.get("telemetry", []):
            log.warning("[%s] Failed to process J1939 message (PGN=%d): data conversion failure",
                        self.get_name(), pgn)
            return

        self.__check_and_send(parsing_conf, data, timestamp)

    def __check_and_send(self, conf, new_data, timestamp):
        self.statistics['MessagesReceived'] += 1
        to_send = {"attributes": [], "telemetry": []}
        send_on_change = conf["sendOnChange"]

        for tb_key in to_send.keys():
            for key, new_value in new_data[tb_key].items():
                if not send_on_change or self.__devices[conf["deviceName"]][tb_key][key] != new_value:
                    self.__devices[conf["deviceName"]][tb_key][key] = new_value
                    to_send[tb_key].append({key: new_value})

        if to_send["attributes"] or to_send["telemetry"]:
            to_send["deviceName"] = conf["deviceName"]
            to_send["deviceType"] = conf["deviceType"]

            log.debug("[%s] Pushing to TB server '%s' device data: %s", self.get_name(), conf["deviceName"], to_send)

            to_send["telemetry"] = [
                {
                    TELEMETRY_TIMESTAMP_PARAMETER: timestamp * 1000,
                    TELEMETRY_VALUES_PARAMETER: { key: val },
                }
                for ts_kv in to_send["telemetry"]
                for key, val in ts_kv.items()
            ]

            self.__gateway.send_to_storage(self.get_name(), to_send)
            self.statistics['MessagesSent'] += 1
        else:
            log.debug("[%s] '%s' device data has not been changed", self.get_name(), conf["deviceName"])

    def __parse_config(self, config):
        self.__reconnect_count = 0
        self.__reconnect_conf = {
            "enabled": config.get("reconnect", self.DEFAULT_RECONNECT_STATE),
            "period": config.get("reconnectPeriod", self.DEFAULT_RECONNECT_PERIOD),
            "maxCount": config.get("reconnectCount", None)
            }

        self.__bus_conf = {
            "interface": config.get("interface", "socketcan"),
            "channel": config.get("channel", "vcan0"),
            }
        self.__bus_conf.update(config.get("backend", {}))

        # CA name must be set for polling, RPCs or shared attributes
        ca_name = config.get("ca_name")
        if ca_name is not None:
            ca_name = j1939.Name(**ca_name)
            self.__ca = j1939.ControllerApplication(name = ca_name)

        for device_config in config.get("devices"):
            is_device_config_valid = False
            device_name = device_config["name"]
            device_type = device_config.get("type", self._connector_type)
            strict_eval = device_config.get("strictEval", self.DEFAULT_STRICT_EVAL_FLAG)

            self.__devices[device_name] = {}
            self.__devices[device_name]["enableUnknownRpc"] = device_config.get("enableUnknownRpc",
                                                                                self.DEFAULT_ENABLE_UNKNOWN_RPC)
            self.__devices[device_name]["overrideRpcConfig"] = True if self.__devices[device_name]["enableUnknownRpc"] \
                else device_config.get("overrideRpcConfig", self.DEFAULT_OVERRIDE_RPC_PARAMS)

            self.__converters[device_name] = {}

            if not strict_eval:
                log.info("[%s] Data converters for '%s' device will use non-strict eval", self.get_name(), device_name)

            for config_key in ["timeseries", "attributes"]:
                if config_key not in device_config or not device_config[config_key]:
                    continue

                is_device_config_valid = True
                is_ts = (config_key[0] == "t")
                tb_item = "telemetry" if is_ts else "attributes"

                self.__devices[device_name][tb_item] = {}

                if "uplink" not in self.__converters[device_name]:
                    self.__converters[device_name]["uplink"] = self.__get_converter(device_config.get("converters"),
                                                                                    True)
                for msg_config in device_config[config_key]:
                    tb_key = msg_config["key"]
                    msg_config["strictEval"] = strict_eval
                    msg_config["is_ts"] = is_ts

                    pgn = msg_config.get("pgn")
                    if pgn is None:
                        log.warning("[%s] Ignore '%s' %s configuration: no PGN",
                                    self.get_name(), tb_key, config_key)
                        continue

                    value_config = self.__parse_value_config(msg_config.get("value"))
                    if value_config is not None:
                        msg_config.update(value_config)
                    else:
                        log.warning("[%s] Ignore '%s' %s configuration: no value configuration",
                                    self.get_name(), tb_key, config_key, )
                        continue

                    if pgn not in self.__nodes:
                        self.__nodes[pgn] = {
                            "deviceName": device_name,
                            "deviceType": device_type,
                            "sendOnChange": device_config.get("sendDataOnlyOnChange", self.DEFAULT_SEND_IF_CHANGED),
                            "configs": [],
                        }

                    self.__nodes[pgn]["configs"].append(msg_config)
                    self.__devices[device_name][tb_item][tb_key] = None

                    if "polling" in msg_config:
                        try:
                            polling_config = msg_config.get("polling")
                            polling_config["key"] = tb_key  # Just for logging
                            polling_config["type"] = polling_config.get("type", "always")
                            polling_config["period"] = polling_config.get("period", self.DEFAULT_POLL_PERIOD)
                            polling_config["pgn"] = pgn
                            self.__polling_messages.append(polling_config)
                        except (ValueError, TypeError) as e:
                            log.warning("[%s] Ignore '%s' %s polling configuration: %s",
                                        self.get_name(), tb_key, config_key, str(e))
                            continue

            if is_device_config_valid:
                log.debug("[%s] Done parsing of '%s' device configuration", self.get_name(), device_name)
                self.__gateway.add_device(device_name, {"connector": self})
            else:
                log.warning("[%s] Ignore '%s' device configuration, because it doesn't have attributes,"
                            "attributeUpdates,timeseries or serverSideRpc", self.get_name(), device_name)

    def __parse_value_config(self, config):
        if config is None:
            log.warning("[%s] Wrong value configuration: no data", self.get_name())
            return

        if isinstance(config, str):
            value_matches = re.search(self.VALUE_REGEX, config)
            if not value_matches:
                log.warning("[%s] Wrong value configuration: '%s' doesn't match pattern", self.get_name(), config)
                return

            value_config = {
                "start": int(value_matches.group(1)),
                "length": int(value_matches.group(2)),
                "byteorder": value_matches.group(3) if value_matches.group(3) else self.DEFAULT_BYTEORDER,
                "type": value_matches.group(4)
                }

            if value_config["type"][0] == "i" or value_config["type"][0] == "l":
                value_config["signed"] = value_matches.group(5) == "signed" if value_matches.group(5) \
                    else self.DEFAULT_SIGNED_FLAG
            elif value_config["type"][0] == "s" or value_config["type"][0] == "r":
                value_config["encoding"] = value_matches.group(5) if value_matches.group(5) else self.DEFAULT_ENCODING

            return value_config
        elif isinstance(config, dict):
            try:
                value_config = {
                    "start": int(config["start"]),
                    "length": int(config["length"]),
                    "byteorder": config["byteorder"] if config.get("byteorder", "") else self.DEFAULT_BYTEORDER,
                    "type": config["type"]
                    }

                if value_config["type"][0] == "i" or value_config["type"][0] == "l":
                    value_config["signed"] = config.get("signed", self.DEFAULT_SIGNED_FLAG)
                elif value_config["type"][0] == "s":
                    value_config["encoding"] = config["encoding"] if config.get("encoding", "") else self.DEFAULT_ENCODING

                return value_config
            except (KeyError, ValueError) as e:
                log.warning("[%s] Wrong value configuration: %s", self.get_name(), str(e))
                return
        log.warning("[%s] Wrong value configuration: unknown type", self.get_name())
        return

    def __get_converter(self, config, need_uplink):
        if config is None:
            return BytesJ1939UplinkConverter() if need_uplink else BytesJ1939DownlinkConverter()
        else:
            if need_uplink:
                uplink = config.get("uplink")
                return BytesJ1939UplinkConverter() if uplink is None \
                    else TBModuleLoader.import_module(self._connector_type, uplink)
            else:
                downlink = config.get("downlink")
                return BytesJ1939DownlinkConverter() if downlink is None \
                    else TBModuleLoader.import_module(self._connector_type, downlink)

    def get_config(self):
        return self.__config

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
