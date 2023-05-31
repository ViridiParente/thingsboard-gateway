#     Copyright 2022. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the 'License');
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an 'AS IS' BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from __future__ import annotations

import asyncio
import re
import sched
import time
from collections.abc import Iterator
from copy import copy
from random import choice
from string import ascii_lowercase
from threading import Thread
from typing import Any, Optional, Literal, Union

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

from aioisotp import ISOTPNetwork

from thingsboard_gateway.connectors.isotp.isotp_converter import IsotpConverter
from thingsboard_gateway.connectors.isotp.bytes_isotp_downlink_converter import BytesIsotpDownlinkConverter
from thingsboard_gateway.connectors.isotp.bytes_isotp_uplink_converter import BytesIsotpUplinkConverter
from thingsboard_gateway.connectors.connector import Connector, log

def parse_int_literal(lit: str) -> int:
    return int(lit,
        16 if lit.startswith('0x') else
        8 if lit.startswith('0o') else
        2 if lit.startswith('0b') else
        10)

class IsotpConnector(Connector, Thread):

    __devices: dict[str, Device]
    __rx_ids: dict[int, Device]
    loop: asyncio.AbstractEventLoop
    network: ISOTPNetwork
    __net_conf: dict[str, Any]
    poll_lock: asyncio.Lock
    poll_delay: Union[int, float]

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.setName(config.get('name', 'ISO-TP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.gateway = gateway
        self._connector_type = connector_type
        self.__config = config
        self.daemon = True
        self.__connected = False
        self.loop = asyncio.new_event_loop()
        self.__devices = {}
        self.__rx_ids = {}
        self.poll_lock = asyncio.Lock()
        self.poll_delay = 1
        self.__parse_config(config)

    def open(self):
        log.info('[%s] Starting...', self.get_name())
        self.start()

    def close(self):
        self.loop.stop()
        log.debug('[%s] Stopping', self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.network = ISOTPNetwork(**self.__net_conf)
        self.network.open()
        self.loop.run_until_complete(asyncio.gather(*(
            device.open()
            for device in self.__devices.values()
        )))
        log.info('[%s] Connected', self.get_name())
        self.__connected = True
        self.loop.run_forever()
        log.info('[%s] Stopped', self.get_name())

    def is_stopped(self):
        return self.__stopped

    def __parse_config(self, config):
        self.__net_conf = {
            'interface': config.get('interface', 'socketcan'),
            'channel': config.get('channel', 'vcan0'),
        }
        self.__net_conf.update(config.get('backend', {}))

        poll_delay = config.get('pollDelay', self.poll_delay)
        assert isinstance(self.poll_delay, (int, float))
        self.poll_delay = poll_delay

        for device_config in config.get('devices', []):
            name = device_config.get('name')
            if not isinstance(name, str):
                self.log.warning('Invalid device name: %r', name)
                continue

            try:
                devtype = device_config.get('type', self._connector_type)
                assert isinstance(devtype, str), f'Invalid device type: {devtype!r}'

                tx_id = device_config.get('txId')
                if isinstance(tx_id, str):
                    tx_id = parse_int_literal(tx_id)
                assert isinstance(tx_id, int), f'Invalid tx id: {tx_id!r}'

                rx_id = device_config.get('rxId')
                if isinstance(rx_id, str):
                    rx_id = parse_int_literal(rx_id)
                assert isinstance(rx_id, int), f'Invalid rx id: {rx_id!r}'

                send_on_change = device_config.get('sendOnlyOnChange', False)
                assert isinstance(send_on_change, bool), f'Invalid sendOnlyOnChange: {send_on_change!r}'

                case_tree = CaseTreeNode.parse(device_config.get('casetree'))

                polls = device_config.get('polling')
                if polls is not None:
                    assert isinstance(polls, list), f'Invalid polling config: {polls!r}'
                    polling = [PollConfig.parse(poll) for poll in polls]
                else:
                    polling = []

                device = Device(
                    connector = self,
                    name = name,
                    type = devtype,
                    tx_id = tx_id,
                    rx_id = rx_id,
                    case_tree = case_tree,
                    send_on_change = send_on_change,
                    polling = polling,
                    uplink_converter = self.__get_converter(config.get('converters'), True),
                )

                self.__devices[device.name] = device
            except Exception as e:
                log.exception('[%s] Parsing \'%s\' device configuration failed',
                              self.get_name(), name)
            else:
                log.debug('[%s] Done parsing of \'%s\' device configuration',
                          self.get_name(), device.name)
                self.gateway.add_device(device.name, {'connector': self})

    def __get_converter(self, config, need_uplink):
        if config is None:
            return BytesIsotpUplinkConverter() if need_uplink else BytesIsotpDownlinkConverter()
        else:
            if need_uplink:
                uplink = config.get('uplink')
                return BytesIsotpUplinkConverter() if uplink is None \
                    else TBModuleLoader.import_module(self._connector_type, uplink)
            else:
                downlink = config.get('downlink')
                return BytesIsotpDownlinkConverter() if downlink is None \
                    else TBModuleLoader.import_module(self._connector_type, downlink)

    def get_config(self):
        return self.__config

    def on_attributes_update(self, content):
        pass # TODO

    def server_side_rpc_handler(self, content):
        pass # TODO


class Device(asyncio.Protocol):
    connector: IsotpConnector
    name: str
    type: str
    tx_id: int
    rx_id: int
    case_tree: CaseTreeNode
    send_on_change: bool
    polling: list[PollConfig]

    uplink_converter: IsotpConverter
    transport: Optional[asyncio.Transport] = None

    old_data: dict[str, Any]

    def __init__(self, *, connector: IsotpConnector, name: str, type: str, tx_id: int,
                 rx_id: int, case_tree: CaseTreeNode, send_on_change: bool,
                 polling: list[PollConfig], uplink_converter: IsotpConverter) -> None:
        self.connector = connector
        self.name = name
        self.type = type
        self.tx_id = tx_id
        self.rx_id = rx_id
        self.case_tree = case_tree
        self.send_on_change = send_on_change
        self.polling = polling
        self.uplink_converter = uplink_converter

        self.old_data = {}

    async def open(self) -> None:
        self.transport, protocol = await self.connector.network.create_connection(
                lambda: self, self.rx_id, self.tx_id)
        log.debug('[%s] Opened "connection" to %x (RX) / %x (TX)',
                  self.connector.get_name(), self.rx_id, self.tx_id)

        def done_cb(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is None:
                return
            log.error('[%s] Poll task failed', self.connector.get_name(), exc_info=exc)

        for poll_config in self.polling:
            asyncio.create_task(self.bg_poll(poll_config)).add_done_callback(done_cb)

    async def bg_poll(self, poll_config: PollConfig) -> None:
        while True:
            assert self.transport is not None
            log.debug('[%s] Polling: %s',
                      self.connector.get_name(), poll_config.data.hex(' '))

            async with self.connector.poll_lock:
                self.transport.write(poll_config.data)
                await asyncio.sleep(self.connector.poll_delay)

            if poll_config.period is None:
                break
            else:
                await asyncio.sleep(poll_config.period)

    def data_received(self, data: bytes) -> None:
        log.debug('[%s] Got ISO-TP message from %x: %s',
                  self.connector.get_name(), self.rx_id, data.hex(' '))

        endpoints = list(self.case_tree.match(data))
        if not endpoints:
            return

        try:
            converted = self.uplink_converter.convert(endpoints, data)
            if converted is None or not (converted.get('attributes') or
                                         converted.get('telemetry')):
                log.warning(
                    '[%s] Failed to process ISO-TP message (id=%x): '
                    'data conversion failure',
                    self.connector.get_name(), self.rx_id)
            else:
                self.connector.statistics['MessagesReceived'] += 1
                to_send: dict = {'attributes': [], 'telemetry': []}
                for tb_key in to_send:
                    for key, value in converted.get(tb_key, {}).items():
                        if not self.send_on_change or self.old_data[key] != value:
                            self.old_data[key] = value
                            to_send[tb_key].append({key: value})

                if to_send['attributes'] or to_send['telemetry']:
                    to_send['deviceName'] = self.name
                    to_send['deviceType'] = self.type

                    log.debug('[%s] Pushing to TB server \'%s\' device data: %s',
                              self.connector.get_name(), self.name, to_send)

                    self.connector.gateway.send_to_storage(
                        self.connector.get_name(), to_send)
                    self.connector.statistics['MessagesSent'] += 1
                else:
                    log.debug('[%s] \'%s\' device data has not been changed',
                              self.connector.get_name(), self.name)
        except:
            log.exception('[%s] \'%s\' parsing failed',
                          self.connector.get_name(), self.name)

class PollConfig:
    data: bytes
    period: Union[None, int, float]

    def __init__(self, *, data: bytes, period: Optional[float]) -> None:
        self.data = data
        self.period = period

    @classmethod
    def parse(cls, config: dict) -> PollConfig:
        data_str = config.get('data')
        assert isinstance(data_str, str), f'Invalid data: {data_str!r}'
        data = bytes.fromhex(data_str)

        period = config.get('period')
        assert isinstance(period, (type(None), float, int))

        return PollConfig(data = data, period = period)

class CaseTreeSplit:
    start: int
    length: int
    byteorder: Literal['big', 'little']
    mask: int

    children: dict[int, CaseTreeNode]

    def __init__(self, *, start: int, length: int, byteorder: Literal['big', 'little'],
                 mask: int) -> None:
        self.start = start
        self.length = length
        self.byteorder = byteorder
        self.mask = mask
        self.children = {}

    @classmethod
    def parse(cls, config: dict) -> CaseTreeSplit:
        start = config.get('start', 0)
        assert isinstance(start, int), f'Invalid start: {start!r}'

        length = config.get('length', 1)
        assert isinstance(length, int), f'Invalid length: {length!r}'

        byteorder = config.get('byteorder', 'big')
        assert byteorder in ('big', 'little'), f'Invalid byte order: {byteorder!r}'

        mask = config.get('mask', 2**(8 * length + 1) - 1)
        assert isinstance(mask, int), f'Invalid mask: {mask!r}'

        split = CaseTreeSplit(
            start = start,
            length = length,
            byteorder = byteorder,
            mask = mask,
        )

        children = config.get('children', [])
        assert isinstance(children, list), f'Invalid children: {children!r}'
        for child in children:
            assert isinstance(child, dict), f'Invalid child: {child!r}'
            value = child.pop('splitValue')
            if isinstance(value, str):
                value = parse_int_literal(value)
            assert isinstance(value, int), f'Invalid split value: {value!r}'
            assert value not in split.children, f'Duplicate child value: {value!r}'
            split.children[value] = CaseTreeNode.parse(child)

        return split

    def match(self, data: bytes) -> Optional[CaseTreeNode]:
        value = int.from_bytes(data[self.start:self.start + self.length],
                               self.byteorder)
        value &= self.mask
        return self.children.get(value)

class CaseTreeNode:
    endpoints: list[dict]
    splits: list[CaseTreeSplit]

    def __init__(self) -> None:
        self.endpoints = []
        self.splits = []

    @classmethod
    def parse(cls, config: object) -> CaseTreeNode:
        assert isinstance(config, dict), f'Invalid case tree node: {config!r}'

        node = cls()

        endpoints = config.get('endpoints', [])
        assert isinstance(endpoints, list), f'Invalid endpoints: {endpoints!r}'
        node.endpoints = endpoints

        splits = config.get('splits', [])
        assert isinstance(splits, list), f'Invalid splits: {splits!r}'
        node.splits = [CaseTreeSplit.parse(split) for split in splits]

        return node

    def match(self, data: bytes) -> Iterator[dict]:
        yield from self.endpoints

        for split in self.splits:
            child = split.match(data)
            if child is not None:
                yield from child.match(data)
