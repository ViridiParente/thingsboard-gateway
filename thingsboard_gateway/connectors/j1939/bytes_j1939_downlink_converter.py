#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License"];
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

import struct

from thingsboard_gateway.connectors.j1939.j1939_converter import J1939Converter
from thingsboard_gateway.connectors.converter import log
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class BytesJ1939DownlinkConverter(J1939Converter):
    @StatisticsService.CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                                         end_stat_type='allBytesSentToDevices')
    def convert(self, config, data):
        try:
            if config.get("dataInHex", ""):
                return list(bytearray.fromhex(config["dataInHex"]))

            if not isinstance(data, dict) or not data:
                log.error("Failed to convert TB data to J1939 payload: data is empty or not a dictionary")
                return

            if data.get("dataInHex", ""):
                return list(bytearray.fromhex(data["dataInHex"]))

            if config.get("dataExpression", ""):
                value = eval(config["dataExpression"],
                             {"__builtins__": {}} if config.get("strictEval", True) else globals(),
                             data)
            elif "value" in data:
                value = data["value"]
            else:
                log.error("Failed to convert TB data to J1939 payload: no `value` or `dataExpression` property")
                return

            j1939_data = []

            if config.get("dataBefore", ""):
                j1939_data.extend(bytearray.fromhex(config["dataBefore"]))

            if isinstance(value, bool):
                j1939_data.extend([int(value)])
            elif isinstance(value, int) or isinstance(value, float):
                byteorder = config["dataByteorder"] if config.get("dataByteorder", "") else "big"
                if isinstance(value, int):
                    j1939_data.extend(value.to_bytes(config.get("dataLength", 1),
                                                   byteorder,
                                                   signed=(config.get("dataSigned", False) or value < 0)))
                else:
                    j1939_data.extend(struct.pack(">f" if byteorder[0] == "b" else "<f", value))
            elif isinstance(value, str):
                j1939_data.extend(value.encode(config["dataEncoding"] if config.get("dataEncoding", "") else "ascii"))

            if config.get("dataAfter", ""):
                j1939_data.extend(bytearray.fromhex(config["dataAfter"]))

            return j1939_data
        except Exception as e:
            log.error("Failed to convert TB data to J1939 payload: %s", str(e))
            return
