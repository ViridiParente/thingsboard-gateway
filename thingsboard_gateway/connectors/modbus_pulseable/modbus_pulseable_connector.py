import time

from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
from pymodbus.register_write_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.bit_read_message import ReadBitsResponseBase

from thingsboard_gateway.connectors.modbus.modbus_connector import ModbusConnector
from thingsboard_gateway.connectors.modbus.constants import *
from thingsboard_gateway.connectors.connector import log

class ModbusPulseableConnector(ModbusConnector):
    def __pulse_values(self, content, device, rpc_command_config):
        end_value = rpc_command_config.get("end_value")
        pulsed_value = rpc_command_config.get("pulsed_value")
        command_configs = [{**rpc_command_config,**{"value":val}} for val in (pulsed_value, end_value)]
        success = True
        for idx, command_config in enumerate(command_configs):
            if command_config.get(FUNCTION_CODE_PARAMETER) in (5, 6):
                converted_data = device.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(command_config,
                                                                                              content)
                try:
                    command_config[PAYLOAD_PARAMETER] = converted_data[0]
                except IndexError and TypeError:
                    command_config[PAYLOAD_PARAMETER] = converted_data
            elif command_config.get(FUNCTION_CODE_PARAMETER) in (15, 16):
                converted_data = device.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(command_config,
                                                                                              content)
                converted_data.reverse()
                command_config[PAYLOAD_PARAMETER] = converted_data
            response = self.__function_to_device(device, command_config)
            success &= isinstance(response, (WriteMultipleRegistersResponse,
                                            WriteMultipleCoilsResponse,
                                            WriteSingleCoilResponse,
                                            WriteSingleRegisterResponse))
            if not idx:
                time.sleep(rpc_command_config.get("wait_time"))
        
        assert success, "Failed to pulse values"
        return response
        
        
        

    def __process_request(self, content, rpc_command_config, request_type='RPC'):
        log.debug('Processing %s request', request_type)
        if rpc_command_config is not None:
            device = tuple(filter(lambda slave: slave.name == content[DEVICE_SECTION_PARAMETER], self.__slaves))[0]
            rpc_command_config[UNIT_ID_PARAMETER] = device.config['unitId']
            rpc_command_config[BYTE_ORDER_PARAMETER] = device.config.get("byteOrder", "LITTLE")
            rpc_command_config[WORD_ORDER_PARAMETER] = device.config.get("wordOrder", "LITTLE")
            self.__connect_to_current_master(device)

            if rpc_command_config.get("pulse"):
                pass
            elif rpc_command_config.get(FUNCTION_CODE_PARAMETER) in (5, 6):
                converted_data = device.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(rpc_command_config,
                                                                                              content)
                try:
                    rpc_command_config[PAYLOAD_PARAMETER] = converted_data[0]
                except IndexError and TypeError:
                    rpc_command_config[PAYLOAD_PARAMETER] = converted_data
            elif rpc_command_config.get(FUNCTION_CODE_PARAMETER) in (15, 16):
                converted_data = device.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(rpc_command_config,
                                                                                              content)
                converted_data.reverse()
                rpc_command_config[PAYLOAD_PARAMETER] = converted_data

            try:
                if rpc_command_config.get("pulse"):
                    response = self.__pulse_values(content, device, rpc_command_config)
                else:
                    response = self.__function_to_device(device, rpc_command_config)
            except Exception as e:
                log.exception(e)
                response = e

            if isinstance(response, (ReadRegistersResponseBase, ReadBitsResponseBase)):
                to_converter = {
                    RPC_SECTION: {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: {"data_sent": rpc_command_config,
                                                                                  "input_data": response}}}
                response = device.config[
                    UPLINK_PREFIX + CONVERTER_PARAMETER].convert(
                    config={**device.config,
                            BYTE_ORDER_PARAMETER: device.byte_order,
                            WORD_ORDER_PARAMETER: device.word_order
                            },
                    data=to_converter)
                log.debug("Received %s method: %s, result: %r", request_type,
                          content[DATA_PARAMETER][RPC_METHOD_PARAMETER],
                          response)
            elif isinstance(response, (WriteMultipleRegistersResponse,
                                       WriteMultipleCoilsResponse,
                                       WriteSingleCoilResponse,
                                       WriteSingleRegisterResponse)):
                log.debug("Write %r", str(response))
                response = {"success": True}

            if content.get(RPC_ID_PARAMETER) or (
                    content.get(DATA_PARAMETER) is not None and content[DATA_PARAMETER].get(RPC_ID_PARAMETER)):
                if isinstance(response, Exception):
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: str(response)})
                else:
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  response)

            log.debug("%r", response)