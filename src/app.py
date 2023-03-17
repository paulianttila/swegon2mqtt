from mqtt_framework import Framework
from mqtt_framework import Config
from mqtt_framework.callbacks import Callbacks
from mqtt_framework.app import TriggerSource

from prometheus_client import Counter

import threading
import socket
import struct
import binascii
import difflib
import ctypes
from cacheout import Cache
from crccheck.crc import Crc16CcittFalse


class MyConfig(Config):
    def __init__(self):
        super().__init__(self.APP_NAME)

    APP_NAME = "swegon2mqtt"

    # App specific variables

    UDP_PORT = 9999
    CACHE_TIME = 300
    MSG_THROTTLE_TIME = 5
    ANALYZER_MODE = False


class MyApp:
    def init(self, callbacks: Callbacks) -> None:
        self.logger = callbacks.get_logger()
        self.config = callbacks.get_config()
        self.metrics_registry = callbacks.get_metrics_registry()
        self.add_url_rule = callbacks.add_url_rule
        self.publish_value_to_mqtt_topic = callbacks.publish_value_to_mqtt_topic
        self.subscribe_to_mqtt_topic = callbacks.subscribe_to_mqtt_topic
        self.received_messages_metric = Counter(
            "received_messages", "", registry=self.metrics_registry
        )
        self.received_messages_errors_metric = Counter(
            "received_message_errors", "", registry=self.metrics_registry
        )
        self.exit = False

        self.udp_receiver = None
        self.messageCache = Cache(maxsize=256, ttl=self.config["MSG_THROTTLE_TIME"])
        self.valueCache = Cache(maxsize=256, ttl=self.config["CACHE_TIME"])
        self.messagesForDiff = {}
        self.analyzer_mode = False
        if self.config["ANALYZER_MODE"].lower() == "true":
            self.analyzer_mode = True

    def get_version(self) -> str:
        return "2.0.0"

    def stop(self) -> None:
        self.logger.debug("Stopping...")
        self.exit = True
        if self.udp_receiver:
            self.udp_receiver.stop()
            if self.udp_receiver.is_alive():
                self.udp_receiver.join()
        self.logger.debug("Exit")

    def subscribe_to_mqtt_topics(self) -> None:
        self.subscribe_to_mqtt_topic("analyzerMode")

    def mqtt_message_received(self, topic: str, message: str) -> None:
        if topic == "analyzerMode":
            self.analyzer_mode = message.lower() in {"yes", "true", "1"}

    def do_healthy_check(self) -> bool:
        return self.udp_receiver.is_alive()

    # Do work
    def do_update(self, trigger_source: TriggerSource) -> None:
        self.logger.debug("update called, trigger_source=%s", trigger_source)
        if trigger_source == trigger_source.MANUAL:
            self.valueCache.clear()

        if self.udp_receiver is None:
            self.logger.info("Start UDP receiver")
            self.udp_receiver = threading.Thread(target=self.start, daemon=True)
            self.udp_receiver.start()

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", self.config["UDP_PORT"]))  # nosec
        self.logger.debug("Waiting data from UDP port %d", self.config["UDP_PORT"])

        while not self.exit:
            try:
                data, addr = sock.recvfrom(1024)  # buffer size is 1024 bytes
                self.received_messages_metric.inc()

                if not data:
                    self.logger.debug("No data")
                    continue
                self.handle_data(data)
            except Exception as e:
                self.received_messages_errors_metric.inc()
                self.logger.error(f"Error occured: {e}")
                self.logger.debug(f"Error occured: {e}", exc_info=True)

        self.logger.debug("UDP receiver stopped")

    def publish_value(self, key, value):
        previousvalue = self.valueCache.get(key)
        publish = False
        if previousvalue is None:
            self.logger.debug("%s: no cache value available", key)
            publish = True
        elif value == previousvalue:
            self.logger.debug("%s = %s : skip update because of same value", key, value)
        else:
            publish = True

        if publish:
            self.logger.info("%s = %s", key, value)
            self.publish_value_to_mqtt_topic(key, value, False)
            self.valueCache.set(key, value)

    def parse_message(self, msg):
        unpackStr = "<BxBBBBxx{0}s".format(len(msg) - 8)
        self.logger.debug(
            "unpackStr=%s, msg len=%s, msg type=%s, data=%s",
            unpackStr,
            len(msg),
            type(msg),
            binascii.hexlify(msg).upper(),
        )

        (
            firstByte,
            destinationAddress,
            sourceAddress,
            dataLen,
            msgType,
            msgData,
        ) = struct.unpack(unpackStr, msg)
        self.logger.debug(
            "firstByte=%02X, destinationAddress=%02X"
            ", sourceAddress=%02X, dataLen=%02X, msgType=%02X, msgData=%s",
            firstByte,
            destinationAddress,
            sourceAddress,
            dataLen,
            msgType,
            binascii.hexlify(msgData).upper(),
        )

        msgKey = "{0}{1}{2}".format(msgType, sourceAddress, destinationAddress)
        previousMsg = self.messageCache.get(msgKey, None)

        if previousMsg is None:
            if msgType == 0x21:
                self.parse_msg21(msgData)
            elif msgType == 0x71:
                self.parse_msg71(msgData)
            elif msgType == 0x73:
                self.parse_msg73(msgData)
            else:
                self.logger.debug("Unsupported first byte received: %02X" % firstByte)
            self.messageCache.set(msgKey, msg)
        else:
            self.logger.debug(f"Skip message parsing for msgKey: {msgKey}")

    def parse_msg21(self, msg):
        self.logger.debug(f"Parse message 0x21, data: {binascii.hexlify(msg).upper()}")
        data = bytearray(msg)
        self.publish_value("operatingMode", data[0])
        self.publish_value("unitState", data[1])
        self.publish_value("fanSpeed", data[3] & 0x0F)

    def parse_msg71(self, msg):
        self.logger.debug(f"Parse message 0x71, data: {binascii.hexlify(msg).upper()}")
        data = bytearray(msg)
        outdoorTemp = ctypes.c_int8(data[0]).value
        supplyTemp = ctypes.c_int8(data[1]).value
        extractTemp = ctypes.c_int8(data[2]).value
        exhaustTemp = ctypes.c_int8(data[7]).value

        calcSupplyEfficiency = 100
        calcExtractEfficiency = 100

        if (extractTemp - outdoorTemp) != 0:
            calcSupplyEfficiency = int(
                (float(supplyTemp - outdoorTemp) / float(extractTemp - outdoorTemp))
                * 100
            )
            calcExtractEfficiency = int(
                (float(extractTemp - exhaustTemp) / float(extractTemp - outdoorTemp))
                * 100
            )

        calcSupplyEfficiency = min(calcSupplyEfficiency, 100)
        calcSupplyEfficiency = max(calcSupplyEfficiency, 0)

        calcExtractEfficiency = min(calcExtractEfficiency, 100)
        calcExtractEfficiency = max(calcExtractEfficiency, 0)

        self.publish_value("outdoorTemp", outdoorTemp)
        self.publish_value("supplyTemp", supplyTemp)
        self.publish_value("extractTemp", extractTemp)
        self.publish_value("supplyTempHeated", ctypes.c_int8(data[3]).value)
        self.publish_value("t5", ctypes.c_int8(data[4]).value)
        self.publish_value("t6", ctypes.c_int8(data[5]).value)
        self.publish_value("t7", ctypes.c_int8(data[6]).value)
        self.publish_value("exhaustTemp", exhaustTemp)
        self.publish_value("co2", data[8])
        self.publish_value("humidity", data[9])
        self.publish_value("supplyFanSpeed", data[10] * 10)
        self.publish_value("extractFanSpeed", data[11] * 10)
        self.publish_value("efficiency", data[12])
        self.publish_value("calcSupplyEfficiency", calcSupplyEfficiency)
        self.publish_value("calcExtractEfficiency", calcExtractEfficiency)

    def parse_msg73(self, msg):
        self.logger.debug(f"Parse message 0x73, data: {binascii.hexlify(msg).upper()}")
        data = bytearray(msg)

        self.publish_value("heatingState", (data[0] & 0x01) > 0)
        self.publish_value("coolingState", (data[0] & 0x02) > 0)
        self.publish_value("bybassState", (data[0] & 0x04) > 0)
        self.publish_value("freezeProtectionState", (data[0] & 0x08) > 0)
        self.publish_value("preheatingState", (data[0] & 0x10) > 0)
        self.publish_value("chillingState", (data[0] & 0x20) > 0)
        self.publish_value("preheaterOverheatState", (data[0] & 0x40) > 0)
        self.publish_value("reheatingState", (data[0] & 0x80) > 0)
        self.publish_value("fireplaceFunctionState", (data[1] & 0x01) > 0)
        self.publish_value("underpressureCompensationState", (data[1] & 0x02) > 0)
        self.publish_value("externalBoostState", (data[1] & 0x04) > 0)
        self.publish_value("humidityBoostState", (data[1] & 0x08) > 0)
        self.publish_value("co2BoostState", (data[1] & 0x10) > 0)
        self.publish_value("defrostingState", (data[1] & 0x20) > 0)
        self.publish_value("defrostStarterMode", (data[1] & 0x40) > 0)
        self.publish_value("tfStopState", (data[1] & 0x80) > 0)
        self.publish_value("externalBoostFunctionState", (data[3] & 0x04) > 0)
        self.publish_value("externalFireplaceFunctionState", (data[3] & 0x08) > 0)
        self.publish_value("filterGuardStatus", (data[3] & 0x10) > 0)
        self.publish_value("irFreezeProtectionStatus", (data[3] & 0x20) > 0)
        self.publish_value("emergencyStopState", (data[3] & 0x80) > 0)
        self.publish_value("reheatingFreezingAlarm", (data[7] & 0x01) > 0)
        self.publish_value("reheatingOverheatAlarm", (data[7] & 0x02) > 0)
        self.publish_value("irSensorFailure", (data[7] & 0x04) > 0)
        self.publish_value("supplyFanFailure", (data[7] & 0x08) > 0)
        self.publish_value("extractFanFailure", (data[7] & 0x10) > 0)
        self.publish_value("temperatureDeviationFailure", (data[7] & 0x20) > 0)
        self.publish_value("efficinecyAlarm", (data[8] & 0x01) > 0)
        self.publish_value("filterGuardAlarm", (data[8] & 0x02) > 0)
        self.publish_value("serviceReminder", (data[8] & 0x04) > 0)
        self.publish_value("temperatureFailure", (data[8] & 0x08) > 0)
        self.publish_value("afterheatingSetpointSupplyAirRegulated", data[10])
        self.publish_value("afterheatingSetpointRoomRegulated", data[11])
        self.publish_value("supplyFanVirtualSpeed", data[12])
        self.publish_value("extractFanVirtualSpeed", data[13])
        self.publish_value("unitStatus", data[14])

    def handle_data(self, data):
        self.logger.debug(
            "Received data (type=%s): %s", type(data), binascii.hexlify(data).upper()
        )
        d = bytearray(data)

        # UDP message might contain multiple short messages

        elements = d.split(b"\xCC\x64")
        for msg in elements:
            if len(msg):
                self.handle_message(bytearray(b"\xCC\x64") + msg)

    def handle_message(self, data):
        if len(data) > 10 and data[0] == 0xCC:
            crcFromMsg = (data[-2] << 8) + data[-1]

            # copy payload from second byte to the end without crc checksum
            msg = data[1 : len(data) - 2]

            calcCrc = Crc16CcittFalse.calc(msg)

            if calcCrc == crcFromMsg:
                self.logger.debug(
                    "CRC OK, Message: %s, 0x%04X (crcFromMsg) == 0x%04X (calcCrc)",
                    binascii.hexlify(msg).upper(),
                    crcFromMsg,
                    calcCrc,
                )
                if self.analyzer_mode:
                    self.analyze_message(msg)
                self.parse_message(msg)
            elif self.analyzer_mode:
                self.logger.error(
                    "CRC FAILURE, PDU: %s, Message: %s"
                    ", 0x%04X (crcFromMsg) !=0x%04X (calcCrc)",
                    binascii.hexlify(data).upper(),
                    binascii.hexlify(msg).upper(),
                    crcFromMsg,
                    calcCrc,
                )
            else:
                self.logger.debug(
                    "CRC FAILURE, PDU: %s, Message: %s"
                    ", 0x%04X (crcFromMsg) !=0x%04X (calcCrc)",
                    binascii.hexlify(data).upper(),
                    binascii.hexlify(msg).upper(),
                    crcFromMsg,
                    calcCrc,
                )
        elif self.analyzer_mode:
            self.logger.error(
                "Invalid message received, PDU: %s", binascii.hexlify(data).upper()
            )
        else:
            self.logger.debug(
                "Invalid message received, PDU: %s", binascii.hexlify(data).upper()
            )

    def analyze_message(self, msg):
        unpackStr = "BxBBBBxx{0}s".format(len(msg) - 8)
        (
            firstByte,
            destinationAddress,
            sourceAddress,
            dataLen,
            msgType,
            msgData,
        ) = struct.unpack(unpackStr, msg)
        msgKey = "{0}{1}{2}".format(msgType, sourceAddress, destinationAddress)

        previousMsg = self.messagesForDiff.get(msgKey, bytearray())
        self.messagesForDiff[msgKey] = msg

        p = binascii.hexlify(previousMsg).upper()
        n = binascii.hexlify(msg).upper()
        self.logger.error(
            "Diff (%02X: %02X->%02X): %s",
            msgType,
            sourceAddress,
            destinationAddress,
            self.inline_diff(p, n),
        )

    def inline_diff(self, a, b):
        """
        http://stackoverflow.com/a/788780
        Unify operations between two compared strings seqm is a difflib.
        SequenceMatcher instance whose a & b are strings
        """
        matcher = difflib.SequenceMatcher(None, a, b)

        def process_tag(tag, i1, i2, j1, j2):
            CSI = "\033["
            BLUE = f"{CSI}1;34m"
            RED = f"{CSI}1;31m"
            GREEN = f"{CSI}0;32m"
            NORMAL = f"{CSI}0;0m"
            if tag == "replace":
                return (
                    GREEN
                    + "{"
                    + str(matcher.a[i1:i2])
                    + " -> "
                    + str(matcher.b[j1:j2])
                    + "}"
                    + NORMAL
                )
            elif tag == "delete":
                return RED + "{- " + str(matcher.a[i1:i2]) + "}" + NORMAL
            elif tag == "equal":
                return str(matcher.a[i1:i2])
            elif tag == "insert":
                return BLUE + "{+ " + str(matcher.b[j1:j2]) + "}" + NORMAL
            else:
                raise RuntimeError("unexpected opcode: %s", tag)

        return "".join(process_tag(*t) for t in matcher.get_opcodes())


if __name__ == "__main__":
    Framework().run(MyApp(), MyConfig())
