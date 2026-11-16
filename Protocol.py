import struct
import zlib
from enum import IntEnum

Max_Packet_Size=512
VERSION=1
Header_Format="!BBIHBI"
Header_Size=struct.calcsize(Header_Format)
Max_Payload_Size=Max_Packet_Size-Header_Size


class MessageTypes(IntEnum):
    WORKER_JOIN=1
    HEARTBEAT=2
    TASK_DISPATCH=3
    TASK_RESULT=4
    ACK_RESULT=5
    STOP = 6

def calculate_checksum(payload: bytes)->int:
    return zlib.crc32(payload) & 0xFFFFFFFF

def create_packet(message_type,Task_id,worker_id,attempt_num,payload=b"" ):

    if not isinstance(payload,bytes):
        raise TypeError("Payload must be bytes")

    if len(payload)>Max_Payload_Size:
        raise ValueError("Payload exceeds 512 bytes")

    checksum=calculate_checksum(payload)

    header= struct.pack(Header_Format, VERSION,int(message_type),Task_id,worker_id,attempt_num,checksum)

    return header+ payload

def decode_packet(packet):
    if len(packet) < Header_Size:
        raise ValueError("Packet is too short")

    if len(packet) > Max_Packet_Size:
        raise ValueError("packet exceeds 512 bytes")

    header=packet[:Header_Size]
    payload=packet[Header_Size:]

    (version,message_type, Task_id, worker_id, attempt_num, received_checksum)=struct.unpack(Header_Format,header)

    if version != VERSION:
        raise ValueError(f"Unsupported protocol version: {version}")

    message_type=MessageTypes(message_type)

    expected_checksum=calculate_checksum(payload)

    if received_checksum != expected_checksum:
        raise ValueError("checksum mismatch")

    return(version, message_type, Task_id, worker_id, attempt_num, payload)