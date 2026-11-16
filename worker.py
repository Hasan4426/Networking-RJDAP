import socket
import sys
from datetime import datetime
import Protocol as p

SERVER = (sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1", 1111)
WORKER_ID = int(sys.argv[2]) if len(sys.argv) > 2 else 1
TIMEOUT = 2

logfile = open(f"client_{WORKER_ID}.log", "w")


def log(text):
    line = datetime.now().strftime("%H:%M:%S.%f")[:-3] + " | " + text
    print(line)
    logfile.write(line + "\n")
    logfile.flush()


def send_and_wait(sock, packet, expected_type):
    sock.sendto(packet, SERVER)
    while True:
        try:
            data, addr = sock.recvfrom(p.Max_Packet_Size)
        except socket.timeout:
            return None
        try:
            version, msg_type, task_id, worker_id, attempt, payload = p.decode_packet(data)
        except ValueError as e:
            log(f"DROP invalid packet from {addr}: {e}")   # bad packet = lost packet, keep waiting
            continue
        log(f"RECV {msg_type.name} task={task_id} worker={worker_id}")
        if msg_type == expected_type:
            return task_id, payload
        log(f"ignored {msg_type.name} (waiting for {expected_type.name})")


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)
    log(f"Worker {WORKER_ID} starting, server = {SERVER}")
    try:
        # 1. Join, then wait for a task
        log("SEND WORKER_JOIN")
        join = p.create_packet(p.MessageTypes.WORKER_JOIN, 0, WORKER_ID, 1)
        reply = send_and_wait(sock, join, p.MessageTypes.TASK_DISPATCH)
        if reply is None:
            log("No TASK_DISPATCH received (timeout). Giving up.")
            return
        task_id, payload = reply

        # 2. TASK_DISPATCH payload = two comma-separated ASCII integers, e.g. b"1,100"
        try:
            num1, num2 = (int(x) for x in payload.decode("ascii").split(","))
        except ValueError:
            log(f"Bad TASK_DISPATCH payload {payload!r}. Giving up.")
            return
        log(f"task {task_id}: {num1} + {num2}")

        # 3. Compute, send TASK_RESULT (payload = the sum as ASCII, e.g. b"101"), wait for the ACK
        result = num1 + num2
        log(f"SEND TASK_RESULT task={task_id} result={result}")
        packet = p.create_packet(p.MessageTypes.TASK_RESULT, task_id, WORKER_ID, 1,
                                 str(result).encode("ascii"))
        reply = send_and_wait(sock, packet, p.MessageTypes.ACK_RESULT)
        if reply is None:
            log("No ACK_RESULT received (timeout).")
        else:
            log(f"task {task_id} acknowledged. Done.")
    except OSError as e:
        log(f"Network error: {e}")
    finally:
        sock.close()


main()