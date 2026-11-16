import socket
import time
from datetime import datetime

from Protocol import MessageTypes, create_packet, decode_packet


SERVER_ADDRESS = ("127.0.0.1", 1111)
JOIN_TIMEOUT = 60
RESULT_TIMEOUT = 10
TASK_ID = 1
ATTEMPT_NUM = 1
NUMBER_1 = 1
NUMBER_2 = 100


def log(message):
    timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
    print(f"[{timestamp}] {message}", flush=True)


def receive_message(server, deadline):
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("No expected message arrived before the deadline")
        server.settimeout(remaining)

        packet, address = server.recvfrom(65535)
        log(f"RECEIVE from={address} bytes={len(packet)}")
        try:
            message = decode_packet(packet)
        except ValueError as error:
            log(f"DROP invalid packet: {error}")
            continue

        version, message_type, task_id, worker_id, attempt_num, payload = message
        log(f"DECODE type={message_type.name} task={task_id} "
            f"worker={worker_id} attempt={attempt_num} payload={payload!r}")
        return message, address


def send_message(server, address, message_type, task_id, worker_id,
                 attempt_num, payload=b""):
    packet = create_packet(message_type, task_id, worker_id, attempt_num, payload)
    server.sendto(packet, address)
    log(f"SEND to={address} type={message_type.name} task={task_id} "
        f"worker={worker_id} attempt={attempt_num} bytes={len(packet)} "
        f"payload={payload!r}")


def main():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
            server.bind(SERVER_ADDRESS)
            log(f"Server listening on {SERVER_ADDRESS}; waiting for WORKER_JOIN")

            deadline = time.monotonic() + JOIN_TIMEOUT
            while True:
                message, address = receive_message(server, deadline)
                version, message_type, task_id, worker_id, attempt_num, payload = message
                if (message_type == MessageTypes.WORKER_JOIN and task_id == 0
                        and worker_id > 0 and payload == b""):
                    worker_address = address
                    registered_worker_id = worker_id
                    break
                log("DROP: expected WORKER_JOIN with task=0, worker>0, empty payload")

            log(f"Registered worker {registered_worker_id} at {worker_address}")

            task_payload = f"{NUMBER_1},{NUMBER_2}".encode("ascii")
            send_message(server, worker_address, MessageTypes.TASK_DISPATCH,
                         TASK_ID, registered_worker_id, ATTEMPT_NUM, task_payload)

            deadline = time.monotonic() + RESULT_TIMEOUT
            while True:
                message, address = receive_message(server, deadline)
                version, message_type, task_id, worker_id, attempt_num, payload = message

                if address != worker_address or worker_id != registered_worker_id:
                    log("DROP: message is not from the registered worker")
                    continue

                if (message_type != MessageTypes.TASK_RESULT or task_id != TASK_ID
                        or attempt_num != ATTEMPT_NUM):
                    log("DROP: expected TASK_RESULT for the current task and attempt")
                    continue

                try:
                    result = int(payload.decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    log("DROP: result payload is not an ASCII integer")
                    continue

                if result != NUMBER_1 + NUMBER_2:
                    log(f"DROP: incorrect result {result}; expected {NUMBER_1 + NUMBER_2}")
                    continue
                break

            log(f"Task {TASK_ID} completed: {NUMBER_1} + {NUMBER_2} = {result}")
            send_message(server, worker_address, MessageTypes.ACK_RESULT,
                         TASK_ID, registered_worker_id, ATTEMPT_NUM)
            log("Exchange complete; server closing")
        return 0

    except TimeoutError:
        log("ERROR: timed out waiting for the worker join or a valid task result")
        return 1
    except OSError as error:
        log(f"ERROR: socket operation failed: {error}")
        return 1
    except KeyboardInterrupt:
        log("Server stopped by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
