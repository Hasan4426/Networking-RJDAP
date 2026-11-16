import socket
import time
from datetime import datetime

from Protocol import MessageTypes, create_packet, decode_packet


SERVER_ADDRESS = ("127.0.0.1", 1111)
JOIN_TIMEOUT = 60
RESULT_TIMEOUT = 10
WORKER_COUNT = 4
TASK_COUNT = 1000
ATTEMPT_NUM = 1


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

    if not hasattr(MessageTypes, "SHUTDOWN"):
        log("ERROR: add SHUTDOWN = 6 to MessageTypes in Protocol.py before running")
        return 1

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
            server.bind(SERVER_ADDRESS)
            log(f"Server listening on {SERVER_ADDRESS}; waiting for {WORKER_COUNT} workers")

            workers = {}
            deadline = time.monotonic() + JOIN_TIMEOUT
            while len(workers) < WORKER_COUNT:
                message, address = receive_message(server, deadline)
                version, message_type, task_id, worker_id, attempt_num, payload = message
                if (message_type != MessageTypes.WORKER_JOIN or task_id != 0
                        or worker_id <= 0 or payload != b""):
                    log("DROP: expected a valid WORKER_JOIN")
                    continue
                if worker_id in workers:
                    log(f"DROP: worker ID {worker_id} is already registered")
                    continue
                if address in workers.values():
                    log(f"DROP: address {address} is already registered")
                    continue
                workers[worker_id] = address
                log(f"Registered worker {worker_id} at {address} "
                    f"({len(workers)}/{WORKER_COUNT})")

            worker_ids = sorted(workers)
            completed_tasks = set()
            tasks_per_worker = {worker_id: 0 for worker_id in worker_ids}
            started = time.monotonic()


            for task_id in range(1, TASK_COUNT + 1):
                worker_id = worker_ids[(task_id - 1) % WORKER_COUNT]
                worker_address = workers[worker_id]
                number_1, number_2 = task_id, 100
                expected_result = number_1 + number_2
                task_payload = f"{number_1},{number_2}".encode("ascii")
                send_message(server, worker_address, MessageTypes.TASK_DISPATCH,
                             task_id, worker_id, ATTEMPT_NUM, task_payload)

                deadline = time.monotonic() + RESULT_TIMEOUT
                while True:
                    message, address = receive_message(server, deadline)
                    (version, message_type, received_task_id, received_worker_id,
                     received_attempt, payload) = message
                    if address != worker_address or received_worker_id != worker_id:
                        log("DROP: result is not from the assigned worker")
                        continue
                    if message_type != MessageTypes.TASK_RESULT:
                        log("DROP: expected TASK_RESULT")
                        continue
                    if received_task_id in completed_tasks:
                        log(f"DROP: duplicate result for task {received_task_id}")
                        continue
                    if received_task_id != task_id or received_attempt != ATTEMPT_NUM:
                        log("DROP: wrong task ID or attempt number")
                        continue
                    try:
                        result = int(payload.decode("ascii"))
                    except (UnicodeDecodeError, ValueError):
                        log("DROP: result must be an ASCII integer")
                        continue
                    if result != expected_result:
                        log(f"DROP: incorrect result {result}; expected {expected_result}")
                        continue

                    completed_tasks.add(task_id)
                    tasks_per_worker[worker_id] += 1
                    log(f"Task {task_id} completed: {number_1} + {number_2} = {result}")
                    send_message(server, worker_address, MessageTypes.ACK_RESULT,
                                 task_id, worker_id, ATTEMPT_NUM)
                    break

            elapsed = time.monotonic() - started
            for worker_id, worker_address in workers.items():
                send_message(server, worker_address, MessageTypes.SHUTDOWN,
                             0, worker_id, 0)
            log(f"Completed {len(completed_tasks)}/{TASK_COUNT} tasks in {elapsed:.3f} seconds")
            for worker_id, count in tasks_per_worker.items():
                log(f"Worker {worker_id} completed {count} tasks")
            log("Exchange complete; server closing")
        return 0

    except TimeoutError:
        log("ERROR: timed out waiting for workers or a valid result")
        return 1
    except OSError as error:
        log(f"ERROR: socket operation failed: {error}")
        return 1
    except KeyboardInterrupt:
        log("Server stopped by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
