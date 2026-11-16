import socket
import sys
import time
from datetime import datetime
import Protocol as p

SERVER = (sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1", 1111)
WORKER_ID = int(sys.argv[2]) if len(sys.argv) > 2 else 1
JOIN_TIMEOUT = 65
IDLE_TIMEOUT = 60
ACK_TIMEOUT = 10

logfile = None


def log(text):
    line = datetime.now().strftime("%H:%M:%S.%f")[:-3] + " | " + text
    print(line)
    if logfile and not logfile.closed:
        logfile.write(line + "\n")
        logfile.flush()


def send_and_wait(sock, packet, expected_type, expected_task_id=None, expected_attempt=None):
    deadline = time.monotonic() + sock.gettimeout()
    sock.sendto(packet, SERVER)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(65535)
        except socket.timeout:
            return None
        try:
            version, msg_type, task_id, worker_id, attempt, payload = p.decode_packet(data)
        except ValueError as e:
            log(f"DROP invalid packet from {addr}: {e}")
            continue
        

        if not validate_packet(
            addr, worker_id, task_id, attempt,
            expected_task_id, expected_attempt
        ):
            continue
        
        log(f"RECV {msg_type.name} task={task_id} worker={worker_id}")
        if msg_type == expected_type:
            return task_id, attempt, payload
        log(f"ignored {msg_type.name} (waiting for {expected_type.name})")


def receive_next_task(sock):
    deadline = time.monotonic() + IDLE_TIMEOUT
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log("Idle timeout.")
            return "timeout", None
        sock.settimeout(remaining)
        try:
            data, addr = sock.recvfrom(65535)
        except socket.timeout:
            log("Idle timeout.")
            return "timeout", None
        try:
            version, msg_type, task_id, worker_id, attempt, payload = (
                p.decode_packet(data)
            )
        except ValueError as e:
            log(f"DROP invalid packet from {addr}: {e}")
            continue


        if not validate_packet(addr, worker_id, task_id, attempt):
            continue
        log(f"RECV {msg_type.name} task={task_id} worker={worker_id}")

        if msg_type == p.MessageTypes.SHUTDOWN:
            log("Server requested shutdown.")
            return "shutdown", None

        if msg_type == p.MessageTypes.TASK_DISPATCH:
            return "task", (task_id, attempt, payload)
        log(f"ignored {msg_type.name}")


def validate_packet(addr, worker_id, task_id, attempt,
                    expected_task_id=None, expected_attempt=None):


    expected_ip = socket.gethostbyname(SERVER[0])
    if addr[0] != expected_ip or addr[1] != SERVER[1]:
        log(f"DROP packet from unexpected sender: {addr}")
        return False


    if worker_id != WORKER_ID:
        log(f"DROP packet for wrong worker: {worker_id}")
        return False


    if expected_task_id is not None and task_id != expected_task_id:
        log(f"DROP packet for wrong task: {task_id}")
        return False


    if expected_attempt is not None and attempt != expected_attempt:
        log(f"DROP packet for wrong attempt: {attempt}")
        return False

    return True


def main():
    global logfile
    logfile = open(f"client_{WORKER_ID}.log", "w")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(JOIN_TIMEOUT)

    log(f"Worker {WORKER_ID} starting, server = {SERVER}")

    try:

        log("SEND WORKER_JOIN")
        join = p.create_packet(
            p.MessageTypes.WORKER_JOIN, 0, WORKER_ID, 1
        )

        reply = send_and_wait(
            sock,
            join,
            p.MessageTypes.TASK_DISPATCH
        )

        if reply is None:
            log("ERROR: JOIN timeout.")
            return 1

        task_id, attempt, payload = reply


        while True:

            try:
                num1, num2 = (
                    int(x) for x in payload.decode("ascii").split(",")
                )
            except ValueError:
                log(f"Bad TASK_DISPATCH payload: {payload!r}")
                return 1

            log(f"task {task_id}: {num1} + {num2}")
            result = num1 + num2

            log(f"SEND TASK_RESULT task={task_id} attempt={attempt} result={result}")

            packet = p.create_packet(
                p.MessageTypes.TASK_RESULT,
                task_id,
                WORKER_ID,
                attempt,
                str(result).encode("ascii")
            )
            sock.settimeout(ACK_TIMEOUT)
            reply = send_and_wait(
                sock,
                packet,
                p.MessageTypes.ACK_RESULT,
                expected_task_id=task_id,
                expected_attempt=attempt
            )

            if reply is None:
                log(f"ERROR: ACK timeout task={task_id} attempt={attempt}")
                return 1

            log(f"task {task_id} acknowledged.")


            status, reply = receive_next_task(sock)

            if status == "shutdown":
                log("Worker shutting down.")
                return 0

            elif status == "timeout":
                log("No response from server. Stopping worker.")
                return 1

            task_id, attempt, payload = reply

    except OSError as e:
        log(f"Network error: {e}")
        return 1

    finally:
        sock.close()
        if logfile and not logfile.closed:
            logfile.close()

if __name__ == "__main__":
    raise SystemExit(main())
