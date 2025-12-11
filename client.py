import os
import sys
import select
import time

def get_pipe_names(rank):
    return f"/tmp/chat_fifo_in_{rank}", f"/tmp/chat_fifo_out_{rank}"

def ensure_fifo(path):
    if not os.path.exists(path):
        os.mkfifo(path)

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <rank_id>")
        return

    rank_id = sys.argv[1]

    READ_FIFO, WRITE_FIFO = get_pipe_names(rank_id)

    # 1. Open Pipes

    print(f"[INFO] Connecting to MPI Node {rank_id}...")

    write_fd = None
    read_fd = None

    while True:
        try:
            write_fd = os.open(READ_FIFO, os.O_WRONLY | os.O_NONBLOCK)
            break
        except OSError:
            time.sleep(0.5)

    while True:
        try:
            read_fd = os.open(WRITE_FIFO, os.O_RDONLY | os.O_NONBLOCK)
            break
        except OSError:
            time.sleep(0.5)

    print("[INFO] Connected! Type your username to register.")

    # 2. Main loop
    while True:
        readable, _, _ = select.select([read_fd, sys.stdin], [], [])

        # Incoming message 
        if read_fd in readable:
            data = os.read(read_fd, 4096)
            if data:
                print(f"{data.decode().strip()}")
                sys.stdout.flush()
            else:
                print("[INFO] Server closed the connection.")
                break

        # Outgoing message
        if sys.stdin in readable:
            msg = sys.stdin.readline()
            if msg:
                os.write(write_fd, msg.encode())
            else:
                break

    os.close(read_fd)
    os.close(write_fd)

if __name__ == "__main__":
    main()
