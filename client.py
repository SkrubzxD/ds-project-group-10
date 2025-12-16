from mpi4py import MPI
import os
import sys
import select
import time
import json

# Configuration
TAG_REGISTER = 1
TAG_DIR_UPDATE = 2
TAG_CHAT = 3

def get_pipe_names(rank):
    return f"/tmp/chat_fifo_in_{rank}", f"/tmp/chat_fifo_out_{rank}"

def ensure_fifo(path):
    if not os.path.exists(path):
        os.mkfifo(path)

def setup_fifos(rank):
    p_in, p_out = get_pipe_names(rank)
    ensure_fifo(p_in)
    ensure_fifo(p_out)
    return p_in, p_out

# Rank 0
def handle_server(comm, size):
    print(f"[Server-0] Registrar started. Waiting for peers (Ranks 1-{size-1})....")
    user_directory = {} # {username: rank}

    while True:
        # Non-blocking check for incoming MPI messages
        status = MPI.Status()
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            data = comm.recv(source=status.Get_source(), tag=status.Get_tag(), status=status)
            source_rank = status.Get_source()
            tag = status.Get_tag()

            if tag == TAG_REGISTER:
                username = data
                print(f"[Server-0] Registering '{username}' at Rank {source_rank}")
                user_directory[username] = source_rank

                # Broadcast updated directory to all peers
                for r in range(1, size):
                    comm.isend(user_directory, dest=r, tag=TAG_DIR_UPDATE)
                print(f"[Server-0] Broadcasted updated user directory", flush=True)

        time.sleep(0.01)

# Rank 1+
def handle_client(comm, rank):
    # 1. Setup pipes
    pipe_in_path, pipe_out_path = setup_fifos(rank)
    print(f"[Peer-{rank}] Done setup_fifos.")

    # 2. Open pipes
    pipe_in_fd = os.open(pipe_in_path, os.O_RDONLY | os.O_NONBLOCK)

    pipe_out_fd = None
    print(f"[Peer-{rank}] Waiting for client connection...")
    while pipe_out_fd is None:
        try:
            pipe_out_fd = os.open(pipe_out_path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            time.sleep(0.5)

    print(f"[Peer-{rank}] Client connected!")

    # Send initial prompt to client
    os.write(pipe_out_fd, f"Connected to MPI Rank {rank}.\n".encode())
    os.write(pipe_out_fd, b"Enter Username to register:\n")

    user_directory = {}
    username = None

    # 3. Main loop
    while True:
        readable, _, _ = select.select([pipe_in_fd], [], [], 0.01)

        if pipe_in_fd in readable:
            try:
                data = os.read(pipe_in_fd, 1024)
                if not data:
                    print(f"[Peer-{rank}] Client disconnected.")
                    break

                msg_str = data.decode().strip()
                if not msg_str: continue

                # Registration
                if username is None:
                    username = msg_str
                    comm.send(username, dest=0, tag=TAG_REGISTER)
                    os.write(pipe_out_fd, f"[System] Registered as '{username}'.\n".encode())
                    os.write(pipe_out_fd, b"[System] Usage: /<username> <msg>; /all <msg>; /ls\n")
                # List user and rank
                elif msg_str == "/ls":
                    os.write(pipe_out_fd, f"User online: {user_directory}\n".encode())
                # Send message
                elif msg_str.startswith("/"):
                    parts = msg_str[1:].split(" ", 1)
                    dest = parts[0]
                    content = parts[1] if len(parts) > 1 else ""

                    if dest == "all":
                        for user, r_id in user_directory.items():
                            if r_id != rank:
                                comm.isend((username, f"[Broadcast] {content}"), dest=r_id, tag=TAG_CHAT)
                        os.write(pipe_out_fd, f"[Broadcast]: {content}\n".encode())
                    elif dest in user_directory:
                        target_rank = user_directory[dest]
                        comm.isend((username, content), dest=target_rank, tag=TAG_CHAT)
                        os.write(pipe_out_fd, f"[To {dest}]: {content}\n".encode())
                    else:
                        os.write(pipe_out_fd, f"[Error] User '{dest}' not found.\n".encode())
                else:
                    os.write(pipe_out_fd, b"[Error] Invalid format. Use /<user> <msg>\n")


            except OSError:
                break

        # Handle incoming messages 
        status = MPI.Status()
        while comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            data = comm.recv(source=status.Get_source(), tag=status.Get_tag(), status=status)
            tag = status.Get_tag()

            if tag == TAG_DIR_UPDATE:
                user_directory = data
            elif tag == TAG_CHAT:
                sender, msg = data
                os.write(pipe_out_fd, f"[{sender}]: {msg}\n".encode())

    os.close(pipe_in_fd)
    os.close(pipe_out_fd)

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        print("Error: Need at least 2 ranks.")
        exit(1)

    if rank == 0:
        handle_server(comm, size)
    else:
        handle_client(comm, rank)
