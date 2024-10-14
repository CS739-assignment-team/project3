import sqlite3
import socket
import threading
import string
import queue
import argparse
import random
import time
import json

HOST = socket.gethostname()
DATABASE = 'kvstore.db'
POOL_SIZE = 32
PEER_LIST = None

class ConnectionPool:
    def __init__(self, database, pool_size):
        self.database = database
        self.pool_size = pool_size
        self.connections = queue.Queue(maxsize=pool_size)
        self.size = 0
        self.lock = threading.Lock()

    def get_connection(self):
        if self.connections.empty() and self.size < self.pool_size:
            with self.lock:
                if self.size < self.pool_size:
                    conn = sqlite3.connect(self.database, check_same_thread=False)
                    self.size += 1
                    return conn
        try:
            return self.connections.get(block=True, timeout=5)
        except queue.Empty:
            raise Exception("Timeout waiting for a database connection")

    def return_connection(self, connection):
        self.connections.put(connection)

    def close_all(self):
        while not self.connections.empty():
            conn = self.connections.get()
            conn.close()
        self.size = 0

db_pool = None
db_lock = threading.Lock()

def init_db():
    global db_pool
    db_pool = ConnectionPool(DATABASE, POOL_SIZE)
    conn = db_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kvstore (
            key TEXT PRIMARY KEY,
            value TEXT,
            version INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    db_pool.return_connection(conn)

def is_valid_key(key):
    return len(key) <= 128 and all(c in string.printable for c in key)

def is_valid_value(value):
    return len(value) <= 2048 and all(c in string.printable for c in value)

def put_value(key, value):
    with db_lock:
        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT version FROM kvstore WHERE key = ?", (key,))
            row = cursor.fetchone()
            version = row[0] + 1 if row else 1
            cursor.execute("INSERT OR REPLACE INTO kvstore (key, value, version) VALUES (?, ?, ?)", (key, value, version))
            conn.commit()
        finally:
            db_pool.return_connection(conn)

def get_value(key):
    conn = db_pool.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM kvstore WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        db_pool.return_connection(conn)

def get_versioned_data():
    conn = db_pool.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value, version FROM kvstore")
        data = cursor.fetchall()
        return {row[0]: (row[1], row[2]) for row in data}
    finally:
        db_pool.return_connection(conn)

def merge_data(peer_data):
    with db_lock:
        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            for key, (value, version) in peer_data.items():
                cursor.execute("SELECT version FROM kvstore WHERE key = ?", (key,))
                local_row = cursor.fetchone()
                local_version = local_row[0] if local_row else 0
                if version > local_version:
                    cursor.execute("INSERT OR REPLACE INTO kvstore (key, value, version) VALUES (?, ?, ?)", (key, value, version))
            conn.commit()
        finally:
            db_pool.return_connection(conn)

def gossip(peers, PORT):
    host_name = socket.gethostname()
    server_address = (host_name, PORT)

    for peer in peers:
        if peer == server_address:
            continue
        try:
            with socket.create_connection(peer, timeout=.05) as sock:
                local_data = get_versioned_data()
                data = json.dumps({"gossip": True, "store": local_data})
                sock.sendall(data.encode("utf-8"))

                response = sock.recv(4096).decode("utf-8")
                # print(f"Raw response from {peer}: '{response}'")

                if response:
                    peer_data = json.loads(response).get("store")
                    if peer_data:
                        merge_data(peer_data)
                else:
                    print(f"No response received from {peer}")

        except (socket.timeout, socket.error, json.JSONDecodeError) as e:
            print(f"Failed to gossip with {peer}: {e}")

def gossip_periodically(peers, PORT):
    while True:
        time.sleep(.05)
        gossip(peers, PORT)

def handle_client(conn, addr):
    print(f"Connected by {addr}")
    try:
        while True:
            data = conn.recv(1024).decode("utf-8")
            # if not data:
            #     break
            try:
                request_json = json.loads(data)
                if request_json.get("gossip"):
                    local_data = get_versioned_data()
                    response = json.dumps({"store": local_data})
                    conn.sendall(response.encode("utf-8"))
                    return
            except json.JSONDecodeError:
                pass

            command = data.split()

            if command[0] == "GET":
                key = command[1]
                if not is_valid_key(key):
                    conn.sendall(b"INVALID_KEY")
                    continue

                value = get_value(key)
                if value:
                    conn.sendall(f"VALUE {value}".encode("utf-8"))
                else:
                    conn.sendall(b"KEY_NOT_FOUND")

            elif command[0] == "PUT":
                key = command[1]
                value = command[2]

                if not is_valid_key(key):
                    conn.sendall(b"INVALID_KEY")
                    continue
                if not is_valid_value(value):
                    conn.sendall(b"INVALID_VALUE")
                    continue

                old_value = get_value(key)
                put_value(key, value)

                if old_value:
                    conn.sendall(f"UPDATED {old_value}".encode("utf-8"))
                else:
                    conn.sendall(b"INSERTED")

            elif command[0] == "SHUTDOWN":
                conn.sendall(b"Goodbye! Closing client connection.")
                print(f"Client at {addr} is shutting down.")
                break

            else:
                conn.sendall(b"INVALID_COMMAND")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def start_server():
    parser = argparse.ArgumentParser(description="Key-Value Store Server")
    parser.add_argument("--port", help="Server port", required=True)
    parser.add_argument("--peers", help="Comma-separated list of peer host:port")

    args = parser.parse_args()
    PORT = int(args.port)

    peers = [(peer.split(":")[0], int(peer.split(":")[1])) for peer in args.peers.split(",")] if args.peers else []

    init_db()
    threading.Thread(target=gossip_periodically, args=(peers,PORT), daemon=True).start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server started on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            client_handler = threading.Thread(target=handle_client, args=(conn, addr))
            client_handler.start()

if __name__ == "__main__":
    start_server()