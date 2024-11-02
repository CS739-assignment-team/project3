import sqlite3
import socket
import threading
import string
import queue
import argparse
import random
import time
import json
import mmh3, pickle, os
from utils import *
import traceback
import psutil
import concurrent.futures
import time

#todo
# HOST = socket.gethostname()
# HOST = resolve_host("localhost")
HOST = '127.0.0.1'
DATABASE = 'kvstore.db'
POOL_SIZE = 32
PEER_LIST = None
MAX_RETRIES = 1
PORT = 4000
PID = os.getpid()

# disable all prints
# class NullWriter:
#     def write(self, arg):
#         pass
#     def flush(self):
#         pass  
# sys.stdout = NullWriter()

global_state = None
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
            version TEXT,
            key_hash TEXT
        )
    ''')
    conn.commit()
    db_pool.return_connection(conn)

def is_valid_key(key):
    return len(key) <= 128 and all(c in string.printable for c in key)

def is_valid_value(value):
    return len(value) <= 2048 and all(c in string.printable for c in value)

def share_data(key, value, version, host, port):

    # print(f'current host {HOST} and  port {PORT}')
    # print(f'replicating host {host} port {port}')
    if host == -1:
        return -1
    # if host == HOST and port == PORT:
    #     return 0
    try:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((host, port))
        print(f"Connected to {host}:{port}")
        
        payload = {
            'key': key,
            'value': value,
            'version': version
        }
        message = b"PROPAGATE"+b"|--|"+pickle.dumps(payload)
        conn.sendall(message)
        response = conn.recv(1024).decode('utf-8')
        # print(f'response from remote {response}')
        conn.sendall(b'SHUTDOWN')
        # print('sent shotdown')
    except Exception as e:
        print(f"Connection error to {host}:{port}: {e}")
        return -1
    finally:
        conn.close()
    return 0
        
def propagate_key(key, value, version, backup_nodes):
    threads = []
    for host, port in backup_nodes:
        thread = threading.Thread(target=share_data, args=(key, value, version, host, port), daemon=True)
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

    return 0

def replicate_state(data, retry_attempt):
    #Mapping local state file name to port to make it unique 
    #since all servers share fs we have to name files uniquely 
    temp_file_name = f'sw_state_{PORT}.pickle'
    state_file_name = f'state_{PORT}.pickle'
    global global_state
    while retry_attempt < MAX_RETRIES:
        try: 
            global_state = pickle.loads(data)  # Deserializes from file

            with open(temp_file_name, 'wb') as file:  # Write binary
                pickle.dump(global_state, file)  # Serializes and saves to file

            if os.path.exists(state_file_name):
                os.remove(state_file_name)

            os.rename(temp_file_name, state_file_name)
            return 0
        except Exception as e:
            retry_attempt += 1
            print(e)
            traceback.print_exc()
            print(f'couldn\'t replicate the global state attempt {retry_attempt} max retries {MAX_RETRIES}')
    if retry_attempt == MAX_RETRIES:
        return -1
    return 0

def die(server_name, clean, client_connection):
    if clean == 1:
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leader_host, leader_port = global_state['leader_address'].split(':')
        conn.connect((leader_host, int(leader_port)))
        print('leader connected to kill ndoe')
        conn.sendall(f'DIE {server_name}'.encode('utf-8'))
        response = conn.recv(1024).decode('utf-8')
        print('kill response ', response)
        conn.close()
        state_file_name = f'state_{PORT}.pickle'
        if os.path.exists(state_file_name):
            os.remove(state_file_name)
        
        client_connection.sendall(b'Done with operation! shutting down')

        print(f'killing the process with PID: {PID}')
        process = psutil.Process(PID)
        process.terminate()
    else:
        print(f'killing the process with PID: {PID}')
        process = psutil.Process(PID)
        process.terminate()

def put_value(key, value, server_index):
    key_hash = hash(key)
    replica_nodes = find_nodes_for_key(global_state['tokens'], global_state['token_map'], key)
    host, port = extract_server_url(replica_nodes[0])

    #if server index exists it means we are retrying, so even if its not primary accept it
    if (host != HOST or port != PORT) and not server_index:
        return b"RETRY_PRIMARY"+ b'|--|'+ pickle.dumps(replica_nodes), 'reply'

    old_value = None
    new_version = None

    with db_lock:
        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT version,value FROM kvstore WHERE key = ?", (key,))
            row = cursor.fetchone()
            new_version = int(row[0].rpartition('-')[0]) + 1 if row else 1
            new_version = str(new_version) + '-' + str(time.time())
            cursor.execute("INSERT OR REPLACE INTO kvstore (key, value, version, key_hash) VALUES (?, ?, ?, ?)", (key, value, new_version, str(key_hash)))
            conn.commit()

            if row:
                old_value = row[1]

        except Exception:
            pass
        finally:
            db_pool.return_connection(conn)
        
    servers = deduplicate_nodes(replica_nodes)
    if (HOST, PORT) in servers:
        servers.remove((HOST, PORT))

    propagate_key(key,value,new_version, servers)

    if old_value:
        return old_value, 'old_value'

    return None, 'INSERTED'

'''
check the hash of the key, identify who owns the key and redirect
'''
def get_value(key, server_index):
    replica_nodes = find_nodes_for_key(global_state['tokens'], global_state['token_map'], key)
    primary_host, primary_port = extract_server_url(replica_nodes[0])

    #if current node is not primary send primary details
    #client updates the cache 
    if not server_index and (primary_host != HOST or primary_port != PORT):
        return b"RETRY_PRIMARY"+ b'|--|'+ pickle.dumps(replica_nodes), 'reply'
    
    #either primary or supposed primary is handling
    #call replicas to read their values
    def get_value_from_replica(host, port):

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
                conn.connect((host, port))
                # Send request for the value
                request_payload = b"GET_LOCAL_VALUE"  + f'{key}'.encode('utf-8')
                conn.sendall(request_payload)
                # Receive the response
                response = conn.recv(1024).decode('utf-8')
                messages = response.split()
                if messages[0]== 'KEY_NOT_FOUND':
                    return (None, None)
                return (messages[1], messages[2])  # Return the replica and decoded response
        except Exception as e:
            return (None, None)
    
    conn = db_pool.get_connection()
    local_result = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value, version FROM kvstore WHERE key = ?", (key,))
        row = cursor.fetchone()
        local_result = row
    finally:
        db_pool.return_connection(conn)
    
    servers = deduplicate_nodes(replica_nodes)

    if (HOST, PORT) in servers:
        servers.remove((HOST, PORT))

    curr_value = None if not local_result else local_result[0]
    curr_version = None if not local_result else local_result[1]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_replica = {executor.submit(get_value_from_replica, replica[0], replica[1]): replica for replica in replica_nodes}

        
        for future in concurrent.futures.as_completed(future_to_replica):
            replica = future_to_replica[future]
            try:
                value, version = future.result()
                
                if not value or version:
                    continue
                
                if not compare_versions(curr_version, version):
                    curr_version = version
                    curr_value = value

            except Exception as e:
                continue
    if curr_value:
        return curr_value, 'value'
    return None, 'value'

def compare_versions(version1, version2):
    if not version1:
        return False
    
    v1_version, v1_timestamp = version1.split('-')
    v2_version, v2_timestamp = version2.split('-')
    if v1_timestamp > v2_timestamp:
        return True
    elif v1_timestamp == v2_timestamp:
        return v1_version >= v2_version
    return False

def deduplicate_nodes(replicas):
    servers = [(extract_server_url(replica)) for replica in replicas]

    return list(set(servers))

def replicate_key(key, value, version):
    key_hash = hash(key)

    with db_lock:
        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            # cursor.execute("SELECT version,value FROM kvstore WHERE key = ?", (key,))
            # row = cursor.fetchone()
            # if row and version <= row[0]:
            #     print(f'Warning: replicating to older version for key, {key}')
            cursor.execute("INSERT OR REPLACE INTO kvstore (key, value, version, key_hash) VALUES (?, ?, ?, ?)", (key, value, version, str(key_hash)))
            conn.commit()

        finally:
            db_pool.return_connection(conn)
    return 0

'''
def get_local_value(): 
For the sake of consensus no validation returns the bersion and value it has
else None, None
'''
def get_local_value(key):
    with db_lock:
        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT value, version FROM kvstore WHERE key = ?", (key,))
            row = cursor.fetchone()

            if row:
                return (row[0], row[1])
        finally:
            db_pool.return_connection(conn)
    return (None, None)


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
    #todo
    #host_name = socket.gethostname()
    host_name = "localhost"
    server_address = (host_name, PORT)
    for peer in peers:
        if peer == server_address:
            continue
        try:
            with socket.create_connection(peer, timeout=0.3) as sock:
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
            pass
            #todo too much info
            #print(f"Failed to gossip with {peer}: {e}")

def gossip_periodically(peers, PORT):
    while True:
        time.sleep(0.2)
        gossip(peers, PORT)

def handle_client(conn, addr):
    #print(f"Connected by {addr}")
    try:
        while True:
            data = conn.recv(20480)
            messages = data.split(b'|--|', 1)
            if not data:
                print('server sent empty message closig connection', addr)
                break
            if len(messages) > 1:
                code = messages[0].decode('utf-8')
                if code == "REPLICATE":
                    response = replicate_state(messages[1], 0)
                    conn.sendall(f"{response}".encode("utf-8"))
                    continue
                if code == "PROPAGATE":
                    payload = pickle.loads(messages[1])
                    response = replicate_key(payload['key'], payload['value'], payload['version'])

                    conn.sendall(f"{response}".encode("utf-8"))
                    break
                
            data = data.decode("utf-8")

            try:
                request_json = json.loads(data)
                if request_json.get("gossip"):
                    local_data = get_versioned_data()
                    response = json.dumps({"store": local_data})
                    conn.sendall(response.encode("utf-8"))
                    return
            except json.JSONDecodeError:
                pass
            if not data:
                continue
            # print(data)
            command = data.split()

            if command[0] == "GET":
                key = command[1]
                server_index = command[2] if len(command) > 2 else None

                if not is_valid_key(key):
                    conn.sendall(b"INVALID_KEY")
                    continue

                response, response_type = get_value(key, server_index)
                if response_type == 'reply':
                    conn.sendall(response)
                else:
                    if response:
                        conn.sendall(f"VALUE {response}".encode("utf-8"))
                    else:
                        conn.sendall(b"KEY_NOT_FOUND")

            elif command[0] == "GET_LOCAL_VALUE":
                value, version = get_local_value(command[1])
                if value:
                    conn.sendall(f'VALUE_VERSION {value} {version}'.encode('utf-8'))
                else:
                    conn.sendall(b"KEY_NOT_FOUND")
                break
            elif command[0] == "PUT":
                key = command[1]
                value = command[2]
                server_index = command[3] if len(command) > 3 else None

                if not is_valid_key(key):
                    conn.sendall(b"INVALID_KEY")
                    continue
                if not is_valid_value(value):
                    conn.sendall(b"INVALID_VALUE")
                    continue

                response, response_type = put_value(key, value, server_index)

                if response_type == "old_value":
                    conn.sendall(f"UPDATED {response}".encode("utf-8"))
                elif response_type == "INSERTED":
                    conn.sendall(b"INSERTED")
                else:
                    conn.sendall(response)

            elif command[0] == "SHUTDOWN":
                conn.sendall(b"Goodbye! Closing client connection.")
                print(f"Client at {addr} is shutting down.")
                break
            
            elif command[0] == "DIE":
                print(command[1], command[2])
                die(command[1], int(command[2]), conn)
            elif command[0] == "GET_LEADER":
                find_leader(conn)
                break
            else:
                #print(data)
                conn.sendall(b"INVALID_COMMAND")

    except Exception as e:
        # pass
        traceback.print_exc()
        print(f"Error: {e}")
    finally:
        conn.close()

def find_leader(conn):
    global global_state
    leader_address = global_state.get('leader_address', None)
    if not leader_address:
        conn.sendall('NOT_FOUND')
    else:
        conn.sendall(f'{leader_address}'.encode('utf-8'))

    return 
def start_server():
    parser = argparse.ArgumentParser(description="Key-Value Store Server")
    parser.add_argument("--port", help="Server port", required=True)
    parser.add_argument("--servfile", help="Filename of a text file with peer host:port list", required=True)
    parser.add_argument("--leaderaddress", help="Address of leader if joining cluster later", required=False)
    args = parser.parse_args()
    global PORT
    PORT = int(args.port)

    global DATABASE
    DATABASE = f'kvstore.db-{PORT}'

    #if local state exists load it 
    #else wait for leader to sened it
    global global_state
    local_state_filename = f'state_{PORT}.pickle'
    if os.path.exists(local_state_filename):
        with open(local_state_filename, 'rb') as file:
            bytes = file.read()
            if bytes:
                global_state = pickle.loads(bytes)


    # peers = []

    # with open(args.servfile, 'r') as f:
    #     for line in f:
    #         host, port = line.strip().split(":")
    #         peers.append((host, int(port)))
            
    init_db()
    # threading.Thread(target=gossip_periodically, args=(peers,PORT), daemon=True).start()

    if args.leaderaddress:
        leader_host, leader_port = args.leaderaddress.split(':')
        send_join_cluster_request((leader_host, leader_port))

    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        s.settimeout(100)
        print(f"Server started on {HOST}:{PORT}")

        while True:
            try:
                conn, addr = s.accept()
                client_handler = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
                client_handler.start()
            except KeyboardInterrupt:
                print("Server shutting down...")
                s.close()
            except Exception:
                continue

def send_join_cluster_request(leader_address):
    try:
        with socket.create_connection(leader_address, timeout=0.3) as sock:
            address = f'{leader_address[0]}:{leader_address[1]}'
            byte_message = b'JOIN' + b'|--|' + b'address'
            sock.sendall(byte_message)

            response = sock.recv(4096).decode("utf-8")
            # print(f"Raw response from {peer}: '{response}'")

    except (socket.timeout, socket.error, json.JSONDecodeError) as e:
        print('cannot connect to leader')
        pass

if __name__ == "__main__":
    start_server()
