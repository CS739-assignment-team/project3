import socket
import re
import argparse
import threading
import random
import pickle
from utils import extract_server_url

thread_local = threading.local()
servfile = ''

def reconnect():
    try:
        with open(servfile, 'r') as file:
            servers = file.read().splitlines()
            if not servers:
                print("No alternative servers found in the file.")
                return -1

            alternative_server = random.choice(servers)
            print(f"Attempting connection to alternative server: {alternative_server}")

            HOST, PORT = alternative_server.split(':')
            PORT = int(PORT)

            ip_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
            if not ip_pattern.match(HOST):
                HOST = socket.gethostbyname(HOST)

            thread_local.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            thread_local.conn.connect((HOST, PORT))
            print(f"Connected to {HOST}:{PORT}")
            return 0
    except Exception as e:
        print(f"Failed to reconnect to any server: {e}")
        return -1

def kv739_init(server_name, servers_file):
    global servfile
    servfile = servers_file
    try:
        HOST, PORT = server_name.split(':')
        PORT = int(PORT)

        ip_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
        if not ip_pattern.match(HOST):
            try:
                HOST = socket.gethostbyname(HOST)
                print(f"Resolved DNS name to IP: {HOST}")
            except socket.gaierror:
                print("Failed to resolve DNS name")
                return -1

        # HOST = 'localhost'
        # print(f"host: {HOST} port: {PORT}")
        thread_local.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        thread_local.conn.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")
        return 0

    except Exception as e:
        print(f"Connection error to {server_name}: {e}")
        return reconnect()

def init_server_without_reconnect(server_name):
    HOST, PORT = server_name.split(':')
    PORT = int(PORT)

    ip_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
    if not ip_pattern.match(HOST):
        try:
            HOST = socket.gethostbyname(HOST)
            print(f"Resolved DNS name to IP: {HOST}")
        except socket.gaierror:
            print("Failed to resolve DNS name")
            return -1

    # HOST = 'localhost'
    # print(f"host: {HOST} port: {PORT}")
    thread_local.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    thread_local.conn.connect((HOST, PORT))
    print(f"Connected to {HOST}:{PORT}")
    return 0

def kv739_shutdown():
    conn = thread_local.conn
    try:
        if conn:
            conn.sendall(b'SHUTDOWN')
            response = conn.recv(1024)
            print(response.decode('utf-8'))
            
            conn.close()
            print("Connection closed and state freed.")
            return 0
        else:
            print("No active connection to close.")
            return -1
    except (ConnectionResetError, socket.error):
        print("Connection to server lost during shutdown.")
        return reconnect()
    except Exception as e:
        print(f"Error during shutdown: {e}")
        return -1

def init_random_connection():
    reconnect()

def kv739_get(key):
    conn = thread_local.conn
    MAX_VALUE_SIZE = 2048

    try:
        conn.sendall(f'GET {key}'.encode('utf-8'))
        response = conn.recv(4096)

        messages = response.split(b'|--|', 1)

        if len(messages) > 1:
            print(messages)
            code = messages[0].decode('utf-8')
            if code == "RETRY_PRIMARY":
                replicas = pickle.loads(messages[1])
                kv739_shutdown()
                found_server = False
                for index, replica in enumerate(replicas):
                    curr_server = replica.rpartition('-')[0]
                    try:
                        response = init_server_without_reconnect(curr_server)
                        if response == -1:
                            continue
                        conn = thread_local.conn
                        conn.sendall(f'GET {key} {index}'.encode('utf-8'))
                        response = conn.recv(4096)
                        found_server = True
                        break
                    except Exception as e:
                        print(e)
                        print(f'exception occured while connecting to server {curr_server} ignoring')
                        continue
                #couldnot connect to any server should add more backups or redistribute 
                if not found_server:
                    return -1


        response = response.decode("utf-8")
        if response.startswith('VALUE'):
            value = response.split(' ', 1)[1]
            if len(value) > MAX_VALUE_SIZE:
                print("Error: Retrieved value exceeds maximum allowed size.")
                return -1
            return (0, value)

        elif response == "KEY_NOT_FOUND":
            return 1
        else:
            print("Connection to server lost during GET request.")
            return reconnect()
        
    except (ConnectionResetError, socket.error):
        print("Connection to server lost during GET request.")
        return reconnect()
    except Exception as e:
        print(f"Error during GET: {e}")
        return -1

def kv739_put(key, new_value):
    conn = thread_local.conn
    MAX_VALUE_SIZE = 2048

    if len(new_value) > MAX_VALUE_SIZE:
        print("Error: New value exceeds maximum allowed size...")
        return -1
    
    try:
        conn.sendall(f'PUT {key} {new_value}'.encode('utf-8'))
        response = conn.recv(4096)

        messages = response.split(b'|--|', 1)

        if len(messages) > 1:
            code = messages[0].decode('utf-8')
            if code == "RETRY_PRIMARY":
                replicas = pickle.loads(messages[1])
                kv739_shutdown()
                found_server = False
                for index, replica in enumerate(replicas):
                    curr_server = replica.rpartition('-')[0]
                    try:
                        response = init_server_without_reconnect(curr_server)
                        if response == -1:
                            continue
                        conn = thread_local.conn
                        conn.sendall(f'PUT {key} {new_value} {index}'.encode('utf-8'))
                        response = conn.recv(4096)
                        found_server = True
                        break
                    except Exception as e:
                        print(e)
                        print(f'exception occured while connecting to server {curr_server} ignoring')
                        continue
                #couldnot connect to any server should add more backups or redistribute 
                if not found_server:
                    return -1

        response = response.decode("utf-8")

        if response.startswith('UPDATED'):
            old_value = response.split(' ')[1]
            return (0, old_value)
        elif response == "INSERTED":
            return 1
        else:
            print("Unexpected server response during PUT, reconnecting...")
            return reconnect()

    except (ConnectionResetError, socket.error):
        print("Connection to server lost during PUT request.")
        return reconnect()
    except Exception as e:
        print(f"Error during PUT: {e}")
        return -1

def kv739_die(server_name, clean):
    kv739_shutdown()
    try:
        init_server_without_reconnect(server_name)
        conn = thread_local.conn
        conn.sendall(f'DIE {server_name} {clean}'.encode('utf-8'))
        response= conn.recv(4096).decode('utf-8')
        return response
    except Exception:
        print(f'Failed killing server {server_name}')

def main():
    parser = argparse.ArgumentParser(description="Key-Value Store Client")
    parser.add_argument('--init', help="Server address in format host:port", required=True)
    parser.add_argument('--servfile', help="filename of list of server addresses", required=True)

    args = parser.parse_args()

    if kv739_init(args.init, args.servfile) != 0:
        print("Failed to initialize connection.")
        return

    while True:
        command = input("Enter command (get <key>, put <key> <value>, shutdown): ").strip().split()

        if not command:
            continue

        elif command[0] == 'get' and len(command) == 2:
            response = kv739_get(command[1])
            if type(response) == int:
                if response == 1:
                    print("Key not found...")
                elif response == -1:
                    break
            else:
                print(f"Value: {response[1]}")

        elif command[0] == 'put' and len(command) == 3:
            response = kv739_put(command[1], command[2])
            if type(response) == int:
                break
            elif len(response) == 3:
                print(f"Old Value: {response[1]}\nNew Value: {response[2]}")
            elif len(response) == 2:
                print(f"New Value: {response[1]}")

        elif command[0] == 'shutdown':
            kv739_shutdown()
            break

        else:
            print("Invalid command. Available commands: get <key>, put <key> <value>, shutdown.")

if __name__ == '__main__':
    main()