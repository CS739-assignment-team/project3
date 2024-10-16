import mmh3
import socket
import re

def hash(key):
    return mmh3.hash128(key)
      
def find_nodes_for_key(tokens,token_map, key):
    key_hash = hash(key)
    index = tokens.bisect_left(key_hash)
    replicated_nodes = []
    if index == len(tokens):
        replicated_nodes = token_map[tokens[-1]]
    elif tokens[index] <= key_hash:
        replicated_nodes = token_map[tokens[index]]
    else:
        replicated_nodes = token_map[tokens[index-1]]
    return replicated_nodes

def extract_server_url(server_name):
    server_name = server_name.rpartition('-')[0]
    HOST, PORT = server_name.split(':')
    PORT = int(PORT)
    
    ip_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
    if not ip_pattern.match(HOST):
        try:
            HOST = socket.gethostbyname(HOST)
            print(f"Resolved DNS name to IP: {HOST}")
        except socket.gaierror:
            print("Failed to resolve DNS name")
            return -1, -1
    return HOST, PORT