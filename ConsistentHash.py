from sortedcontainers import SortedList
from collections import defaultdict, deque
import random 
import mmh3
import re
import socket 
import pickle
import os
import threading
from kvstore_client_V2 import *

class ConsistentHash:
    HASH_BITS = 128
    
    '''
    USAGE:
    nodes: nodes in the system (host:port)
    Token Map: virtual nodes are mapped to real nodes on the hash ring
    num_tokens: Tokens are placed randomly on the ring and num_tokens represent the number of tokens/virtual nodes
    tokens: list of all tokens 
    replicas: number of replicas for each key
    Master Persistent State: 
    list of nodes
    list of tokens 
    num_tokens_per_node
    hash function used 
    token_map - virtual nodes mapped to set of physical nodes Eg: t1 -> {n1, n2, n3}
    '''

    def __init__(self, nodes, num_tokens, replicas, leader_address):
        self.num_nodes = len(nodes)  
        self.num_tokens_per_node = num_tokens
        self.replicas = replicas
        self.nodes = nodes
        self.leader_address = leader_address

        #setup virtual nodes 
        virtual_nodes = SortedList([])
        token_to_node_map = {}
        for i in range(self.num_nodes):
            for j in range(num_tokens):
                server_name = f"{nodes[i]}-{j+1}"
                server_hash = self._hash(server_name)
                virtual_nodes.add(server_hash)

                if server_hash in token_to_node_map:
                    print(f'Collision in server hash for {token_to_node_map[server_hash]} and {server_name}')
                    print('Choose a better hash')
                    #TODO: provide options for family of hash functions 
                else:
                    token_to_node_map[server_hash] = server_name
                    
        self.tokens = virtual_nodes

        #Assign token to nodes in ring fashion each vn get rf physical nodes
        self.token_map = defaultdict(deque)
        #token1 -> node1, (replicas-1) - nodes from next tokens
        #for token1 node1 is primary and rest of the nodes are backup

        for index, token in enumerate(self.tokens):
            for i in range(replicas):
                self.token_map[token].append(self.token_map[self.tokens[(index+i)%len(self.tokens)]])

        #TODO: replicate the token map to all nodes 
        self._save_and_share_globalstate()

    def _hash(self, key):
        return mmh3.hash128(key)

    def _get_server_ip(self, server_name):
        HOST, PORT = server_name.split(':')
        PORT = int(PORT)

        ip_pattern = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
        if not ip_pattern.match(HOST):
            try:
                HOST = socket.gethostbyname(HOST)
                print(f"Resolved DNS name to IP: {HOST}")
            except socket.gaierror:
                print("Failed to resolve DNS name")
                return "-1"  
        return f"{HOST}:{PORT}"
    
    def _share_data(self, node, byte):
        try:

            HOST, PORT = self._get_server_ip(node).split(':')
            PORT = int(PORT)

            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.connect((HOST, PORT))

            #replicate the global state
            conn.sendall(f'REPLICATE {byte}'.encode('utf-8'))
            response = conn.recv(4096).decode('utf-8')
            #disconnect
            conn.sendall(b'SHUTDOWN')
            response = conn.recv(1024)
            print(response.decode('utf-8'))
            
            conn.close()
        except Exception:
            print(f'Master cannot connet to the node {node} to replicate state')

        
    def _save_and_share_globalstate(self):
        global_state = {
            "token_map": self.token_map,
            "tokens": self.tokens,
            "num_tokens_per_node": self.num_tokens_per_node,
            "replicas": self.replicas,
            "nodes": self.nodes,
            "leader_address": self.leader_address
        }
        #write to temp file and rename 
        with open('sw_state.pickle', 'wb') as file:  # Write binary
            pickle.dump(global_state, file)  # Serializes and saves to file
  
        os.rename('sw_state.pickle' , 'state.pickle')

        byte_data = pickle.dumps(global_state)

        for node in self.nodes:
            threading.Thread(target=self._share_data, args=(node,byte_data), daemon=True).start()

    '''
    node_Address : ip:port or hostname:port
    '''
    def add_node(self, node_address):
        for i in range(self.num_tokens_per_node):
            server_name = f"{node_address}-{i+1}"
            server_hash = mmh3.hash128(server_name)

            #find the neighbours in the ring for the hash 
            leftindex = self.tokens.bisect_left(server_hash)
            if leftindex < len(self.tokens) and self.tokens[leftindex] == server_hash:
                print(f'Error occured hash already exists. Cannot add the server {node_address}')
                return -1
            '''
            [t2, t4} --> split into [t2, t3}, [t3, t4}
            copy contents from t2 -> [t3, t4} and rf-1 next ranges to keep up replication 
            NOTE: rf-1 ranges can be copied from any of the next rf-1 virtual nodes 
            Just add the metadata data will forked by new server
            '''
            self.tokens.add(server_hash) 
            self.nodes.append(node_address)
            #build metadata mapped to this token/virtual node
            #toekn - > servername (A-1)
            self.token_map[server_hash].append(server_name) 
            for i in range(self.num_tokens_per_node-1):
                self.token_map[server_hash].append(self.token_map[self.tokens[(leftindex+i+1)%len(self.tokens)]][0])
        self._save_and_share_globalstate()
            
    
    def remove_node(self, node_address):
        '''
        get the virtual nodes and assign them to next set of nodes
        '''
        for i in range(self.replicas):
            server_name = f'{node_address}-{i+1}'
            server_hash = self._hash(server_name)

            leftindex = self.tokens.bisect_left(server_hash)
            if leftindex == len(self.tokens) or self.tokens[leftindex] != server_hash:
                print(f'token for {server_name} doesnt exist')
                continue

            self.tokens.remove(server_hash)
            self.nodes.remove(node_address)
            #Iterate over the left nodes which still points the deleted server and update it with next server
            #update token map
            leftindex -= 1
            for i in range(self.replicas):
                curr_token = self.tokens[leftindex-i]
                replication_nodes = self.token_map[curr_token]
                replication_nodes.remove(server_name)
                #primary node of tx + rf -- token 
                replication_nodes.append(self.token_map[self.tokens[(leftindex+self.replicas)%len(self.tokens)]][0])

        self._save_and_share_globalstate()

        

        


