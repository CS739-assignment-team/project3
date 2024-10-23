from sortedcontainers import SortedList
import random 
import mmh3

class ConsistentHash:
    HASH_BITS = 64
    '''
    nodes: number of nodes in the system
    Token Map: virtual nodes are mapped to real nodes on the hash ring
    num_tokens: Tokens are placed randomly on the ring and num_tokens represent the number of tokens/virtual nodes
    tokens: list of all tokens 
    replicas: number of replicas for each key
    '''
    def findtoken(tokens: SortedList, searchtoken):
        ind = tokens.bisect_left(searchtoken)
        if ind != len(tokens) and tokens[ind] == searchtoken:
            return ind
        return -1

    def __init__(self, num_nodes, num_tokens, replicas):
        self.num_nodes = num_nodes  
        self.num_tokens = num_tokens
        self.replicas = replicas

        #setup virtual nodes 
        self.tokens = [0]*num_tokens
        virtual_nodes = SortedList([])
        token_number = 0 
        while token_number < num_tokens:
            vnode = random.randint(0, 2**self.HASH_BITS - 1)
            index = self.findtoken(virtual_nodes, vnode)
            if index == -1:
                virtual_nodes.add(vnode)
                token_number += 1
        self.tokens = list(virtual_nodes)

        #token Map 
        self.node_token_map = [[self.tokens[j+i] for i in range(0, num_tokens, num_nodes)] for j in range(num_nodes)]
    
    def remove_node(self, node_number):
        '''
        get the virtual nodes and assign them to next set of nodes
        '''
        vnodes  = self.node_token_map[node_number]

        

        


