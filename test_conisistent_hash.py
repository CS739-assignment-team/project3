import unittest
from unittest.mock import patch, MagicMock
from ConsistentHash import ConsistentHash  # Assuming this code is in `consistent_hash.py`
import pickle

class TestConsistentHash(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nodes = ['127.0.0.1:5000', '127.0.0.1:5001', '127.0.0.1:5002']
        cls.num_tokens = 3
        cls.replicas = 2
        cls.leader_address = '127.0.0.1:8000'
        cls.consistent_hash = ConsistentHash(cls.nodes, cls.num_tokens, cls.replicas, cls.leader_address)

    def test_1_initialization(self):
        """Test that the object initializes with correct token mappings."""
        # Check token list length
        expected_num_tokens = len(self.nodes) * self.num_tokens

        self.assertEqual(len(self.consistent_hash.tokens), expected_num_tokens)
        
        # Verify token mappings match the number of nodes and replicas
        for token, replicas in self.consistent_hash.token_map.items():
            self.assertEqual(len(replicas), self.replicas)
        
        # print('token map after nodes initialization')
        # print(self.consistent_hash.nodes)

    def test_2_add_node(self):
        """Test adding a node to the consistent hash ring."""
        new_node = '127.0.0.1:5003'
        initial_token_count = len(self.consistent_hash.tokens)
        
        self.consistent_hash.add_node(new_node)
        
        # print('token map after adding node')
        # print(self.consistent_hash.nodes)
        # Verify the new node is added
        self.assertIn(new_node, self.consistent_hash.nodes)
        
        # Ensure tokens increased as expected
        self.assertEqual(len(self.consistent_hash.tokens), initial_token_count + self.num_tokens)
        
        # Check if the token map is updated for the new node’s tokens
        new_node_tokens = [f"{new_node}-{i+1}" for i in range(self.num_tokens)]
        for virtual_node in new_node_tokens:
            server_hash = self.consistent_hash._hash(virtual_node)
            self.assertIn(server_hash, self.consistent_hash.tokens)

    def test_3_remove_node(self):
        """Test removing a node from the consistent hash ring."""
        node_to_remove = '127.0.0.1:5002'
        initial_token_count = len(self.consistent_hash.tokens)
        
        self.consistent_hash.remove_node(node_to_remove)
        
        # print('token map after removing node')
        # print(self.consistent_hash.nodes)

        # Verify the node is removed from the nodes list
        self.assertNotIn(node_to_remove, self.consistent_hash.nodes)
        
        # Check that tokens have decreased
        self.assertEqual(len(self.consistent_hash.tokens), initial_token_count - self.num_tokens)
        
        # Ensure no token maps to the removed node
        for token, nodes in self.consistent_hash.token_map.items():
            for node in nodes:
                self.assertNotIn(node_to_remove, node)

    
if __name__ == '__main__':
    unittest.main()