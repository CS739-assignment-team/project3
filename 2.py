import socket
import pickle
from kvstore_client_V2 import *

servfile = 'servfile.txt'
kv739_init('127.0.0.1:4000', 'servfile.txt')
res = kv739_put('key2', 'new_value_4')
print(res)

res = kv739_get('key2')
print(res)

res = kv739_die('127.0.0.1:6000', 1)
print(res)

kv739_shutdown()