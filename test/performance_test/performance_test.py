import time
import os
import sys
import threading
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from collections import defaultdict
import random

# lock plt
lock = threading.Lock()

# add main dir to sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if main_dir not in sys.path:
    sys.path.append(main_dir)

from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get

def performance_test_with_different_instances(client_id,hot_key_percent, hot_key_access_percent , results):
    num_instances = 100
    config_file = "config.txt" 

    total_keys = 1000  
    num_hot_keys = int(total_keys * hot_key_percent)  # count hot_key_num
    hot_keys = set(random.sample(range(total_keys), num_hot_keys))  # 随机选择热门键集合

    # instances num + 10 every time
    for i in range(10, num_instances + 1, 10):
        # temp configuration file name, add instance information when increasing
        temp_config_file = f"configure_temp_instancenum_{i}.txt"  
        #clear file
        with lock:
            with open(temp_config_file, 'w'):
                pass
            # read first i lines to temp_config
            with open( config_file, 'r') as src:
                lines = src.readlines()
                selected_lines = lines[:i]
                # write to temp_config  
                with open(temp_config_file, 'a') as tgt:
                    tgt.writelines(selected_lines)
                    print(f"[Client {client_id}] Copied first {i} lines to {temp_config_file}.")

        # init connection
        if kv739_init(temp_config_file) == 0:
            print(f"[Client {client_id}] Successfully connected to {i} instances.")
        else:
            print(f"[Client {client_id}] Failed to connect to {i} instances.")
            continue

        successful_puts = 0
        successful_gets = 0
        
        start_time = time.time()
        # test 1000 get and put
        for j in range(total_keys):
            if random.random() < hot_key_access_percent:
                # 访问热门键
                key_index = random.choice(list(hot_keys))
            else:
                # 访问非热门键
                key_index = random.choice([k for k in range(total_keys) if k not in hot_keys])
            
            key = f"key_{client_id}_{i}_{key_index}"
            value = f"value_{client_id}_{i}_{key_index}"
            old_value = " " * 2049 
            
            # PUT and check result
            put_result = kv739_put(key, value, old_value)
            if put_result == 0:
                successful_puts += 1
                #print(f"[Client {client_id}] PUT succeeded for key: {key}, value: {value}")
            else:
                print(f"[Client {client_id}] PUT failed for key: {key}, value: {value}")

            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{client_id}_{i}_{j}"
            get_result = kv739_get(key, get_value)
            if get_result == 0 and get_value.strip() == value:
                successful_gets += 1
                #print(f"[Client {client_id}] GET succeeded for key: {key}, value: {get_value.strip()}")
            else:
                print(f"[Client {client_id}] GET failed for key: {key}, expected value: {value}")

        total_time = time.time() - start_time

        with lock:
            results.append((i ,total_time))

        # close connection
        kv739_shutdown()
        print(f"[Client {client_id}] Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    return

# multi clients
def multi_client_test(num_clients,hot_key_percent, hot_key_access_percent , ax, ax2):
    results = []
    threads = []

    for client_id in range(1, num_clients + 1):
        client_thread = threading.Thread(target=performance_test_with_different_instances, args=(client_id,hot_key_percent, hot_key_access_percent, results))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()

    # count average time with same instances number
    result_dict = defaultdict(lambda: [0])
    # calculate average
    for i, a in results:
        result_dict[i][0] += a  

    # todo draw 3d points
    for i, (a_sum,) in result_dict.items():
        ax.scatter(i, num_clients, a_sum/ 2000 / num_clients * 1000 , color='r', marker='o', label = 'Averate Latency(ms)')
        ax2.scatter(i, num_clients, 2000 * num_clients / a_sum, color='r', marker='o', label = 'Throughput(action/s)')

if __name__ == "__main__":
    fig = plt.figure(figsize=(12, 6))  # 一个图形，包含两个子图
    ax = fig.add_subplot(121, projection='3d')
    
    ax.set_xlabel('X (Server Instances)')
    ax.set_ylabel('Y (Clients Num)')
    ax.set_zlabel('Latency (ms)')

    ax2 = fig.add_subplot(122, projection='3d')
    
    ax2.set_xlabel('X (Server Instances)')
    ax2.set_ylabel('Y (Clients Num)')
    ax2.set_zlabel('Throughput (action/s)')
    
    hot_key_percent = 0.1  # hotkey_percent
    hot_key_access_percent = 0.9  # hotkey_access_percent
    
    # from 1 client to 10 clients
    for c in range(1, 11):
        multi_client_test(c ,hot_key_percent, hot_key_access_percent ,ax, ax2)

    #show graph
    handles, labels = ax.get_legend_handles_labels()
    unique_handles_labels = dict(zip(labels, handles))
    ax.legend(unique_handles_labels.values(), unique_handles_labels.keys())

    #show graph
    handles2, labels2 = ax2.get_legend_handles_labels()
    unique_handles_labels2 = dict(zip(labels2, handles2))
    ax2.legend(unique_handles_labels2.values(), unique_handles_labels2.keys())

    plt.savefig('output_plot.png')
    plt.show()