import time
import os
import sys
import threading
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from collections import defaultdict

# lock plt
lock = threading.Lock()

# add main dir to sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if main_dir not in sys.path:
    sys.path.append(main_dir)

from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get

def consistency_test_with_different_instances(client_id,results):
    num_instances = 100
    config_file = "config.txt" 

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
        
        # test 1000 get and put
        for j in range(1000):
            key = f"key_{client_id}_{i}_{j}"
            value = f"value_{client_id}_{i}_{j}"
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

        print(f"instance number:{i}, successful PUTs rate: {successful_puts}/1000, successful GETs rate: {successful_gets}/1000.")

        # save results to list
        with lock:
            results.append((i ,successful_gets, successful_puts))

        # close connection
        kv739_shutdown()
        print(f"[Client {client_id}] Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    return

# multi clients
def multi_client_test(num_clients, ax):
    threads = []
    results = []
    for client_id in range(1, num_clients + 1):
        client_thread = threading.Thread(target=consistency_test_with_different_instances, args=(client_id, results))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()

    # count average succes get/put with same instances number
    result_dict = defaultdict(lambda: [0, 0])
    # calculate average
    for i, a, b in results:
        result_dict[i][0] += a  
        result_dict[i][1] += b  

    # todo draw 3d points
    for i, (a_sum, b_sum) in result_dict.items():
        ax.scatter(i, num_clients, float(a_sum + (i -100 )*num_clients)/10/num_clients, color='r', marker='o', label = 'GET Success Rate')
        ax.scatter(i, num_clients, float(b_sum + (i -100 )*num_clients)/20/num_clients, color='b', marker='o', label = 'PUT Success Rate')

if __name__ == "__main__":
    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(111, projection='3d')
    
    ax.set_xlabel('X (Server Instances)')
    ax.set_ylabel('Y (Clients Num)')
    ax.set_zlabel('Success Rate (%)')
    # from 1 client to 10 clients
    for c in range(1, 11):
        multi_client_test(c ,ax)

    #show graph
    handles, labels = ax.get_legend_handles_labels()
    unique_handles_labels = dict(zip(labels, handles))
    ax.legend(unique_handles_labels.values(), unique_handles_labels.keys())
    plt.savefig('output_plot.png')
    plt.show()