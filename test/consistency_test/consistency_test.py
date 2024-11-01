import time
import os
import sys
import threading
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from collections import defaultdict
import subprocess

config_file = "servfile.txt"
num_instances = 100
# lock plt
lock = threading.Lock()

# add main dir to sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if main_dir not in sys.path:
    sys.path.append(main_dir)

#from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get
from kvstore_client_V2 import kv739_init, kv739_shutdown, kv739_put, kv739_get

server_process_list =[]

# 定义处理 KeyboardInterrupt 信号的函数
def handle_keyboard_interrupt(signum, frame):
    print("\nKeyboardInterrupt detected. Terminating all child processes...")

    # 遍历所有子进程并终止它们
    for process in server_process_list:
        try:
            process.terminate()  # 尝试优雅地终止子进程
            process.wait(timeout=1)  # 等待子进程退出
        except subprocess.TimeoutExpired:
            process.kill()  # 强制杀死子进程
            print(f"Forcefully killed process with PID {process.pid}")

    print("All child processes terminated. Exiting...")
    sys.exit(0)  # 退出主进程

def consistency_test_with_different_instances(client_id,results):
    # instances num + 10 every time
    for i in range(10, num_instances + 1, 10):
        # temp configuration file name, add instance information when increasing
        temp_config_file = f"configure_temp_instancenum_{i}.txt"  

        with open(temp_config_file, 'r') as servfile:
            sn = servfile.readline()
        # init connection
        if kv739_init(sn, temp_config_file) == 0:
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
            put_result = kv739_put(key, value, old_value, temp_config_file)
        
            if isinstance(put_result, tuple):  # ifput_result is tuple
                successful_puts += 1
            elif put_result == 0:
                successful_puts += 1
                #print(f"[Client {client_id}] PUT succeeded for key: {key}, value: {value}")
            else:
                print(f"[Client {client_id}] PUT failed for key: {key}, value: {value}")

            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{client_id}_{i}_{j}"
            get_result = kv739_get(key , temp_config_file)
            if isinstance(get_result, tuple):  # ifput_result is tuple
                if get_result[0] == 0 and get_result[1].strip() == value:
                    successful_gets += 1
                    #print(f"[Client {client_id}] GET succeeded for key: {key}, value: {get_value.strip()}")
                else:
                    print(f"in consistency happen: [Client {client_id}] GET failed for key: {key}, expected value: {value}, get value: {get_value}")
            else:
                print(f"in consistency happen: [Client {client_id}] GET failed for key: {key}, expected value: {value}, get value: {get_value}")

        print(f"instance number:{i}, successful PUTs rate: {successful_puts}/1000, successful GETs rate: {successful_gets}/1000.")

        # save results to list
        with lock:
            results.append((i ,successful_gets, successful_puts))

        # close connection
        kv739_shutdown(temp_config_file)
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
    try:
        # get all server ports and run
        with open(config_file, 'r') as file:
            for line in file:
                address, port = line.strip().split(":")
                # todo server run code
                #server_process_list.append(subprocess.Popen(["python3", f"../../test_kv739_server.py --port {port} --servfile {config_file}"]))
                server_process_list.append(subprocess.Popen(["python3", "../../kvstore_server_V2.py", "--port", port, "--servfile", config_file]))
                num_instances += 1
                print(f"server start port:{port}")

        fig = plt.figure(figsize=(10, 6))
        ax = fig.add_subplot(111, projection='3d')
        
        ax.set_xlabel('X (Server Instances)')
        ax.set_ylabel('Y (Clients Num)')
        ax.set_zlabel('Success Rate (%)')
        
        config_file = "config.txt" 
        for i in range(10, num_instances + 1, 10):
            temp_config_file = f"configure_temp_instancenum_{i}.txt"  
            with open(temp_config_file, 'w'):
                pass
            # read first i lines to temp_config
            with open( config_file, 'r') as src:
                lines = src.readlines()
                selected_lines = lines[:i]
                # write to temp_config  
                with open(temp_config_file, 'a') as tgt:
                    tgt.writelines(selected_lines)

        # wait server to start
        time.sleep(3)
        # from 1 client to 10 clients
        for c in range(1, 11):
            multi_client_test(c ,ax)

        #todo
        for process in server_process_list:
            process.terminate()
        #show graph
        handles, labels = ax.get_legend_handles_labels()
        unique_handles_labels = dict(zip(labels, handles))
        ax.legend(unique_handles_labels.values(), unique_handles_labels.keys())
        plt.savefig('output_plot.png')
        plt.show()
    except Exception as e:
        handle_keyboard_interrupt(None, None)