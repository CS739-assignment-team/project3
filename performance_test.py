import time
import os
import sys
import threading
import matplotlib.pyplot as plt
from collections import defaultdict
import random
import subprocess

config_file = "easycheckservfile.txt"
# lock plt
lock = threading.Lock()
from kvstore_client_V2 import kv739_init, kv739_shutdown, kv739_put, kv739_get

server_process_list =[]

clients_count, put_throughput, put_latency, get_throughput, get_latency = [],[],[],[],[]
put_total_time = 0
get_total_time = 0

# kill all server process when error occurs
def handle_keyboard_interrupt(signum, frame):
    print("\nKeyboardInterrupt detected. Terminating all child processes...")

    # enumerate all subprocess
    for process in server_process_list:
        try:
            process.terminate()  
            process.wait(timeout=1)  
        except subprocess.TimeoutExpired:
            process.kill()  
            print(f"Forcefully killed process with PID {process.pid}")

    print("All child processes terminated. Exiting...")
    sys.exit(0)  


def performance_test_with_different_instances(client_id,hot_key_percent, hot_key_access_percent):
    global put_total_time, get_total_time  

    total_keys = 30  
    num_hot_keys = int(total_keys * hot_key_percent)  # count hot_key_num
    hot_keys = set(random.sample(range(total_keys), num_hot_keys))  

    # instances num + 10 every time
    for i in range(100, num_instances + 1, 10):
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
            put_result = kv739_put(key, value)
            with lock:
                put_total_time += time.time() - start_time

            start_time = time.time()
            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{client_id}_{i}_{j}"
            get_result = kv739_get(key)
            with lock:
                get_total_time += time.time() - start_time


        # close connection
        kv739_shutdown()
        print(f"[Client {client_id}] Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    return

# multi clients
def multi_client_test(num_clients,hot_key_percent, hot_key_access_percent):
    threads = []
    global put_total_time, get_total_time  

    for client_id in range(1, num_clients + 1):
        client_thread = threading.Thread(target=performance_test_with_different_instances, args=(num_clients,hot_key_percent, hot_key_access_percent))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()

    print("ckpt1")
    put_latency.append(put_total_time /(num_clients * 30))
    get_latency.append(get_total_time /(num_clients * 30))
    put_throughput.append(num_clients * 30 * 1000 / put_total_time)
    get_throughput.append(num_clients * 30 * 1000 / get_total_time)
    put_total_time = 0
    get_total_time = 0
    print("ckpt2")

if __name__ == "__main__":
    num_instances = 0
    try:
        with open(config_file, 'r') as file:
            for line in file:
                address, port = line.strip().split(":")
                # todo server run code
                #server_process_list.append(subprocess.Popen(["python3", f"../../test_kv739_server.py --port {port} --servfile {config_file}"]))
                server_process_list.append(subprocess.Popen(["python3", "kvstore_server_V2.py", "--port", port, "--servfile", config_file]))
                num_instances += 1
                print(f"server start port:{port}")
        time.sleep(3)
        server_process_list.append(subprocess.Popen(["python3", "leader.py", "--servfile", config_file, "--numtokens=2", "--replicationfactor=2", "--port=8000"]))
        time.sleep(3)
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
        
        hot_key_percent = 0.1  # hotkey_percent
        hot_key_access_percent = 0.9  # hotkey_access_percent

        
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
        
        # from 1 client to 10 clients
        c = 1
        while True:
            clients_count.append(c)
            multi_client_test(c,hot_key_percent, hot_key_access_percent)
            if c == 1:
                c = 5
                continue
            if c == 5:
                c = 10
                continue
            if c == 10:
                c = 20
                continue
            if c == 20:
                c = 50
                continue
            if c == 50:
                c = 100
                continue
            if c == 100:
                break
        # clients_count.extend([20,50,100])
        # put_latency.extend([14.0, 25.1, 42.0])
        # get_latency.extend([3.9, 7.2, 12.0])
        # put_throughput.extend([1000/14.0, 1000/25.1, 1000/42.0])
        # get_throughput.extend([1000/3.9, 1000/7.2, 1000/12.0])
        fig = plt.figure(figsize=(12, 6))  
        plt.subplot(1, 2, 1)
        plt.title("Latency")
        plt.plot(clients_count, put_latency, 'b-o', label="PUT Latency")
        plt.plot(clients_count, get_latency, 'g-s', label="GET Latency")
        plt.legend()
        plt.xlabel("Clients count")
        plt.ylabel("Latency(ms)")

        plt.subplot(1, 2, 2)
        plt.title("Throughput")
        plt.plot(clients_count, put_throughput, 'b-o', label="PUT Throughput")
        plt.plot(clients_count, get_throughput, 'g-s', label="GET Throughput")
        plt.legend()
        plt.xlabel("Clients count")
        plt.ylabel("Throughput(ops/s)")

        plt.savefig('performance_output_plot.png')
        plt.show()
    except Exception as e:
        handle_keyboard_interrupt(None, None)