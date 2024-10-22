import time
import os
import sys
import threading
import matplotlib.pyplot as plt
from collections import defaultdict
import subprocess

config_file = "easycheckservfile.txt"
# lock plt
lock = threading.Lock()

#from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get
from kvstore_client_V2 import kv739_init, kv739_shutdown, kv739_put, kv739_get

server_process_list =[]
clients_count, success_rate_without_inconsistency = [], []

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

def consistency_test_with_different_instances(client_id,results):
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

        successful_puts = 0
        successful_gets = 0
        
        # test 1000 get and put
        for j in range(30):
            key = f"key_{client_id}_{i}_{j}"
            value = f"value_{client_id}_{i}_{j}"
            old_value = " " * 2049 
            
            # PUT and check result
            put_result = kv739_put(key, value)
            print("put result:",put_result)
            if isinstance(put_result, tuple):  # ifput_result is tuple
                successful_puts += 1
            elif put_result == 1:
                successful_puts += 1
            else:
                print(f"[Client {client_id}] PUT failed for key: {key}, value: {value}")

            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{client_id}_{i}_{j}"
            get_result = kv739_get(key)
            if isinstance(get_result, tuple):  # ifput_result is tuple
                if get_result[0] == 0 and get_result[1].strip() == value:
                    successful_gets += 1
                    #print(f"[Client {client_id}] GET succeeded for key: {key}, value: {get_value.strip()}")
                else:
                    print(f"in consistency happen: [Client {client_id}] GET failed for key: {key}, expected value: {value}, get value: {get_value}")
            else:
                print(f"in consistency happen: [Client {client_id}] GET failed for key: {key}, expected value: {value}, get value: {get_value}")

        print(f"instance number:{i}, successful PUTs rate: {successful_puts}/30, successful GETs rate: {successful_gets}/30.")

        # save results to list
        with lock:
            results.append(successful_gets)

        # close connection
        kv739_shutdown()
        print(f"[Client {client_id}] Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    return

# multi clients
def multi_client_test(num_clients):
    threads = []
    results = []
    for client_id in range(1, num_clients + 1):
        client_thread = threading.Thread(target=consistency_test_with_different_instances, args=(num_clients, results))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()
    
    sum = 0
    for r in results:
        sum += r
    success_rate_without_inconsistency.append(sum / num_clients / 30 * 100)

    # todo draw 3d points
if __name__ == "__main__":
    num_instances = 0
    try:
        # get all server ports and run
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

        # from 1 client to 10 clients
        c = 1
        while True:
            clients_count.append(c)
            multi_client_test(c)
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
        

        #todo
        for process in server_process_list:
            process.terminate()
        #show graph
        plt.figure(figsize=(10, 6))
        plt.xlabel('Clients Count')
        plt.ylabel('Sucess Rate Without Inconsistency(%)')
        plt.plot(clients_count, success_rate_without_inconsistency, 'b-o', label="successful rate without inconsistency")
        plt.legend()
        plt.savefig('consistency_output_plot.png')
        print(success_rate_without_inconsistency)
        plt.show()
    except Exception as e:
        handle_keyboard_interrupt(None, None)