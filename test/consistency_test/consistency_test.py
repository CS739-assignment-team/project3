import time
import os
import sys
import threading

# add main dir to sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if main_dir not in sys.path:
    sys.path.append(main_dir)

from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get

def consistency_test_with_different_instances(client_id):
    num_instances = 100
    config_file = "config.txt" 

    # instances num + 10 every time
    for i in range(10, num_instances + 1, 10):
        # temp configuration file name, add instance information when increasing
        temp_config_file = f"configure_temp_client_{client_id}_{i}.txt"  
        #clear file
        with open(temp_config_file, 'w'):
            pass
        # 打开源文件 A 读取 i 行写入B，模拟接下来与i个实例连接
        with open( config_file, 'r') as src:
            # 选择前i行
            lines = src.readlines()
            selected_lines = lines[:i]

            # 打开目标文件 B 追加模式写入
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

        # close connection
        kv739_shutdown()
        print(f"[Client {client_id}] Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    return

# 多线程测试，每个线程模拟一个客户端
def multi_client_test(num_clients):
    threads = []

    # 创建并启动 num_clients 个线程，每个线程模拟一个客户端
    for client_id in range(1, num_clients + 1):
        client_thread = threading.Thread(target=consistency_test_with_different_instances, args=(client_id,))
        threads.append(client_thread)
        client_thread.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    multi_client_test(7)