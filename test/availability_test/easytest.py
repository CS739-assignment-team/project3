import subprocess
import time
import sys
from kvstore_client_V2 import kv739_init, kv739_put, kv739_get


config_file = "servfile.txt"
server_process_list = []

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


def test_decreasing_instances():    
    # get all server ports and run
    with open(config_file, 'r') as file:
        for line in file:
            address, port = line.strip().split(":")
            # todo server run code
            #server_process_list.append(subprocess.Popen(["python3", f"../../test_kv739_server.py --port {port} --servfile {config_file}"]))
            server_process_list.append(subprocess.Popen(["python3", "kvstore_server_V2.py", "--port", port, "--servfile", config_file]))
            print(f"server start port:{port}")
    
    time.sleep(3)
    server_process_list.append(subprocess.Popen(["python3", "leader.py", "--servfile", config_file, "--numtokens=2", "--replicationfactor=2", "--port=8000"]))

    time.sleep(3)
    
    with open(config_file, 'r') as servfile:
        sn = servfile.readline()
    if kv739_init(sn, config_file) == 0:
        print(f"Successfully connected.")

    print("CKPT1")
    key = f"key_100_1"
    value = f"value_100_1"
    put_result = kv739_put(key, value)
    print("putres:",put_result)
       
    print("CKPT2")
    get_result = kv739_get(key)
    print("getvalue:",get_result)

    for process in server_process_list:
        process.terminate()

if __name__ == "__main__":
    try:
        test_decreasing_instances()
    except Exception as e:
        print(e)
        handle_keyboard_interrupt(None, None)
