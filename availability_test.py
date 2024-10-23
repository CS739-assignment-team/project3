import time
import os
import sys
import subprocess
import matplotlib.pyplot as plt

config_file = "easycheckservfile.txt"
server_process_list = []
# add main dir to sys.path
# main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
# if main_dir not in sys.path:
#     sys.path.append(main_dir)

#from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get, kv739_die
from kvstore_client_V2 import kv739_init, kv739_shutdown, kv739_put, kv739_get, kv739_die
#from kvstore_client_V2 import kv739_init, kv739_shutdown, kv739_put, kv739_get

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

# decrese instances
def test_decreasing_instances():
    # init plot
    plt.figure(figsize=(10, 6))
    plt.xlabel('Server Instances')
    plt.ylabel('Sucess Rate (%)')
    instances, get_success_rate, put_success_rate = [], [], []
    
    num_instances = 0

    # get all server ports and run
    with open(config_file, 'r') as file:
        for line in file:
            address, port = line.strip().split(":")
            # todo server run code
            #server_process_list.append(subprocess.Popen(["python3", f"../../test_kv739_server.py --port {port} --servfile {config_file}"]))
            #server_process_list.append(subprocess.Popen(["python3", "../../kvstore_server_V2.py", "--port", port, "--servfile", config_file]))
            server_process_list.append(subprocess.Popen(["python3", "kvstore_server_V2.py", "--port", port, "--servfile", config_file]))
            num_instances += 1
            print(f"server start port:{port}")
    
    time.sleep(3)
    server_process_list.append(subprocess.Popen(["python3", "leader.py", "--servfile", config_file, "--numtokens=2", "--replicationfactor=2", "--port=8000"]))
    #server_process_list.append(subprocess.Popen(["python3", "../../leader.py", "--servfile", config_file, "--numtokens=2", "--replicationfactor=2", "--port=8000"]))

    # wait server to start
    time.sleep(3)

    temp_config_file = "configure_temp.txt"  
    # copy original config to temp config
    with open(config_file, 'r') as src:
        content = src.read()
        with open(temp_config_file, 'w') as tgt:
            tgt.write(content)

    # with open(config_file, 'r') as servfile:
    #     sn = servfile.readline()
    # if kv739_init(sn, config_file) == 0:
    #     print(f"Successfully connected.")

    successful_puts = 0
    successful_gets = 0

    # decrese instances number one by one and record success ratio
    #for i in range(num_instances, 0, -1):
    while(num_instances != 0):
        print("server nodes number:", num_instances)
        # print(f"Starting {i} instances...")
        
        with open(temp_config_file, 'r') as servfile:
            sn = servfile.readline()
        if kv739_init(sn, temp_config_file) == 0:
            print(f"Successfully connected to {num_instances} instances.")
        else:
            print(f"Failed to connect to {num_instances} instances.")
            continue

        successful_puts = 0
        successful_gets = 0
        for j in range(30):
            # PUT and check result
            key = f"key_{num_instances}_{j}"
            value = f"value_{num_instances}_{j}"
            old_value = " " * 2049  
            
            # todo
            #put_result = kv739_put(key, value, old_value)
            put_result = kv739_put(key, value)
            print("put result:",put_result)
            if isinstance(put_result, tuple):  # ifput_result is tuple
                successful_puts += 1
            elif put_result == 1:
                successful_puts += 1
                print(f"PUT succeeded for key: {key}, value: {value}")
            else:
                print(f"PUT failed for key: {key}, value: {value}")
            
            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{num_instances}_{j}"
            #todo
            #get_result = kv739_get(key, get_value)
            get_result = kv739_get(key)
            if isinstance(get_result, tuple):  # ifput_result is tuple
                if get_result[0] == 0 and get_result[1].strip() == value:
                    successful_gets += 1
                    #print(f"GET succeeded for key: {key}, value: {get_value.strip()}")
                else:
                    print(f"GET failed for key: {key}, expected value: {value}")
            else:
                print(f"GET failed for key: {key}, expected value: {value}")

        print(f"instance number:{num_instances}, successful PUTs rate: {successful_puts}/{30}, successful GETs rate: {successful_gets}/{30}.")
        #draw graph
        instances.append(num_instances)
        put_success_rate.append(float(successful_puts/30*100))
        get_success_rate.append(float(successful_gets/30*100))

        kv739_shutdown()
        #print(f"Shutdown connection to {num_instances} instances.\n")

        # record the invalid node
        server_name =""
        # remove last line in temp config, simulate a node failure
        # adjust step
        if(num_instances > 10):
            with open(temp_config_file, 'r') as file:
                lines = file.readlines()
                for count in range (10):
                    if lines:
                        server_name = lines[-1]
                        lines = lines[:-1]
                    num_instances -= 1
                    kv739_die( server_name ,1)
                with open(temp_config_file, 'w') as file:
                    file.writelines(lines)
        else:
            with open(temp_config_file, 'r') as file:
                lines = file.readlines()
                if lines:
                    server_name = lines[-1]
                    lines = lines[:-1]
                    with open(temp_config_file, 'w') as file:
                        file.writelines(lines)
                num_instances -= 1
                kv739_die( server_name ,1)
            #kill server's last instance
            #todo
        time.sleep(1)
        #server_process_list[-1].terminate()

    #todo
    for process in server_process_list:
        process.terminate()
    plt.plot(instances, get_success_rate, 'b-o', label="GET Success Rate")
    plt.plot(instances, put_success_rate, 'g-s', label="PUT Success Rate")
    plt.legend()

    plt.savefig('availability_output_plot.png')
    plt.show()
    print(get_success_rate)
    print(put_success_rate)

if __name__ == "__main__":
    try:
        test_decreasing_instances()
    except Exception as e:
        print(e)
        handle_keyboard_interrupt(None, None)