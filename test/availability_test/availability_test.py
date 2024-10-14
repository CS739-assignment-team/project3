import time
import os
import sys

# add main dir to sys.path
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if main_dir not in sys.path:
    sys.path.append(main_dir)

from test_kv739_client import kv739_init, kv739_shutdown, kv739_put, kv739_get

# increase instances
def test_increasing_instances():
    # default instance num is 100
    num_instances = 100
    config_file = "config.txt" 
    # temp configuration file name, add instance information when increasing
    temp_config_file = "configure_temp.txt"  
    
    # record availability nums
    successful_puts = 0
    successful_gets = 0

    # original config file
    src = open(config_file, 'r')
    # clear temp config
    with open(temp_config_file, 'w'):
        pass 
    # increase instances number one by one and record success ratio
    for i in range(1, num_instances + 1):
        print(f"Starting {i} instances...")
        
        # add a line to temp config
        with open(temp_config_file, 'a') as tgt:
            line = src.readline()
            tgt.write(line)

        if kv739_init(temp_config_file) == 0:
            print(f"Successfully connected to {i} instances.")
        else:
            print(f"Failed to connect to {i} instances.")
            continue
        
        successful_puts = 0
        successful_gets = 0
        
        for j in range(1000):
            key = f"key_{i}_{j}"
            value = f"value_{i}_{j}"
            old_value = " " * 2049 
            
            # PUT and check result
            put_result = kv739_put(key, value, old_value)
            if put_result == 0:
                successful_puts += 1
                print(f"PUT succeeded for key: {key}, value: {value}")
            else:
                print(f"PUT failed for key: {key}, value: {value}")
            
            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{i}_{j}"
            get_result = kv739_get(key, get_value)
            if get_result == 0 and get_value.strip() == value:
                successful_gets += 1
                print(f"GET succeeded for key: {key}, value: {get_value.strip()}")
            else:
                print(f"GET failed for key: {key}, expected value: {value}")

        print(f"instance number:{i}, successful PUTs rate: {successful_puts}/{1000}, successful GETs rate: {successful_gets}/{1000}.")

        # close connection
        kv739_shutdown()
        print(f"Shutdown connection to {i} instances.\n")
        
        # wait
        time.sleep(0.1)

    src.close()

# decrese instances
def test_decreasing_instances():
    num_instances = 100
    config_file = "config.txt" 
    # temp configuration file name, add instance information when increasing
    temp_config_file = "configure_temp.txt"  
    
    # copy original config to temp config
    with open(config_file, 'r') as src:
        content = src.read()
        with open(temp_config_file, 'w') as tgt:
            tgt.write(content)

    successful_puts = 0
    successful_gets = 0

    # decrese instances number one by one and record success ratio
    for i in range(num_instances, 0, -1):
        print(f"Starting {i} instances...")
        
        if kv739_init(temp_config_file) == 0:
            print(f"Successfully connected to {i} instances.")
        else:
            print(f"Failed to connect to {i} instances.")
            continue
        
        successful_puts = 0
        successful_gets = 0
        
        for j in range(1000):
            # PUT and check result
            key = f"key_{i}_{j}"
            value = f"value_{i}_{j}"
            old_value = " " * 2049  
            
            put_result = kv739_put(key, value, old_value)
            if put_result == 0:
                successful_puts += 1
                print(f"PUT succeeded for key: {key}, value: {value}")
            else:
                print(f"PUT failed for key: {key}, value: {value}")
            
            # GET and check result
            get_value = " " * 2049 #todo
            get_value =  f"value_{i}_{j}"
            get_result = kv739_get(key, get_value)
            if get_result == 0 and get_value.strip() == value:
                successful_gets += 1
                print(f"GET succeeded for key: {key}, value: {get_value.strip()}")
            else:
                print(f"GET failed for key: {key}, expected value: {value}")

        print(f"instance number:{i}, successful PUTs rate: {successful_puts}/{1000}, successful GETs rate: {successful_gets}/{1000}.")

        kv739_shutdown()
        print(f"Shutdown connection to {i} instances.\n")

        # remove last line in temp config, simulate a node failure
        with open(temp_config_file, 'r') as file:
            lines = file.readlines()
            if lines:
                lines = lines[:-1]
                with open(temp_config_file, 'w') as file:
                    file.writelines(lines)
        # 每wait
        time.sleep(0.1)



if __name__ == "__main__":
    test_increasing_instances()
    test_decreasing_instances()