import argparse 
from ConsistentHash import ConsistentHash
import socket
import threading
HOST =  socket.gethostname()
PORT = 4000
chash = None
def main():
    parser = argparse.ArgumentParser(description="Leader Arguments")
    parser.add_argument('--servfile', help="filename of list of server addresses", required=True)
    parser.add_argument('--numtokens', type=int, help="NUmber of virtual nodes for eah physical server", required=True)
    parser.add_argument('--replicationfactor', type=int, help="replication factor for keys", required=True)
    parser.add_argument("--port",type=int, help="Server port", required=True)
    args = parser.parse_args()
    global PORT
    PORT = args.port

    physical_servers = []

    try:
    # Attempt to open the servers file 
        with open(args.servfile, 'r') as server_file:
            for line in server_file:
                physical_servers.append(line.strip())


    except FileNotFoundError:
        # Handle the error if the file doesn't exist
        print("Server File doesnt exist to start")
        raise FileNotFoundError

    global chash
    chash = ConsistentHash(physical_servers, args.numtokens, args.replicationfactor, f'{HOST}:{PORT}')
    start_server()

def handle_client(conn, addr):
    print(f"Connected by {addr}")
    while True:
        data = conn.recv(1024).decode("utf-8")
        if data.startswith("DIE"):
            global chash
            chash.remove_node(data[4:])
            conn.sendall('Killed 0'.encode('utf-8'))
            conn.close()
            return

def start_server():

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"Server started on {HOST}:{PORT}")

        while True:
            conn, addr = s.accept()
            client_handler = threading.Thread(target=handle_client, args=(conn, addr))
            client_handler.start()

if __name__ == "__main__":
    main()