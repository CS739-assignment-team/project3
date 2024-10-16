import argparse 
from ConsistentHash import ConsistentHash

def main():
    parser = argparse.ArgumentParser(description="Leader Arguments")
    parser.add_argument('--servfile', help="filename of list of server addresses", required=True)
    parser.add_argument('--numtokens', type=int, help="NUmber of virtual nodes for eah physical server", required=True)
    parser.add_argument('--replicationfactor', type=int, help="replication factor for keys", required=True)
    args = parser.parse_args()

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

    chash = ConsistentHash(physical_servers, args.numtokens, args.replicationfactor)


if __name__ == "__main__":
    main()