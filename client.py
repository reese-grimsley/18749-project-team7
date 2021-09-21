#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import argparse
import DebugLogger, constants
from helper import is_valid_ipv4

logger = DebugLogger.get_logger('client')

def parse_args():
    parser = argparse.ArgumentParser(description="Application Server")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The period between each heartbeat, in seconds', type=str)
    parser.add_argument('-c', '--client_id', metavar='c', default=1, help="A client identifier (an integer, for simplicity)", type=int) #could also just be a string
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    return args.ip, args.port, args.client_id

def run_client(ip, port, client_id):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((ip, port))
            request_number = 1
            while True:

                data = input("\nType in something to send!\n")
                if data == '': data = "Hello World"
                data += ' ' + str(request_number)

                client_socket.sendall(data.encode('utf-8'))
                response_data = client_socket.recv(1024)

                if response_data != b'':
                    logger.warning("Nothing received from server; connection may be closed")
                    #TODO; something much more intelligent here. Retry making the connection? Contact the replica manager? Contact IT? Cry?
                logger.debug('Received [%s]', response_data.decode('utf-8'))

                request_number += 1
    
    except KeyboardInterrupt:
        logger.critical('Keyboard interrupt in client; exiting')

if __name__ == "__main__":
    ip, port, client_id = parse_args() #won't we need multiple ip, port pairs for each of the replicas? Can pull from config file, CLI, or even RM
    run_client(ip, port, client_id)