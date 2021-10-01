#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import argparse
import time
import threading, queue

import DebugLogger, constants
from helper import is_valid_ipv4, parse_addresses_file, addr_to_ip_port
import messages

logger = DebugLogger.get_logger('client')


def parse_args():
    parser = argparse.ArgumentParser(description="Application Server")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The IPv4 address of the application server', type=str)
    parser.add_argument('-a', '--addresses_path', metavar='a', default='./server_addresses.txt', help='A path to a file containing a list of IP addresses in IPv4 format with ports')
    parser.add_argument('-c', '--client_id', metavar='c', default=1, help="A client identifier (an integer, for simplicity)", type=int) #could also just be a string
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)
    if args.addresses_path:
        address_info = parse_addresses_file(args.addresses_path)
    else:
        address_info = None
        

    return args.ip, args.port, args.client_id, address_info



class Client:
    def __init__(self, address_info=[], client_id=1):
        self.server_addresses = address_info
        self.client_id = client_id

        self.server_communicating_threads = []
        self.voter_thread = None

        self.logger = DebugLogger.get_logger('client.c-'+str(self.client_id))

    def start_client(self):
        self.logger.info("Starting client")
        duplication_handler_queue = queue.Queue()
        for server_addr in self.server_addresses:
            self.logger.info('starting client-server thread for server: %s', server_addr)
            ip, port = addr_to_ip_port(server_addr[1])
            server_id = server_addr[0]
            message_queue = queue.Queue()

            t = threading.Thread(target=self.run_client_server_handler, args=[ip,port,server_id, message_queue, duplication_handler_queue], daemon=True)
            self.server_communicating_threads.append((t, message_queue))
            t.run()
        
        self.logger.info("Started client-server threads; \t starting voter/duplication handler-thread")
        
        self.voter_thread = threading.Thread(target=self.run_duplication_handler, args=[duplication_handler_queue])
        self.voter_thread.run()

    
    def run_client_server_handler(self, ip, port, server_id, outgoing_message_queue, duplication_handler_queue):
        try:
            while True:
                self.logger.info("ip %s, port %d, id %d test", ip, port, server_id)
                # break

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                    client_socket.connect((ip, port))
                    request_number = 1
                    while True:

                        data = input("\nType in something to send!\n")
                        if data == '': data = "Hello World"
                        data += ' ' + str(request_number)

                        client_socket.sendall(data.encode('utf-8'))
                        response_data = client_socket.recv(1024)

                        if response_data == b'':
                            logger.warning("Nothing received from server; connection may be closed; let's wait a moment and retry")
                            time.sleep(10)
                            break; #break inner loop
                            #TODO; something much more intelligent here. Retry making the connection? Contact the replica manager? Contact IT? Cry?
                        logger.debug('Received [%s]', response_data.decode('utf-8'))

                        request_number += 1
        
        except KeyboardInterrupt:
            self.logger.critical('Keyboard interrupt in client; exiting')

    def run_duplication_handler(self, response_queue):
        '''
        Response queue will be a queue.Queue. It will receive 2 types of messages
        1) A message that says a request has been initiated for the servers. It will contain a request number
        2) A message that includes the response from one of the servers. It will contain a request number, the replica number, and the response data.

        Handled by Kiran
        '''
        pending_responses = {}

        while True:

            self.logger.info('here')
            break;


if __name__ == "__main__":
    ip, port, client_id, address_info = parse_args() #won't we need multiple ip, port pairs for each of the replicas? Can pull from config file, CLI, or even RM
    if len(address_info) == 0:
        address_info.append((1, ip+':'+port))
    # addr1 = '127.0.0.1:19618'
    # addr2 = '127.0.0.1:19619'
    # addr3 = '127.0.0.1:19620'
    client = Client(address_info=address_info, client_id=client_id)
    logger.info(client.server_addresses)
    client.start_client()