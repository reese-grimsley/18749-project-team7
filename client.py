#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

from os import kill
import socket
import argparse
import time
import threading, queue
import copy

import DebugLogger, constants
from helper import is_valid_ipv4, parse_addresses_file, addr_to_ip_port
from messages import ClientRequestMessage, ClientResponseMessage, KillThreadMessage

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

        self.server_communicating_threads = [] #will store tuples of form (thread, input-queue, server address)
        self.voter_thread = None # will be tuple of form (thread, input-queue)

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
            self.server_communicating_threads.append((t, message_queue, server_addr))
            t.run()
        
        self.logger.info("Started client-server threads; \t starting voter/duplication handler-thread")
        
        self.voter_thread = (threading.Thread(target=self.run_duplication_handler, args=[duplication_handler_queue]), duplication_handler_queue)
        self.voter_thread[0].run()

        try: 
            request_number = 1
            while True:

                data = input("\nType in something to send!\n")
                if data == '': data = "Hello World"

                self.logger.debug('Sending message #%d [%s] to the servers', request_number, data)

                for server_thread in self.server_communicating_threads:
                    self.logger.info()
                    server_addr = server_thread[2]
                    server_id = server_addr[0]
                    client_message = ClientRequestMessage(self.client_id, request_number, copy.copy(data), server_id)
                    server_thread[1].put(client_message)

                client_message_voter = ClientRequestMessage(self.client_id, request_number, copy.copy(data), constants.NULL_SERVER_ID)
                self.voter_thread[1].put(client_message_voter) #input


        except KeyboardInterrupt:
            self.logger.warning("Received KB interrupt in main client thread. Program should now end to kill server-connect and voter threads. ")
        except Exception as e:
            self.logger.error(e)
        finally: 
            # kill the other threads here, then exit. 
            self.logger.info("Attempting to shut down child threads of this client")
            for t in self.server_communicating_threads:
                killMsg = KillThreadMessage()
                t[1].put(killMsg)
            self.voter_thread[1].put(KillThreadMessage())

    
    def run_client_server_handler(self, ip, port, server_id, outgoing_message_queue, duplication_handler_queue):
        '''
        Handles a client-server connection. It will setup a socket for the given ip and port, and assume it's server ID matches the argument 

        All communication between this thread and others will be through queues; Each thread should dispatch based on a single input queue, and send results on the output queue 'duplication_handler_queue'

        Messages to be sent to the server should be of form ClientRequestMessage, and the server_id within matches the input argument to this function. 
        This thread will be killed when it sees the server shutdown the socket, a timeout on a message exchange with server finishes, or a KillThreadMessage arrives to the input queue

        Responses from the server will be of form ClientResponseMessage, and be send into the output queue that the voter thread listens to (duplication_handler_queue)

        This thread will exit when it receives a kill signal/message through its queue, when a server response times out, or when the socket is closed from the server side
        '''
        assert isinstance(outgoing_message_queue, queue.Queue) and isinstance(duplication_handler_queue, queue.Queue), "queue objects should be from queue.Queue"
        try:
            while True:
                self.logger.info("ip %s, port %d, id %d test", ip, port, server_id)
                # break
                # rad from outgoing message queue

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:

                    kill_signal_received = False

                    while not kill_signal_received:
                        is_connected = False
                        try: 
                            client_socket.connect((ip, port))
                            is_connected = True
                        except Exception as e:
                            self.logger.error('Failed to connect')
                            self.logger.error(e)
                            is_connected = False

                        while is_connected and not kill_signal_received:
                            # read from input queue with small timeout
                            new_request = outgoing_message_queue.get(block=True, timeout=constants.QUEUE_TIMEOUT)

                            if new_request is None: 
                                pass #probably just empty queue

                            elif isinstance(new_request, KillThreadMessage):
                                self.logger.info('Received thread kill signal -- exiting thread for server [%d]', server_id)
                                kill_signal_received = True

                            elif isinstance(new_request, ClientRequestMessage):
                                #do all the normal stuff. Should maybe be a function call
                                response = self.do_request_response(new_request, client_socket)
                                if response is None:
                                    is_connected = False # assuming if we get no response, that the connection is dead
                                duplication_handler_queue.put(response)

                            else:
                                self.logger.warning('Unable to determine what to do with incoming message [%s] in run_client_server_handler for id %d', new_request, server_id)

                        self.logger.info("Closing socket to server [%d]", server_id)
                        client_socket.close()
                        ##TODO: inform voter thread that this server is no longer being used? Implies giving it info that we were able to initiate the connection
                        time.sleep(1) #give some time before trying to reconnect

        
        except KeyboardInterrupt:
            self.logger.critical('Keyboard interrupt in client; exiting')
        self.logger.info('thread for connection to server %d exiting', server_id)

    def do_request_response(self, request_message:ClientRequestMessage, sock, server_id, timeout=constants.CLIENT_SERVER_TIMEOUT):
        '''
        Send the request message through the socket and return a ClientResponseMessage

        Returns None is nothing is received, including due to a timeout
        '''
        response_message = None

        try: 
            req_data = request_message.serialize()
            sock.sendall(req_data)

            #TODO: expect ACK?

            response_data = sock.recv(1024) #TODO: handled scnearios where we send more than 1024 bytes

            if response_data == b'':
                logger.warning("Nothing received from server; connection may be closed; let's wait a moment and retry")
                #TODO; something much more intelligent here. Retry making the connection? Contact the replica manager? Contact IT? Cry?

            else: 
                response_message = ClientResponseMessage.deserialize(response_data)
                self.logger.info('Received response for server #%d:  [%s]', server_id, response_message)
                #TODO: send ACK?

        except socket.timeout as to:
            self.logger.error('Socket timeout: %s', to)
            response_message = None

        except Exception as e:
            self.logger.error(e) 
            response_message = None

        return response_message

    def run_duplication_handler(self, response_queue):
        '''
        Response queue will be a queue.Queue. It will receive 2 types of messages
        1) A message that says a request has been initiated for the servers. It will contain a request number
        2) A message that includes the response from one of the servers. It will contain a request number, the replica number, and the response data.

        This thread needs to print messages that are avoided due to being duplication. Let's use the 'critical' logging level for this so that it always shows and we can suppress other output for demos

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