#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import traceback, argparse
import DebugLogger, constants
from helper import basic_server, is_valid_ipv4

# host and port might change
HOST = constants.LOCAL_HOST # this needs to be outward facing (meaning localhost doesn't work)
PORT = constants.DEFAULT_APP_SERVER_PORT

# The all powerful global variable
state_x = 0

logger = DebugLogger.get_logger('app_server')

def parse_args():
    parser = argparse.ArgumentParser(description="Application Server")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The period between each heartbeat, in seconds', type=str)
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    return args.ip, args.port

def application_server_handler(client_socket, client_addr):
    global state_x
    try:
        data = client_socket.recv(1024)
        while data != b'':
            if constants.MAGIC_MSG_LFD_RESPONSE in data.decode('utf-8'):
                respond_to_heartbeat(client_socket)

            else:
                echo(client_socket, data)
                #TODO: data should be structured
                state_x += 1
                logger.info("state_x is " + str(state_x))

            data = client_socket.recv(1024)
    
    finally: 
        client_socket.close()
        logger.info('Closed connection for client at (%s)', client_addr)

        
def echo(client_socket, data):
    client_socket.sendall(data)


def respond_to_heartbeat(client_socket, response_msg=constants.MAGIC_MSG_LFD_RESPONSE):
    client_socket.sendall(response_msg.encode('utf-8'))


def application_server(ip, port):

    basic_server(application_server_handler, ip, port, logger=logger, reuse_addr=True, deamonic=True)

    logger.info("Echo Server Shutdown\n\n")

if __name__ == "__main__":
    ip, port = parse_args()
    application_server(ip, port)
    print('done')
