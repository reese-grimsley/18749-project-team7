#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import pickle
from re import L
import socket
import traceback, argparse
import DebugLogger, constants
from helper import basic_server, is_valid_ipv4
import messages

# host and port might change
HOST = constants.LOCAL_HOST # this needs to be outward facing (meaning localhost doesn't work)
PORT = constants.DEFAULT_APP_SERVER_PORT

# The all powerful global variable
state_x = 0

DebugLogger.set_console_level(30)
logger = DebugLogger.get_logger('app_server')

def parse_args():
    parser = argparse.ArgumentParser(description="Application Server")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The IP address this server should bind to -- defaults to 0.0.0.0, which will work across any local address', type=str)
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    return args.ip, args.port

def application_server_handler(client_socket, client_addr):
    global state_x
    connected = True
    try:
        while connected:
            data = client_socket.recv(constants.MAX_MSG_SIZE) # assume that we will send no message larger than this. Assume no timeout here.
            if data == b'':
                connected = False
            #TODO: assume the data is a byte/bytearray representation of a class in 'messages.py' that 
            msg = None
            try:
                msg = messages.deserialize(data)
            except pickle.UnpicklingError:
                logger.error("Unexpected Message format; could not deserialize") 
            except EOFError:
                logger.error("deserialization reached end of buffer without finishing; data was %d bytes, and we can only handle %d per recv call", len(data), constants.MAX_MSG_SIZE)
                #If we're hitting this error, then we need to consider sending a length in the first few bytes and looping here until we have received that entire length

            #dispatch message handler
            if isinstance(msg, messages.ClientRequestMessage):
                logger.critical('Received Message from client: %s', msg)
                echo(client_socket, msg, extra_data=str(state_x))
                state_x += 1
                logger.info("state_x is " + str(state_x))

            elif isinstance(msg, messages.LFDMessage) and msg.data == constants.MAGIC_MSG_LFD_REQUEST:
                logger.info("Received from LFD: %s", msg.data)
                respond_to_heartbeat(client_socket)

            else: 
                logger.info("Received unexpected message; type: [%s]", type(msg))
            
        
    finally: 
        client_socket.close()
        logger.info('Closed connection for client at (%s)', client_addr)

        
def echo(client_socket, msg:messages.ClientRequestMessage, extra_data=''):
    #Copy everything back into the response with virtually no changes
    if len(extra_data) > 0:
        response_data = msg.request_data + " : " + extra_data

    response_msg = messages.ClientResponseMessage(msg.client_id, msg.request_number, response_data, msg.server_id)    
    logger.critical('Response to client: %s', response_msg)
    response_bytes = response_msg.serialize()
    client_socket.sendall(response_bytes)


def respond_to_heartbeat(client_socket, response_data=constants.MAGIC_MSG_LFD_RESPONSE):
    lfd_response_msg = messages.LFDMessage(data=response_data)
    response_bytes = lfd_response_msg.serialize()
    client_socket.sendall(response_bytes)
    #Require ACK?


def application_server(ip, port):

    basic_server(application_server_handler, ip, port, logger=logger, reuse_addr=True, daemonic=True)

    logger.info("Echo Server Shutdown\n\n")

if __name__ == "__main__":
    ip, port = parse_args()
    application_server(ip, port)
    DebugLogger.setup_file_handler('./app_server_' + ip+':'+port+'.log', level=1)

    print('done')
