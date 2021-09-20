#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import DebugLogger
from _thread import *
logger = DebugLogger.get_logger('echoserver')

LOCAL_HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
DEFAULT_PORT = 19618        # Port to listen on (non-privileged ports are > 1023)
MAGIC_MSG_LFD = b"lfd-heartbeat" # if we receive this message, we know it's heartbeat, so x shouldn't be


# host and port might change
HOST = 'ece002.ece.local.cmu.edu'
PORT = DEFAULT_PORT

# After successfully processing 1 heartbeat, server stops responding


## This is the original server. I will be changing some structures
# with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#     s.bind((HOST, PORT))
#     s.listen()
#     state_x = 0
#     conn, addr = s.accept()
#     with conn:
#         logger.info('Connected by', addr)
#         logger.info("state_x is " + str(state_x))
#         while True:
#             data = conn.recv(1024)
#             if not data:
#                 #print("Waiting for something else")
#                 continue
#             logger.info(data)
#             # server received valid data
#             # we now need to distinguish if this is from client or lfd
#             if data == MAGIC_MSG_LFD:
#                 # this is heartbeat so we won't increment
#                 logger.info("received heartbeat from LFD")
                
#             else:
#                 state_x += 1
#                 logger.info("state_x after the increment is " + str(state_x))
            
#             conn.sendall(data)

'''while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        state_x = 0
        conn, addr = s.accept()
        with conn:
            logger.info('Connected by' + str(addr))
            logger.info("state_x is " + str(state_x))
            data = conn.recv(1024)
            if not data:
                #print("Waiting for something else")
                continue
            logger.info(data)
            # server received valid data
            # we now need to distinguish if this is from client or lfd
            if data == MAGIC_MSG_LFD:
                # this is heartbeat so we won't increment
                logger.info("received heartbeat from LFD")
                
            else:
                state_x += 1
                logger.info("state_x after the increment is " + str(state_x))
            
            conn.sendall(data)'''
            
def serve_client(conn, state_x, thread_num):
    while True:
        data = conn.recv(1024)
        if not data:
            #print("Waiting for something else")
            continue
        # server received valid data
        # we now need to distinguish if this is from client or lfd
        if data.decode('utf-8') == MAGIC_MSG_LFD:
            # this is heartbeat so we won't increment
            logger.info("Received heartbeat from LFD")
            response_data = MAGIC_MSG_LFD_RESPONSE
                
        else:
            logger.info('Received request from Client ' + str(thread_num) + ': ' + data.decode('utf-8'))
            state_x += 1
            logger.info('Current state_x value is: ' + str(state_x))
            '''logger.info("state_x after the increment is " + str(state_x))'''
            response_data = data
            logger.info('Sending reply message to Client ' + str(thread_num) + ': ' + response_data.decode('utf-8'))
        conn.sendall(response_data)

def run_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
    except socket.error as e:
        logger.error(str(e))
    
    s.listen()
    state_x = 0
    thread_num = 0
    while True: 
        conn, addr = s.accept()
        logger.info('Connected by %s', addr)
        logger.info("state_x is " + str(state_x))
        start_new_thread(serve_client, (conn, state_x, thread_num))
        logger.info('Thread ' + str(thread_num) + ' created')
        thread_num += 1
                
    s.close()

if __name__ == "__main__":
    run_server()