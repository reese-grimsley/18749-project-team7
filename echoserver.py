#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import DebugLogger
from lfd import MAGIC_MSG_LFD_REQUEST, MAGIC_MSG_LFD_RESPONSE

logger = DebugLogger.get_logger('echoserver')

LOCAL_HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
DEFAULT_PORT = 19618        # Port to listen on (non-privileged ports are > 1023)
MAGIC_MSG_LFD = b"lfd-heartbeat" # if we receive this message, we know it's heartbeat, so x shouldn't be


# host and port might change
HOST = LOCAL_HOST
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

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    state_x = 0
    while True: 
        conn, addr = s.accept()
        with conn:
            logger.info('Connected by %s', addr)
            logger.info("state_x is " + str(state_x))
            data = conn.recv(1024)
            if not data:
                #print("Waiting for something else")
                continue
            logger.info(data)
            # server received valid data
            # we now need to distinguish if this is from client or lfd
            if data.decode('utf-8') == MAGIC_MSG_LFD_REQUEST:
                # this is heartbeat so we won't increment
                logger.info("received heartbeat from LFD")
                response_data = MAGIC_MSG_LFD_RESPONSE
                
            else:
                state_x += 1
                logger.info("state_x after the increment is " + str(state_x))
                response_data = data
            
            conn.sendall(data)

