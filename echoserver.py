#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import traceback
import DebugLogger
from lfd import MAGIC_MSG_LFD_REQUEST, MAGIC_MSG_LFD_RESPONSE

logger = DebugLogger.get_logger('echoserver')

LOCAL_HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
DEFAULT_PORT = 19618        # Port to listen on (non-privileged ports are > 1023)

# host and port might change
HOST = LOCAL_HOST
PORT = DEFAULT_PORT

try: 
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1) # More portable to use socket.SO_REUSEADDR than SO_REUSEPORT.
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
                    
                    conn.sendall(response_data)
                    conn.close()
        finally:
            logger.debug('close main server socket')
            s.close()

except KeyboardInterrupt:
    logger.critical('Keyboard interrupt in echoserver; exiting')
except Exception as e:
    logger.error('Caught exception: %s' % e)
finally: 
    logger.info("Echo Server Off.\n\n")
