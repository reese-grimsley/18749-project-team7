#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket
import DebugLogger
logger = DebugLogger.get_logger('client')

HOST = 'ece002.ece.local.cmu.edu'  # The server's hostname or IP address
PORT = 19618        # The port used by the server

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:    
    s.connect((HOST, PORT))
except s.error as e:
    logger.error(str(e))
    
while True:
    request = input('Request Something: ')
    s.sendall(str.encode(request))
    response = s.recv(1024)
    logger.info('Received from server: ' + response.decode('utf-8'))

s.close()