#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import socket

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 19618        # Port to listen on (non-privileged ports are > 1023)

# FIXME: socket needs to be recycled (or spawned into a separate thread to handle that client in parallel on an offshoot of the main socket); current error: the program closes immediately after first use. See socket.create_server
# TODO: accept a connection for the local fault detector. Expect to receive a particular message from an incoming connection on 127.0.0.1 (localhost), and send back a particular message. Do not modify state. Recommended to reuse the same port as the other incoming connections from clients. 

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    state_x = 0
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        print("state_x is " + str(state_x))
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(data)
            # we now know it is valid data
            state_x += 1
            print("state_x after the increment is " + str(state_x))
            conn.sendall(data)
