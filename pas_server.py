#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

import pickle
from re import L
import socket
import traceback, argparse
import DebugLogger, constants
from helper import is_valid_ipv4, basic_primary_server, basic_backup_server
import messages


# host and port might change
HOST = constants.LOCAL_HOST # this needs to be outward facing (meaning localhost doesn't work)
default_ports = [constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, constants.DEFAULT_APP_PRIMARY_SERVER_PORT2]

# The all powerful global variable
# no more global variable
state_x = 0
state_y = 0
state_z = 0
am_i_quiet = False
checkpoint_num = 0
# send checkpoints to the backups for every checkpoint_freq messages received   from the clients
checkpoint_freq = 3


DebugLogger.set_console_level(30)
logger = DebugLogger.get_logger('passive_app_server')

def parse_args():
    parser = argparse.ArgumentParser(description="Passive Application Server")

    parser.add_argument('-p1', '--port1', metavar='p1', default=default_ports[0], help='The port that the server will be listening to and that this LFD will access', type=int)

    parser.add_argument('-p2', '--port2', metavar='p2', default=default_ports[1], help='The port that the server will be listening to and that this LFD will access', type=int)

    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The IP address this server should bind to -- defaults to 0.0.0.0, which will work across any local address', type=str)
    parser.add_argument('-f', '--flag', metavar='f', default=1, help='Primary is flag = 0 and Backup is flag = 1', type=int)
    parser.add_argument('-s', '--server_id', metavar='sid', default=1, type=int, help='Identifier for the server')
    args = parser.parse_args()


    if args.port1 < 1024 or args.port1 > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.port2 < 1024 or args.port2 > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    if args.flag != 0 and args.flag != 1:
        raise ValueError('Please enter a valid flag...')

   


    return args.ip, args.port1, args.port2, args.flag, args.server_id



# make a new handler called primary_server_backup_side_handler which is from a different thread...that should work when am_i_quiet is true

# create a checkpoint message as a new msg_type in messages.py

# keep a checkpoint_msg_count in this handler which can toggle am_i_quiet after crossing a threshold for response messages...also reset checkpoint_msg_count

#? Also should we keep receiving data from client socket (while quiescence is happening) and concatenating these messages into a local queue maintained by pas_server ? Or the client_socket handles this buffering implicitly ? ...talking about the line 69

def primary_backup_side_handler(client_socket):
    global state_x
    global state_y
    global state_z
    global am_i_quiet
    global checkpoint_num

    connected = True
    try:
        while connected:
            if(am_i_quiet):
                checkpoint_num = (checkpoint_num + 1)
                
                # for now constants.ECE_CLUSTER_ONE is primary...
                #later this should be replaced with the primary_id...
                checkpt_message = messages.CheckpointMessage(state_x, state_y, state_z, constants.ECE_CLUSTER_ONE, checkpoint_num)

                checkpt_msg = checkpt_message.serialize()

                client_socket.sendall(checkpt_msg)

                #check whether ack is received? possibility of deadlock if ack is included

                am_i_quiet = False

        
    finally: 
        client_socket.close()
        # for now constants.ECE_CLUSTER_ONE is primary...
        #later this should be replaced with the primary_id...
        logger.info('Closed connection for client at (%s)', constants.ECE_CLUSTER_ONE)



def primary_client_side_handler(client_socket, client_addr):
    global state_x
    global state_y
    global state_z
    global am_i_quiet
    global checkpoint_freq

    #local checkpoint freq variable
    checkpoint_msg_counter = 0



    connected = True
    try:
        while connected:
            if(not am_i_quiet):

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
                    client_id = msg.client_id

                    if client_id == 1:
                        state_x += 1
                        logger.info("state_x is " + str(state_x))
                    elif client_id == 2:
                        state_y += 1
                        logger.info("state_y is " + str(state_y))
                    elif client_id == 3:
                        state_z += 1    
                        logger.info("state_z is " + str(state_z)) 

                    checkpoint_msg_counter = (checkpoint_msg_counter + 1)   
                    if checkpoint_msg_counter == checkpoint_freq:
                        checkpoint_msg_counter = 0
                        # go to quiescience
                        am_i_quiet = True 
                    

                elif isinstance(msg, messages.LFDMessage) and msg.data == constants.MAGIC_MSG_LFD_REQUEST:
                    logger.info("Received from LFD: %s", msg.data)
                    respond_to_heartbeat(client_socket, 0)

                else: 
                    logger.info("Received unexpected message; type: [%s]", type(msg))
        
        
    finally: 
        client_socket.close()
        logger.info('Closed connection for client at (%s)', client_addr)





# make this as backup_server_LFD_handler which will only get LFD messages and will respond to it (as backups dont respond to client messages)

# make a new handler called backup_server_primary_side_handler which is from a different thread...that should work on receiving checkpoints from the primary server and update local state variables x,y,z based on checkpoint messages

# toggle the am_i_quiet variable back to false after serving checkpoints

def backup_server_handler(client_socket, client_addr):
    global state_x
    global state_y
    global state_z
    global checkpoint_num

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
            if isinstance(msg, messages.CheckpointMessage):
                logger.critical('Received Checkpoint Message from Primary Server: %s', msg.primary_server_id)

                # TODO: enable this if we are implementing ack
                #echo(client_socket, msg)

                state_x = msg.x
                state_y = msg.y
                state_z = msg.z
                checkpoint_num = msg.checkpoint_num
                logger.info("Received checkpoint: " + str(checkpoint_num) + "checkpoint value of state_x, state_y, state_z is: " + str(state_x) + str(state_y) + str(state_z))


            elif isinstance(msg, messages.LFDMessage) and msg.data == constants.MAGIC_MSG_LFD_REQUEST:
                logger.info("Received from LFD: %s", msg.data)
                respond_to_heartbeat(client_socket, 1)

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


def respond_to_heartbeat(client_socket, flag, response_data=constants.MAGIC_MSG_LFD_RESPONSE):
    lfd_response_msg = messages.LFDMessage(data=response_data)
    lfd_response_msg.data += str(flag)
    response_bytes = lfd_response_msg.serialize()
    logger.critical('Received LFD Heartbeat')
    client_socket.sendall(response_bytes)
    #Require ACK?


def primary_server(ip, port1, port2):
    # NOTE: we are using the default ip and ports...not from the user arguments
    basic_primary_server(primary_backup_side_handler, primary_client_side_handler, logger=logger, ip=ip, backup_ip1=constants.ECE_CLUSTER_TWO, backup_ip2=constants.ECE_CLUSTER_THREE, backup_port1 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, backup_port2 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1,  port2 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, reuse_addr=True, daemonic=True)
    #logger=helper_logger, ip=constants.CATCH_ALL_IP, backup_ip1 = constants.ECE_CLUSTER_TWO, backup_ip2 = constants.ECE_CLUSTER_THREE, backup_port1 = constants.DEFAULT_APP_BACKUP_SERVER_PORT, backup_port2 = constants.DEFAULT_APP_BACKUP_SERVER_PORT,  port2 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, reuse_addr=True, daemonic=True
    logger.info("Primary Server Shutdown\n\n")



def backup_server(ip, port):
    # NOTE: we are using the default ip and ports specified in the handler functions...not from the user arguments
    basic_backup_server(backup_server_handler, logger=logger, ip=ip, port=port, reuse_addr=True, daemonic=True)
   
    logger.info("Backup Server Shutdown\n\n")


'''
def passive_application_server(ip, port, flag):
    basic_server(passive_application_server_handler, ip, port, logger=logger, reuse_addr=True, daemonic=True, extra_args=[flag])

    logger.info("Echo Server Shutdown\n\n")
'''


if __name__ == "__main__":
    ip, port1, port2, flag, server_id = parse_args()
    DebugLogger.setup_file_handler('./passive_replication_server_' + ip+':'+str(port1)+str(port2)+str(flag)+'.log', level=1)
    #TODO: use the server_id (part of LFD response, check against client requests)
    #passive_application_server(ip, port, flag)

    #primary server
    if flag == 0:
        primary_server(ip, port1, port2)        

    #backup servers
    else:
        backup_server(ip, port1) 


    print('done')
