#!/usr/bin/env python3

# used https://realpython.com/python-sockets/

from os import kill
import pickle
import socket
import traceback, argparse
import DebugLogger, constants
from helper import is_valid_ipv4, basic_primary_server, basic_backup_server, basic_server
import messages
import threading
import queue
import time



# host and port might change
HOST = constants.LOCAL_HOST # this needs to be outward facing (meaning localhost doesn't work)
default_ports = [constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, constants.DEFAULT_APP_PRIMARY_SERVER_PORT2]

server_id = None

# The all powerful global variables
## we have far too many of these.
state_x = 0
state_y = 0
state_z = 0
am_i_quiet = False
checkpoint_operations_ongoing = 0
client_operations_ongoing = 0
checkpoint_num = 0
# send checkpoints to the backups for every checkpoint_freq messages received   from the clients
checkpoint_freq = 3

# triggers checkpoints when no of client messages crosses checkpoint_freq
checkpoint_msg_counter = 0

#
is_primary = None
backup_locations = [] # informationa bout where backups are located. Identify using IP. Also include replica ID. Populated from LFD/GFD
backup_thread_info = [] #tuples containing a thread and a queue and an address. Populated at runtime at backups connect, disconnect
primary_location = None
primary_queue = queue.Queue()

#lock_variable
#lock_var1 = threading.Lock()
lock_var2 = threading.Lock()


DebugLogger.set_console_level(1)
logger = DebugLogger.get_logger('passive_app_server')

def parse_args():
    parser = argparse.ArgumentParser(description="Passive Application Server")

    parser.add_argument('-p1', '--port1', metavar='p1', default=default_ports[0], help='The port that the server will be listening to and that this LFD will access', type=int)

    parser.add_argument('-p2', '--port2', metavar='p2', default=default_ports[1], help='The port that the server will be listening to and that this LFD will access', type=int)

    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The IP address this server should bind to -- defaults to 0.0.0.0, which will work across any local address', type=str)
    # parser.add_argument('-f', '--flag', metavar='f', default=1, help='Primary is flag = 0 and Backup is flag = 1', type=int)
    parser.add_argument('-s', '--server_id', metavar='sid', default=1, type=int, help='Identifier for the server')
    args = parser.parse_args()


    if args.port1 < 1024 or args.port1 > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.port2 < 1024 or args.port2 > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    # we don't need flags anymore
    # if args.flag != 0 and args.flag != 1:
    #     raise ValueError('Please enter a valid flag...')

    # return args.ip, args.port1, args.port2, args.server_id
    return args.server_id



# make a new handler called primary_server_backup_side_handler which is from a different thread...that should work when am_i_quiet is true

# create a checkpoint message as a new msg_type in messages.py

# keep a checkpoint_msg_count in this handler which can toggle am_i_quiet after crossing a threshold for response messages...also reset checkpoint_msg_count

#? Also should we keep receiving data from client socket (while quiescence is happening) and concatenating these messages into a local queue maintained by pas_server ? Or the client_socket handles this buffering implicitly ? ...talking about the line 69

def primary_backup_side_handler(backup_ip1, backup_port1, backup_ip2, backup_port2):
    global state_x
    global state_y
    global state_z
    global am_i_quiet
    global checkpoint_num

    # connect to backup 1
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket1:

        try: 
            # client_socket.settimeout(constants.CLIENT_SERVER_TIMEOUT)
            client_socket1.connect((backup_ip1, backup_port1))
            print('BACKUP IP 1 :'+ str(backup_ip1))
            print('BACKUP PORT 1:'+ str(backup_port1))

            logger.critical('Connected to Backup server 1!')

        except Exception:
            logger.warning('Failed to connect to Backup server 1 with ip: %d', backup_ip1)


        # connect to backup 2
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket2:

            try: 
                # client_socket.settimeout(constants.CLIENT_SERVER_TIMEOUT)
                client_socket2.connect((backup_ip2, backup_port2))
                print('BACKUP IP 2 :'+ str(backup_ip2))
                print('BACKUP PORT 2:'+ str(backup_port2))

                logger.critical('Connected to Backup server 2!')

            except Exception:
                logger.warning('Failed to connect to Backup server 2 with ip: %d', backup_ip2)



            # start executing send checkpoints logic
            connected = True
            try:
                while connected:
                    if(am_i_quiet):
                        
                        # for now constants.ECE_CLUSTER_ONE is primary...
                        #later this should be replaced with the primary_id...
                        checkpt_message = messages.CheckpointMessage(state_x, state_y, state_z, constants.ECE_CLUSTER_ONE, checkpoint_num)

                        checkpt_msg = checkpt_message.serialize()

                        try:
                            client_socket1.sendall(checkpt_msg)
                            client_socket2.sendall(checkpt_msg)
                        except Exception:
                            logger.info('...') # dummy condition
                            # not handled

                        logger.critical('Sending checkpoint' + str(checkpoint_num) + ' to Backup servers.....')

                        #check whether ack is received? possibility of deadlock if ack is included

                        checkpoint_num = (checkpoint_num + 1)

                        #with lock_var1:
                        #critical section
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
    global checkpoint_msg_counter
    global lock_var2

    connected = True
    try:
        while connected:
            #maybe here we should receive the messages and keep enqueuing into a queue regardless of quiescience state
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
                    with lock_var2:

                        # critical section #

                        #wait here until quiescience ends
                        while(am_i_quiet):
                            continue

                    
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

def addr_present(addr_list, addr):

    for i, a in enumerate(addr_list):
        if a[0] == addr[0]:
            return True, i

    return False, -1



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
                logger.critical("Received checkpoint: " + str(checkpoint_num) + " Checkpoint value of state_x, state_y, state_z is: " + str(state_x) + ', ' + str(state_y) + ', '+ str(state_z))


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

# def respond_to_heartbeat(client_socket, flag, response_data=constants.MAGIC_MSG_LFD_RESPONSE):
def respond_to_heartbeat(client_socket, response_data=constants.MAGIC_MSG_LFD_RESPONSE):
    lfd_response_msg = messages.LFDMessage(data=response_data)
    # lfd_response_msg.data += str(flag)
    response_bytes = lfd_response_msg.serialize()
    logger.critical('Received LFD Heartbeat')
    client_socket.sendall(response_bytes)
    #Require ACK?


# #def primary_server(ip, port1, port2):
# def primary_server():
#     # NOTE: we are using the default ip and ports...not from the user arguments
#     basic_primary_server(primary_backup_side_handler, primary_client_side_handler, logger=logger, ip=constants.CATCH_ALL_IP, backup_ip1=constants.ECE_CLUSTER_TWO, backup_ip2=constants.ECE_CLUSTER_THREE, backup_port1 = constants.DEFAULT_APP_BACKUP_SERVER_PORT, backup_port2 = constants.DEFAULT_APP_BACKUP_SERVER_PORT,  port2 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, reuse_addr=True, daemonic=True)

#     logger.info("Primary Server Shutdown\n\n")


# #def backup_server(ip, port):
# def backup_server():
#     # NOTE: we are using the default ip and ports specified in the handler functions...not from the user arguments
#     basic_backup_server(backup_server_handler, logger=logger, ip=constants.CATCH_ALL_IP, port=constants.DEFAULT_APP_BACKUP_SERVER_PORT, reuse_addr=True, daemonic=True)
    
#     logger.info("Backup Server Shutdown\n\n")

# def pas_server():

#     basic_backup_server(backup_server_handler, logger=logger, ip=constants.CATCH_ALL_IP, port=constants.DEFAULT_APP_BACKUP_SERVER_PORT, reuse_addr=True, daemonic=True)
#     basic_primary_server(primary_backup_side_handler, primary_client_side_handler, logger=logger, ip=constants.CATCH_ALL_IP, backup_ip1=constants.ECE_CLUSTER_TWO, backup_ip2=constants.ECE_CLUSTER_THREE, backup_port1 = constants.DEFAULT_APP_BACKUP_SERVER_PORT, backup_port2 = constants.DEFAULT_APP_BACKUP_SERVER_PORT,  port2 = constants.DEFAULT_APP_PRIMARY_SERVER_PORT1, reuse_addr=True, daemonic=True)
#     logger.info("Server shutdown\n")


def passive_application_server(ip, port):
    basic_server(passive_server_handler, ip, port, logger=logger, reuse_addr=True, daemonic=True, extra_args=[])

    logger.info("Echo Server Shutdown\n\n")


def lfd_handler(sock, address):
    assert isinstance(sock, socket) or isinstance(sock, socket.socket), "not a socket; throw error in lfd handler"

    global server_id

    sock.settimeout(.5)
    while True:
        try: 
            data = sock.recv(constants.MAX_MSG_SIZE) 
            msg = None
            try:
                msg = messages.deserialize(data)
            except pickle.UnpicklingError: logger.warning("could not deserialize data: %d" % data)

            if isinstance(msg, messages.LFDMessage) and msg.data == constants.MAGIC_MSG_LFD_REQUEST:
                logger.info("Received from LFD: %s", msg.data)
                respond_to_heartbeat(sock)

            elif isinstance(msg, messages.PrimaryMessage):
                if msg.action is constants.ADD_PRIMARY:
                    logger.info("LFD indicated the primary is changing: %s\nTODO: handle this", msg)
                    primary_id = None
                    if len(msg.primary.keys()):
                        primary_id = msg.primary.keys()[0]
                    if primary_id == server_id:
                        if is_primary is None:
                            # we just joined and are now the priary
                            pass
                        elif is_primary == False:
                            pass
                            # we were promoted to primary. Switch to that. Kill anything listening to the old primary. 
                        else: 
                            pass
                            # we were primary and are now. Is there anything to do?
                    #if this replica is becoming the primary:  
                        #kill thread that was managing connection to former primary 
                        #TODO if time allows: replay logs
                        #set is-primary = True. Then clients and backups should be able to connect now
                    #else, kill off the other primary_handler thread, and spawn a new one

                elif msg.action is constants.ADD_BACKUP:
                    logger.info("Add backup %s\nTODO: handle this" % msg)
                    #if we are a backup, ignore. If we are primary, add its info to the list of location

                elif msg.action is constants.REMOVE_BACKUP:
                    logger.info("Add backup %s\nTODO: handle this" % msg)
                    #if this is us, call existential_crisis.exe; we are dying
                    #if this is not us and we are a backup, ignore
                    #if this is not us and we are the primary, signal that thrad to die
                    # backup_thread_info.remove()


                #if kill backup and is_primary: send message to thread managing that backup to be killed. 
                    #decrement number of backups connected

                #if add backup and is_primary: Add to set of backup addresses, ensure size of array for thread info is large nough

                #if add backup and address is me, then spawn a thread for primary_handler
                #if add backup and address is not me and I am not primary: do nothing. Doesn't concern me.
                pass
            else:
                logger.warning("unexpected message: %s", msg)

        except TimeoutError: pass
        except OSError as oe:
            logger.error(oe)
            # sock.connect(address)
            time.sleep(1)
        except Exception as e:
            logger.error(e)
            
def backup_handler(sock, address, input_queue:queue.Queue):
    '''
    The primary manages its connection tot he backup here. It should continue doing so while it is the primary
    '''
    assert isinstance(sock, socket) or isinstance(sock, socket.socket), "not a socket; throw error in backup handler within primary replica"

    global state_x
    global state_y
    global state_z
    global checkpoint_freq
    global checkpoint_msg_counter
    global checkpoint_operations_ongoing
    global client_operations_ongoing
    global is_primary
    global server_id
    global checkpoint_num

    sock.settimeout(.5)
    while is_primary:
        msg = None
        queue_item = None
        try: 
            # data = sock.recv(constants.MAX_MSG_SIZE) 
            # msg = messages.deserialize(data)
            queue_item = input_queue.get(block=True, timeout=1)
            if isinstance(queue_item, messages.KillThreadMessage): break

            elif queue_item is not None:
                pass
                #assume it is an array containing [state_x, state_y, state_z]
                #wait until client_operations stop
                while client_operations_ongoing > 0: pass #dirty wait until client operations stop

                if queue_item[0] != state_x or queue_item[1] != state_y or queue_item[2] != state_z:
                    logger.log(25, 'inconsistency between state variables and information this checkpoint is suppsoed to contain')

                checkpoint_msg = messages.CheckpointMessage(queue_item[0], queue_item[1], queue_item[2], server_id, checkpoint_num)
                sock.sendall(checkpoint_msg.serialize())

                checkpoint_operations_ongoing = max(checkpoint_operations_ongoing-1, 0)
        except queue.Empty: pass
        except TimeoutError: pass
        except OSError as oe:
            logger.error(oe)
            # sock.connect(address)
            time.sleep(1)
        except Exception as e:
            logger.error(e)


    t = threading.currentThread()
    remove_el = None
    for el in backup_thread_info:
        if t == el[0] and input_queue == el[1] and address == el[2]:
            remove_el = el
            break
    backup_thread_info.remove(remove_el)
    if checkpoint_operations_ongoing > 0: checkpoint_operations_ongoing -= 1
            

def client_handler(sock, address):
    assert isinstance(sock, socket) or isinstance(sock, socket.socket), "not a socket; throw error in client handler"

    global state_x
    global state_y
    global state_z
    global checkpoint_freq
    global checkpoint_msg_counter
    global checkpoint_operations_ongoing
    global client_operations_ongoing
    global is_primary

    num_failures = 0

    sock.settimeout(.5)
    while is_primary:
        try: 
            #if we are not quiesced
            if checkpoint_operations_ongoing == 0:
                
                data = sock.recv(constants.MAX_MSG_SIZE) 
                msg = None
                try:
                    msg = messages.deserialize(data)
                except pickle.UnpicklingError:
                    logger.error("Unexpected Message format; could not deserialize") 

                if isinstance(msg, messages.ClientRequestMessage):
                    while checkpoint_operations_ongoing > 0: pass #dirty wait to wait for checkpoint operations to end if we were waiting when that started

                    client_operations_ongoing += 1 
                    logger.critical('Received Message from client: %s', msg)
                    client_id = msg.client_id

                    if client_id == 1:
                        echo(sock, msg, extra_data=str(state_x))
                        state_x += 1
                        logger.info("state_x is " + str(state_x))
                    elif client_id == 2:
                        echo(sock, msg, extra_data=str(state_y))
                        state_y += 1
                        logger.info("state_y is " + str(state_y))
                    elif client_id == 3:
                        echo(sock, msg, extra_data=str(state_z))
                        state_z += 1    
                        logger.info("state_z is " + str(state_z)) 
                    else: 
                        echo(sock, msg, extra_data=str(0))
                        logger.info('no state for this client (%d)' % client_id)

                    checkpoint_msg_counter = (checkpoint_msg_counter + 1)
                    client_operations_ongoing = max(client_operations_ongoing - 1, 0) 

            #checkpoint
            if checkpoint_msg_counter >= checkpoint_freq:
                logger.info("Checkpoint")
                while client_operations_ongoing > 0: pass #dirty wait to wait for client operations to start quiescence

                if checkpoint_operations_ongoing == 0: #we hope only one client thread will take this condition
                    checkpoint_num = (checkpoint_num + 1)
                    checkpoint_operations_ongoing = len(backup_thread_info) #signal checkpointing may start

                while checkpoint_operations_ongoing > 0: pass #dirty wait for checkpointint operations to end quiescence

                checkpoint_msg_counter = 0


        except TimeoutError: pass
        except OSError as oe:
            logger.error(oe)
            # sock.connect(address)

            num_failures += 1
            if num_failures > 30: break

            time.sleep(1)
        except Exception as e:
            logger.error(e)
            
def primary_handler(sock, address, input_queue:queue.Queue):
    '''
    Only should run when this process is a backup replica

    Queue receives control messages. primarily to tell it to stop what it's doing (primary changes; we'll spawn a new thread to handle it)
    '''
    assert isinstance(sock, socket) or isinstance(sock, socket.socket), "not a socket; throw error in primary handler within backup replica"
    sock.settimeout(.5)
    global is_primary
    while is_primary != True:
        msg = None
        queue_item = None
        try: 
            data = sock.recv(constants.MAX_MSG_SIZE) 
            msg = messages.deserialize(data)

            #should be receiving checkpoints or info for log replays
            if isinstance(msg, messages.CheckpointMessage):
                logger.info("received checkpoint message from primary")
                #lock
                # apply_checkpoint(msg)
                #unlock
                #signal
                pass

            # elif isinstance(msg, messages.LoggingMessage)

        except TimeoutError: pass
        except OSError as oe:
            logger.error(oe)
            # sock.connect(address)
            time.sleep(1)
        except Exception as e:
            logger.error(e)

        try:
            queue_item = input_queue.get_nowait()
            if isinstance(queue_item, messages.KillThreadMessage): return
        except queue.Empty: pass

def passive_server_handler(socket, address):
    '''
    Top level function to call on a newly created connection. Decide if this is the LFD, a backup or a client.

    Decide based on the address. To accept clients or backups, this must be the primary, of course.
    
    '''
    global is_primary
    global backup_locations
    global primary_location
    global backup_thread_info
    if address[0] == '0.0.0.0' or address[0] == '127.0.0.1':
        logger.info('LFD connected')
        lfd_handler(socket, address)

    elif is_primary:
        index, is_backup_connection = addr_present(backup_locations, address)
        if is_backup_connection:
            try:
                q = queue.Queue()
                backup_thread_info.append((threading.currentThread(), q, address))
                logger.info('backup at %s connected' % address)
                backup_handler(socket, address, q)
            except Exception as e: 
                logger.error(e)


        else:
            client_handler(socket, address)


if __name__ == "__main__":
    # ip, port, server_id = parse_args()
    server_id = parse_args()
    ip = constants.CATCH_ALL_IP
    port = constants.DEFAULT_APP_SERVER_PORT

    logger.log(1, "******\n\n\nIf you are seeing this message, change the logging level!!!\n\n******\n")
    DebugLogger.setup_file_handler('./passive_replication_server_' + ip+':'+str(port)+'.log', level=1)
    #TODO: use the server_id (part of LFD response, check against client requests)
    passive_application_server(ip, port)

    # #primary server
    # if flag == 0:
    #     primary_server()   
    #     #primary_server(ip, port1, port2)        

    # #backup servers
    # else:
    #     backup_server()
    #     #backup_server(ip, port1) 
    # pas_server()

    print('done')
