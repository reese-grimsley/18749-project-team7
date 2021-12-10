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

SOCKET_TIMEOUT_SECONDS = 0.5

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
backup_locations = [] # informationa bout where backups are located. Identify using IP. Also include replica ID. Populated from LFD/GFD. (ip, id) tuple
backup_thread_info = [] #tuples containing a thread and a queue and an address. Populated at runtime at backups connect, disconnect
primary_location = (None, None) #tuple with (ip, id). Assume it listens to default server port
primary_queue = queue.Queue()

#lock_variable
#lock_var1 = threading.Lock()
lock_var2 = threading.Lock()


DebugLogger.set_console_level(1)
logger = None 

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


def addr_present(addr_list, addr):

    for i, a in enumerate(addr_list):
        if a[0] == addr[0]:
            return True, i

    return False, -1

def clear_queue(q:queue.Queue):
    with q.mutex:
        q.queue.clear()


        
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


def passive_application_server(ip, port):
    basic_server(passive_server_handler, ip, port, logger=logger, reuse_addr=True, daemonic=True, extra_args=[])

    logger.info("Echo Server Shutdown\n\n")


def lfd_handler(sock, address):
    assert isinstance(sock, socket.socket), "not a socket; throw error in lfd handler"

    global server_id #read
    global is_primary #read and write
    global backup_locations #write
    global primary_location
    global backup_thread_info

    sock.settimeout(SOCKET_TIMEOUT_SECONDS)
    while True:
        try: 
            data = sock.recv(constants.MAX_MSG_SIZE) 
            msg = None
            try:
                msg = messages.deserialize(data)
            except pickle.UnpicklingError: 
                logger.warning("could not deserialize data: %d" % data)

            logger.debug(type(msg))

            if isinstance(msg, messages.LFDMessage):
                logger.info("Received from LFD: %s", msg.data)
                respond_to_heartbeat(sock)

            elif isinstance(msg, messages.PrimaryMessage):
                logger.info("Received PrimaryMessage (action %s) for updating connections: %s" % (msg.action, msg))
                if msg.action == constants.ADD_PRIMARY:
                    logger.info("LFD indicated the primary is changing: %s", msg)
                    primary_id = None
                    if len(msg.primary.keys()):
                        primary_id = list(msg.primary.keys())[0]
                        primary_id = int(primary_id[1:])

                    if primary_id == server_id:
                        logger.info("Add primary message received; this server (%d) is the new primary!" % server_id)
                        if is_primary is None:
                            logger.debug("Newly joined and assigned to be primary")
                            pass
                        elif is_primary == False:
                            logger.debug("were a backup, but promotied to primary")
                            
                            # we were promoted to primary. Switch to that. Kill anything listening to the old primary. 
                            primary_queue.put(messages.KillThreadMessage()) #listener will neeed to clear this queue so any remaining messages don't matter
                            # TODO: replay logs
                        else: 
                            logger.debug('renotified that we are the primary')
                            # we were primary and are now. Is there anything to do? 

                        is_primary = True
                        logger.debug("Backup_locations were: %s" % backup_locations)
                        backup_locations = []
                        for backup_id in msg.backup.keys():
                            backup_ip = msg.backup[backup_id]
                            backup_locations.append((backup_ip, backup_id))
                        logger.debug("Backup_locations are now: %s" % backup_locations)
                    elif primary_id is not None:
                        #the primary is someone else. We are the backup
                        primary_ip = msg.primary[primary_id]
                        if primary_ip == primary_location[0] and primary_id == primary_location[1]:
                            logger.info("Primary does not appear to have changed")
                        else:
                            logger.info('Primary has changed from %s to %s' % (primary_location, (primary_ip, primary_id)))
                            #need to cancel curent backup thread and spawn a new one
                            primary_queue.put(messages.KillThreadMessage()) #kill old thread. It should also clear this Q
                            time.sleep(3 * SOCKET_TIMEOUT_SECONDS) #let the other thread exit before spawning the new one

                            t = threading.Thread(target=primary_handler, args=[primary_ip, primary_queue], daemon=True)
                            t.start()
                            primary_location = (primary_ip, primary_id)


                elif msg.action == constants.ADD_BACKUP:
                    logger.info("Add backup %s\n" % msg)
                    if is_primary:
                        logger.info('backups passed are now: %s', msg.backup)
                        new_backups = 0
                        for backup_id in msg.backup.keys():
                            backup_ip = msg.backup[backup_id]
                            if not addr_present(backup_locations, (backup_ip, backup_id)): #I AM HERE
                                logger.debug("Adding known backup to list: (%s, %d)" % (backup_ip, backup_id))
                                backup_locations.append((backup_ip, backup_id))
                                new_backups +=1
                        logger.debug("Found %d new backups from GFD", new_backups)

                        #the new backups should now be allowed to connect to this replica. They will get their own thread.
                    elif is_primary is None and primary_location[0] is None and primary_location[1] is None:
                        #There is a new backup; it's me!
                        logger.info("New backup, and it is me!")
                        primary_id = list(msg.primary.keys())[0]
                        primary_id = int(primary_id[1:]) #expected format is SX, where X is the number of the server id

                        primary_ip = msg.primary[primary_id]

                        primary_location = (primary_ip, primary_id)
                        is_primary = False

                        t = threading.Thread(target=primary_handler, args=[primary_ip, primary_queue], daemon=True)
                        t.start()

                        
                    # if we are backup and the primary has not been assigned, assume this is 

                elif msg.action == constants.REMOVE_BACKUP:
                    logger.info("remove backup %s" % msg)
                    if is_primary:
                    
                        pass
                        for backup_id in msg.backup.keys():
                            backup_ip = msg.backup[backup_id]
                            #if the address of a backup is missing, that is one that was removed
                            if not addr_present(backup_locations, (backup_ip, backup_id)):
                                bti_to_remove = None
                                #we also need to find the queue this thread listens to so we can signal it should exit
                                for bti in backup_thread_info:
                                    if backup_ip == bti[2]:
                                        q = bti[1]
                                        q.put(messages.KillThreadMessage) #signal exit

                                        bti_to_remove = bti
                                        break
                                #remove 
                                backup_locations.remove((backup_ip, backup_id))
                                if bti_to_remove is not None:
                                    backup_thread_info.remove(bti_to_remove)

                    else: 
                        logger.debug("backup removed; I am backup as well and do not care. Hope it isn't me!: %s", msg.backup)
                        if not server_id in new_backups:
                            logger.info("Apparently I, the backup, am getting removed. PANIK!!!!")
                            exit(2)



                #if add backup and address is me, then spawn a thread for primary_handler
                #if add backup and address is not me and I am not primary: do nothing. Doesn't concern me.
                pass
            elif msg is not None:
                logger.warning("unexpected message: %s", msg)
            else:
                logger.warning("LFD Connection is dead")
                return

        except socket.timeout: pass
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
    assert isinstance(sock, socket.socket), "not a socket; throw error in backup handler within primary replica"
    assert is_valid_ipv4(address), "address of backup is not valid IP address: %s" % address 

    global state_x #read
    global state_y #read
    global state_z #read
    global checkpoint_operations_ongoing #write
    global client_operations_ongoing #read
    global is_primary #read
    global server_id #read
    global checkpoint_num #read

    sock.settimeout(SOCKET_TIMEOUT_SECONDS)
    while is_primary: #if we become the backup, this should automatically exit
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
                logger.info("Received message to make checkpoint. Quiesece until ready to send to address %s" % address)
                while client_operations_ongoing > 0: pass #dirty wait until client operations stop

                if queue_item[0] != state_x or queue_item[1] != state_y or queue_item[2] != state_z:
                    logger.log(25, 'inconsistency between state variables and information this checkpoint is suppsoed to contain')

                logger.info('Send checkpoint')
                checkpoint_msg = messages.CheckpointMessage(queue_item[0], queue_item[1], queue_item[2], server_id, checkpoint_num)
                sock.sendall(checkpoint_msg.serialize())

                checkpoint_operations_ongoing = max(checkpoint_operations_ongoing-1, 0)
                logger.info('Checkpoint sent; %d backups still checkpointing' % checkpoint_operations_ongoing)

        except queue.Empty: pass
        except socket.timeout: pass
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
    assert isinstance(sock, socket.socket), "not a socket; throw error in client handler"
    assert is_valid_ipv4(address), "address of client is not an IP address: %s" % address 

    global state_x #write
    global state_y #write
    global state_z #write
    global checkpoint_freq #read
    global checkpoint_msg_counter #write
    global checkpoint_operations_ongoing #read & write
    global client_operations_ongoing #read & write
    global is_primary #read

    num_failures = 0

    sock.settimeout(SOCKET_TIMEOUT_SECONDS)
    while is_primary:  # if we become a backup, we should close all these connections
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
                    for backup in backup_thread_info:
                        q = backup[1]
                        checkpoint = [state_x, state_y, state_z]
                        q.put(checkpoint)

                while checkpoint_operations_ongoing > 0: pass #dirty wait for checkpointint operations to end quiescence

                checkpoint_msg_counter = 0

        except socket.timeout: pass
        except TimeoutError: pass
        except OSError as oe:
            logger.error(oe)
            # sock.connect(address)

            num_failures += 1
            if num_failures > 30: break

            time.sleep(1)
        except Exception as e:
            logger.error(e)
            
def primary_handler(address, input_queue:queue.Queue):
    '''
    Only should run when this process is a backup replica

    Queue receives control messages. primarily to tell it to stop what it's doing (primary changes; we'll spawn a new thread to handle it)
    '''
    assert is_valid_ipv4(address), "address of client is not an IP address: %s" % address 

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(SOCKET_TIMEOUT_SECONDS)
        global is_primary #read
        global state_x #write
        global state_y #write
        global state_z #write
        global checkpoint_num # write

        sock.connect((address, constants.DEFAULT_APP_SERVER_PORT))

        while is_primary == False:
            msg = None
            queue_item = None
            try: 
                data = sock.recv(constants.MAX_MSG_SIZE) 
                msg = messages.deserialize(data)

                #should be receiving checkpoints or info for log replays
                if isinstance(msg, messages.CheckpointMessage):
                    logger.info("received checkpoint message from primary with ID %d", msg.primary_server_id)
                    state_x = msg.x
                    state_y = msg.y
                    state_z = msg.z
                    checkpoint_num = msg.checkpoint_num

                    #ack through socket?


                # elif isinstance(msg, messages.LoggingMessage)

            except socket.timeout: pass
            except TimeoutError: pass
            except OSError as oe:
                logger.error(oe)
                sock.connect((address, constants.DEFAULT_APP_SERVER_PORT))
                time.sleep(1)
            except Exception as e:
                logger.error(e)

            try:
                queue_item = input_queue.get_nowait()
                if isinstance(queue_item, messages.KillThreadMessage): break
            except queue.Empty: pass

    clear_queue(input_queue)
    logger.info('Exiting thread for connection to primary (from backup)')

def passive_server_handler(socket, address):
    '''
    Top level function to call on a newly created connection. Decide if this is the LFD, a backup or a client.

    Decide based on the address. To accept clients or backups, this must be the primary, of course.
    
    '''
    global is_primary #read
    global backup_locations #read
    global backup_thread_info #write

    address = address[0] #only take the IP. Who cares about a client port... randomly assigned
    if address == '0.0.0.0' or address == '127.0.0.1':
        logger.info('LFD connected')
        lfd_handler(socket, address)

    elif is_primary:
        logger.debug("received new nonlocal connection. Is it a client or backup..")
        index, is_backup_connection = addr_present(backup_locations, (address))
        if is_backup_connection:
            try:
                q = queue.Queue()
                backup_thread_info.append((threading.currentThread(), q, address))
                logger.info('backup at %s connected' % address)
                backup_handler(socket, address, q)
            except Exception as e: 
                logger.error(e)
                traceback.format_exc()

        else:
            logger.info("Client connected")
            client_handler(socket, address)

    else:
        logger.warn("Invalid connection arrived: %s" % address)


if __name__ == "__main__":
    # ip, port, server_id = parse_args()
    server_id = parse_args()
    ip = constants.CATCH_ALL_IP
    port = constants.DEFAULT_APP_SERVER_PORT

    logger = DebugLogger.get_logger('passive_app_server-S'+str(server_id))

    logger.log(1, "******\n\n\nIf you are seeing this message, change the logging level!!!\n\n******\n")
    DebugLogger.setup_file_handler('./passive_replication_server_' + ip+':'+str(port)+'.log', level=1)
    #TODO: use the server_id (part of LFD response, check against client requests)
    passive_application_server(ip, port)

    print('Exit')
