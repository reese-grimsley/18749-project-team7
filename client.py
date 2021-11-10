#!/usr/bin/env python3


from os import kill
import socket
import argparse
import time, threading, queue
import copy, traceback

import DebugLogger, constants
from helper import is_valid_ipv4, parse_addresses_file, addr_to_ip_port
import messages


DebugLogger.set_console_level(1)
logger = DebugLogger.get_logger('client')

kill_switch_engaged = False

def parse_args():
    parser = argparse.ArgumentParser(description="Application Server")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.CATCH_ALL_IP, help='The IPv4 address of the application server', type=str)
    parser.add_argument('-a', '--addresses_path', metavar='a', default='./server_addresses.txt', help='A path to a file containing a list of IP addresses in IPv4 format with ports')
    parser.add_argument('-c', '--client_id', metavar='c', default=1, help="A client identifier (an integer, for simplicity)", type=int) #could also just be a string
    parser.add_argument('-gi', '--gfd_ip', metavar='gi', default=constants.ECE_CLUSTER_FOUR, help="The address of GFD", type=str)
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)
    if args.addresses_path:
        address_info = parse_addresses_file(args.addresses_path)
    else:
        address_info = None
        

    return args.ip, args.port, args.client_id, address_info, args.gfd_ip

class ClientServerConnectionMessage():
    '''
    Inform the duplication handler about the status of a connected server. Message is meant to be passed through a queue, and NOT through a network (hence the lack of serialize/deserialize)
    :param server_id: interger describing which server replica we are referring to
    :param is_connected: boolean for whether the replica can be connected to or not
    :param is_primary: boolean describing is the replica of interest is the primary. Must be None for active-replication systems. NOT SAFE TO CHECK only for falseness; should check falseness AND None-ness (``if not is_primary and is_primary is not None``)
    '''
    def __init__(self, server_id, is_connected, is_primary=None, client_input_queue = None):
        self.server_id = server_id
        self.is_connected = is_connected
        self.is_primary = is_primary
        self.client_input_queue = client_input_queue

    def __repr__(self):
        return '{ClientConnectedMessage: S%d connected:%s}' % (self.server_id, self.is_connected)
    def __eq__(self, other):
        return isinstance(other, ClientServerConnectionMessage) and id(other) == id(self)
    def __lt__(self, other):
        if not isinstance(other, ClientServerConnectionMessage): return False

class Client:
    def __init__(self, address_info=[], client_id=1, gfd_ip=""):
        self.server_addresses = address_info
        self.client_id = client_id
        self.gfd_ip = gfd_ip

        self.server_communicating_threads = [] #will store tuples of form (thread, input-queue, server address)
        self.duplication_handler = None # will be tuple of form (thread, input-queue)
        self.gfd_thread = None

        self.logger = DebugLogger.get_logger('client.c-'+str(self.client_id))

    def start_client(self):
        self.logger.info("Starting client")
        duplication_handler_queue = queue.PriorityQueue()
        request_distributor_queue = queue.PriorityQueue()
        

        # for server_addr in self.server_addresses:
        #     self.logger.info('starting client-server thread for server: %s', server_addr)
        #     ip, port = addr_to_ip_port(server_addr[1])
        #     server_id = server_addr[0]
        #     message_queue = queue.PriorityQueue()

        #     t = threading.Thread(target=self.run_client_server_handler, args=[ip,port,server_id, message_queue, duplication_handler_queue], daemon=True)
        #     self.server_communicating_threads.append((t, message_queue, server_addr))
        #     t.start()
        
        self.logger.info("Starting threads from main")

        self.gfd_thread = (threading.Thread(target=self.gfd_communicator, args=[self.client_id, self.gfd_ip, request_distributor_queue, duplication_handler_queue], daemon=False) )
        self.gfd_thread.start() #no input queue to GFD thread. It'll be busy listening to socket

        self.request_distributor_thread = (threading.Thread(target=self.run_request_distributor, args=[request_distributor_queue, duplication_handler_queue], daemon=False), request_distributor_queue)
        self.request_distributor_thread[0].start()
        
        self.duplication_handler = (threading.Thread(target=self.run_duplication_handler, args=[duplication_handler_queue], daemon=False), duplication_handler_queue)
        self.duplication_handler[0].start()


        # main run loop; this is user facing
        try: 
            request_number = 1
            while True:

                data = input("\nType in something to send!\n")
                if data == '': data = "Hello World"

                # Have some data to send now; let's create ClientRequestMessages and send to all the threads through queues
                self.logger.info('Sending message #%d [%s] to the servers', request_number, data)

                client_request_message = messages.ClientRequestMessage(self.client_id, request_number, copy.copy(data), constants.NULL_SERVER_ID)
                self.request_distributor_thread[1].put((constants.MSG_PRIORITY_DATA, client_request_message))

                request_number += 1
                time.sleep(.25) #just a quick delay


        except KeyboardInterrupt:
            self.logger.warning("Received KB interrupt in main client thread. Program should now end to kill server-connect and voter threads. ")
        except Exception:
            self.logger.error(traceback.format_exc())
        finally: 
            # kill the other threads here, then exit. 
            self.logger.info("Attempting to shut down child threads of this client")
            for t in self.server_communicating_threads:
                t[1].put((constants.MSG_PRIORITY_MGMT, messages.KillThreadMessage()))
            self.duplication_handler[1].put((constants.MSG_PRIORITY_MGMT, messages.KillThreadMessage()))
            self.request_distributor_thread[1].put((constants.MSG_PRIORITY_MGMT, messages.KillThreadMessage()))
            global kill_switch_engaged
            kill_switch_engaged = True

    
    def run_request_distributor(self, input_queue, duplication_handler_queue):
        '''
        Take in requests directly from the main user-input thread and send them to all threads managing a connection to a replica and the duplication handler. Keeps track of which server replicas are active and which is the primary
        '''

        assert isinstance(input_queue, queue.PriorityQueue) and isinstance(duplication_handler_queue, queue.PriorityQueue), "queue objects should be from queue.PriorityQueue"

        self.logger.info("Started request-distributor thread")

        connection_handler_info = [] #entries will be tuples of form (server_id, queue)
        primary_server_id = constants.NULL_SERVER_ID 

        try: 
            kill_signal_received = False
            while not kill_signal_received:
                msg = None
                try:
                    msg = input_queue.get(block=True, timeout=constants.QUEUE_TIMEOUT)[1] #entries are tuples of form (priority, msg)
                    self.logger.debug('Request distributor received msg: [%s]', msg)

                except queue.Empty:
                    pass #nothing to do. 

                if msg is None: 
                    pass #probably just empty queue and timed out

                elif isinstance(msg, messages.KillThreadMessage):
                    kill_signal_received = True

                elif isinstance(msg, ClientServerConnectionMessage):
                    self.logger.info("Received Client Connection mgmt Message; %s", msg)
                    connection_msg = msg
                    if connection_msg.is_primary:
                        primary_server_id = msg.server_id

                    if connection_msg.is_connected:
                        if connection_msg.server_id >= 0 and isinstance(connection_msg.client_input_queue, queue.PriorityQueue): #TODO replace with priority queue
                            connection_handler_info.append((connection_msg.server_id, connection_msg.client_input_queue))
                    else:
                        chi_to_remove = None
                        for chi in connection_handler_info:
                            if chi[1] == connection_msg.server_id:
                                chi_to_remove = chi
                                if primary_server_id == connection_msg.server_id:
                                    primary_server_id = constants.NULL_SERVER_ID
                                    self.logger.warning("Primary killed without replacement!")
                        if chi_to_remove is not None:
                            connection_handler_info.remove(chi_to_remove)

                elif isinstance(msg, messages.ClientRequestMessage):
                    self.logger.info("Received Client Request Message; %s", msg)

                    for handler_info in connection_handler_info:
                        server_id = handler_info[0]
                        handler_queue = handler_info[1]
                        self.logger.info("duplicating request for replica %d", server_id)

                        request_msg_copy = copy.deepcopy(msg)
                        request_msg_copy.server_id = server_id
                        handler_queue.put((constants.MSG_PRIORITY_DATA, request_msg_copy))

                    initiate_duplicator_msg = copy.deepcopy(msg)
                    initiate_duplicator_msg.server_id = constants.NULL_SERVER_ID #ID doesn't matter here
                    duplication_handler_queue.put((constants.MSG_PRIORITY_DATA, initiate_duplicator_msg))
        except Exception as e:
            traceback.print_last()
            self.logger.error(e)

        self.logger.info("Exiting thread for request distribution")



    def run_client_server_handler(self, ip, port, server_id, input_queue, duplication_handler_queue, is_primary=None):
        '''
        Handles a client-server connection. It will setup a socket for the given ip and port, and assume it's server ID matches the argument 

        All communication between this thread and others will be through queues; Each thread should dispatch based on a single input queue, and send results on the output queue 'duplication_handler_queue'

        Messages to be sent to the server should be of form ClientRequestMessage, and the server_id within matches the input argument to this function. 
        This thread will be killed when it sees the server shutdown the socket, a timeout on a message exchange with server finishes, or a KillThreadMessage arrives to the input queue

        Responses from the server will be of form ClientResponseMessage, and be send into the output queue that the voter thread listens to (duplication_handler_queue)

        This thread will exit when it receives a kill signal/message through its queue, when a server response times out, or when the socket is closed from the server side
        '''
        assert isinstance(input_queue, queue.PriorityQueue) and isinstance(duplication_handler_queue, queue.PriorityQueue), "queue objects should be from queue.PriorityQueue"
        try:
            self.logger.info("Client-server handler for ip %s, port %d, S_id %d", ip, port, server_id)


            kill_signal_received = False
            while not kill_signal_received:
                is_connected = False
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:

                    try: 
                        # client_socket.settimeout(constants.CLIENT_SERVER_TIMEOUT)
                        client_socket.connect((ip, port))

                        is_connected = True
                        self.logger.info('Connected!')

                    except Exception:
                        self.logger.warning('Failed to connect to S%d', server_id)
                        # print(traceback.format_exc())
                        is_connected = False
                        #peek into the queue to see if there's actually a kill-thread message waiting for us
                        queue_snapshot = list(input_queue.queue)
                        for q_item in queue_snapshot:
                            if isinstance(q_item, messages.KillThreadMessage):
                                kill_signal_received = True

                    while is_connected and not kill_signal_received:
                        # read from input queue with small timeout
                        new_request = None
                        try:
                            new_request = input_queue.get(block=True, timeout=constants.QUEUE_TIMEOUT)[1] #entries are tuples of form (priority, msg)
                        except queue.Empty:
                            pass #nothing to do. 

                        if new_request is None: 
                            pass #probably just empty queue and timed out

                        elif isinstance(new_request, messages.KillThreadMessage):
                            self.logger.info('Received thread kill signal -- exiting thread for server [%d]', server_id)
                            kill_signal_received = True

                        elif isinstance(new_request, messages.ClientRequestMessage):
                            #do all the normal stuff. Should maybe be a function call
                            try: 
                                if is_primary or is_primary is None:
                                    response = self.do_request_response(new_request, client_socket)
                                    if response is None:
                                        is_connected = False # assuming if we get no response, that the connection is dead
                                        # if the response didn't come and we kill the connection, we should still send something to the duplication handler so 
                                    else:
                                        duplication_handler_queue.put((constants.MSG_PRIORITY_DATA, response))
                                else:
                                    self.logger.debug("received clientRequestMessage as a non-primary connection. Donut send.")

                            except TimeoutError as to:
                                self.logger.info('client timed out. Kill connection and try again')
                                is_connected = False

                        #TODO: add ClientServerConnectionMessage to update who is the primary

                        else:
                            self.logger.warning('Unable to determine what to do with incoming message [%s] in run_client_server_handler for id %d', new_request, server_id)

                    self.logger.info("Closing socket to server [%d]", server_id)
                    client_socket.close()
                    # connected_message = ClientConnectedMessage(server_id, False)
                    # duplication_handler_queue.put(constants.MSG_PRIORITY_CONTROL, connected_message))
                    time.sleep(1) # short delay before trying to reconnect
        except Exception:
            self.logger.error(traceback.format_exc())

        self.logger.info('Exiting thread for connection to server %d', server_id)

    def do_request_response(self, request_message:messages.ClientRequestMessage, sock):
        '''
        Send the request message through the socket and return a ClientResponseMessage

        Returns None is nothing is received, including due to a timeout
        '''
        response_message = None

        try: 
            req_data = request_message.serialize()
            self.logger.critical('Send message to server: %s', request_message)
            sock.sendall(req_data)

            #TODO: expect ACK? Assume no ACKs for now

            response_data = sock.recv(constants.MAX_MSG_SIZE) #TODO: handle scnearios where we send more than 1024 bytes

            if response_data == b'':
                logger.warning("Nothing received from server; connection may be closed; let's wait a moment and retry")
                #TODO; something much more intelligent here. Retry making the connection? Contact the replica manager? Contact IT? Cry?

            else: 
                response_message = messages.deserialize(response_data)
                self.logger.critical('Receive message from server: %s', response_message)
                #TODO: send ACK? Assume no ACKs for now

        except socket.timeout as to:
            self.logger.error('Socket timeout: %s', to)
            raise to
            # response_message = None

        except Exception as e:
            self.logger.error(e) 
            raise e
            # response_message = None

        return response_message

    def gfd_communicator(self, client_id, gfd_ip, request_distributor_queue, duplication_handler_queue):
        global kill_switch_engaged
        port = constants.DEFAULT_GFD_PORT

        client_server_handlers = [] #reference to (thread, input_queue, server_id)
        primary_server_id = constants.NULL_SERVER_ID

        while not kill_switch_engaged:
            is_connected = False
            self.logger.info('GFD:: start')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                try:
                    # client_socket.settimeout(constants.CLIENT_SERVER_TIMEOUT)
                    client_socket.connect((gfd_ip, port))
                    client_socket.settimeout(constants.CLIENT_SERVER_TIMEOUT) #timeout should be used to 

                    is_connected = True
                    self.logger.info('GFD:: Connected at IP:Port  %s:%d', gfd_ip, port)
                except Exception:
                    self.logger.warning('GFD:: Failed to connect to GFD %s', gfd_ip)
                    # print(traceback.format_exc())
                    is_connected = False
                    time.sleep(2)
                while is_connected and not kill_switch_engaged:
                    try:
                        #data = client_socket.recv(1024).decode(encoding='utf-8')
                        gfd_msg_bytes = client_socket.recv(constants.MAX_MSG_SIZE)
                        gfd_msg = messages.deserialize(gfd_msg_bytes)
                        
                        if not gfd_msg:
                            self.logger.info("Connection to GFD appears to have died")
                            is_connected = False
                            #continue
                            
                        if gfd_msg is None:
                            continue
                            
                        logger.info('Received from GFD: [%s]', gfd_msg.data)
                        data = gfd_msg.data
                        
                        if constants.MAGIC_MSG_GFD_REQUEST in gfd_msg.data:
                            response = constants.MAGIC_MSG_RESPONSE_FROM_CLIENT + str(client_id)
                            client_msg = messages.LFDMessage(response)
                            client_msg_bytes = client_msg.serialize()
                            client_socket.sendall(client_msg_bytes)
                            self.logger.info(response)
                            
                        elif constants.GFD_ACTION_DEAD is gfd_msg.action:
                            self.logger.info(data)
                            ##send kill signal to thread and ClientConnectedMessage update to request distributor (handler itself should send to duplication handler)
                            server_id = int(gfd_msg.sid) #TODO: retrieve from message

                            # Tell everyone that that the replica is no longer active; no longer send requests nor expect responses
                            client_disconnected_msg = ClientServerConnectionMessage(server_id, False)
                            request_distributor_queue.put((constants.MSG_PRIORITY_CONTROL, copy.copy(client_disconnected_msg))) #copy to avoid any changes that thread could make
                            duplication_handler_queue.put((constants.MSG_PRIORITY_CONTROL, copy.copy(client_disconnected_msg)))
                            #kil handler thread
                            c_to_remove = None
                            for c in client_server_handlers: #elements of form (client_thread, client_input_queue, server_id)
                                if c[2] == server_id:
                                    c[1].put((constants.MSG_PRIORITY_MGMT, messages.KillThreadMessage()))
                                    c_to_remove = c
                                    if primary_server_id == server_id:
                                        self.logger.error("Primary killed without replacement")
                                        primary_server_id = constants.NULL_SERVER_ID
                            if c_to_remove is not None:
                                self.logger.info("Removing handler thread for replica %d: %s", server_id, c_to_remove)
                                client_server_handlers.remove(c_to_remove)
                            else: self.logger.warn("Didn't find replica [%d] to remove??", server_id)

                        elif constants.GFD_ACTION_NEW is gfd_msg.action: 
                            self.logger.info(gfd_msg.data)
                            #TODO: retrieve IP, port, ID, and primary status from the message
                            #replica_ip = "127.0.0.1"
                            #replica_port = constants.DEFAULT_APP_SERVER_PORT
                            #server_id = 1
                            #is_primary = True
                            replica_ip = gfd_msg.server_ip
                            replica_port = constants.DEFAULT_APP_SERVER_PORT
                            server_id = int(gfd_msg.sid)
                            is_primary = gfd_msg.is_primary

                            for csh in client_server_handlers:
                                if csh[2] == server_id:
                                    self.logger.warning("Received message to add new server [%d] for one we're already connected to!", server_id)
                                    csh[1].put((constants.MSG_PRIORITY_MGMT ,messages.KillThreadMessage()))
                                    client_server_handlers.remove(csh) #this may be unsafe within an iterator
                                    break

                            if is_primary and server_id != primary_server_id:
                                primary_server_id = server_id

                            client_input_queue = queue.PriorityQueue()
                            client_thread = threading.Thread(target=self.run_client_server_handler, args=[replica_ip, replica_port, server_id, client_input_queue, duplication_handler_queue, is_primary], daemon=True)
                            client_thread.start()
                            client_server_handlers.append((client_thread, client_input_queue, server_id))

                            #inform other threads that a new replica is present
                            client_connected_msg = ClientServerConnectionMessage(server_id, True, is_primary=is_primary, client_input_queue=client_input_queue)
                            request_distributor_queue.put((constants.MSG_PRIORITY_CONTROL, copy.copy(client_connected_msg))) #copy to avoid any changes that thread could make
                            duplication_handler_queue.put((constants.MSG_PRIORITY_CONTROL, copy.copy(client_connected_msg)))

                        else:
                            self.logger.warning("GFD:: Received unexpected data: %s", data)
                        #TODO: add extra case for primary update
                    except socket.timeout:
                        pass
                    except (OSError, socket.error) as e:
                        is_connected = False

                    if kill_switch_engaged:
                        self.logger.warning("GFD: gfd_communicator received kill signal; exiting")

        for csh in client_server_handlers:
            self.logger.info("Kill client: %s", csh)
            csh[1].put((constants.MSG_PRIORITY_MGMT ,messages.KillThreadMessage()))
        self.logger.info("Exiting thread for GFD")

    def run_duplication_handler(self, response_queue:queue.PriorityQueue):
        '''
        Response queue will be a queue.PriorityQueue. It will receive 4 types of messages
        1) A message that says a request has been initiated for the servers. It will contain a request number. Instance of messages.ClientRequestMessage
        2) A message that includes the response from one of the servers. It will contain a request number, the replica number, and the response data. Instance of messages.ClientResponseMessage
        3) A message informing whether a server connection has been successfully created or torn down. Instance ofclient.ClientConnectedMessage
        4) A message informing the thread it should exit. Instance of messages.KillThreadMessage

        This thread needs to print messages that are avoided due to being duplication. Let's use the 'critical' logging level for this so that it always shows and we can suppress other output for demos

        '''

        '''
        Helpful data structures for duplication handling.
        '''
        #Dictionary which helps in finding duplicates
        find_dup_resp_msg = {}

        # variable to keep track of active servers communicating with the client
        num_active_servers = 0
        primary_server_id = constants.NULL_SERVER_ID    
        kill_signal_received = False
        while not kill_signal_received:

            msg = None
            try: 
                msg = response_queue.get(block=True, timeout=constants.QUEUE_TIMEOUT)[1] #entries are tuples of form (priority, msg)
                self.logger.debug('Duplication handler received msg: [%s]', msg)

                if(isinstance(msg, messages.ClientRequestMessage)):
                    req_no = msg.request_number
                    # create an entry in the Dictionary for the corresponding request number (to keep track of it)
                    find_dup_resp_msg[req_no] = 0  
                    
                #handle duplicates in this case    
                elif(isinstance(msg, messages.ClientResponseMessage)):  
                    req_no = msg.request_number  
                    s_id = msg.server_id

                    if req_no in find_dup_resp_msg: 

                        #increment to keep track of the number of messages we have got with the same req_no till now
                        find_dup_resp_msg[req_no] = (find_dup_resp_msg[req_no] + 1)
                        
                        # It is a duplicate msg if the value of the entry is more than 1
                        if find_dup_resp_msg[req_no] > 1:
                            self.logger.critical('request_num %d: Discarded duplicate reply from S%d', req_no, s_id)

                        #remove the element with req_no as the key from the dictionary 
                        # we no longer need it as all possible duplicates are received if the below condn is satisfied
                        if find_dup_resp_msg[req_no] == num_active_servers or s_id == primary_server_id:
                            find_dup_resp_msg.pop(req_no)

                # increment active servers as a new server has established connection with the client
                elif(isinstance(msg, ClientServerConnectionMessage)): 
                    if msg.is_connected == True :
                        num_active_servers = (num_active_servers + 1)
                    else:
                        num_active_servers = (num_active_servers - 1)  

                    if msg.is_primary:
                        primary_server_id = msg.server_id   

                # decrement active servers as a server has disconnected
                elif(isinstance(msg,messages.KillThreadMessage)):         
                    kill_signal_received = True
                else:
                    logger.error('It should not reach here. no such msg. msg: [%s]', msg)    

            except queue.Empty: continue

        self.logger.info("Exiting thread for duplicate handling")


if __name__ == "__main__":
    ip, port, client_id, address_info, gfd_ip = parse_args() #won't we need multiple ip, port pairs for each of the replicas? Can pull from config file, CLI, or even RM
    if len(address_info) == 0:
        address_info.append((1, ip+':'+port))
    # addr1 = '127.0.0.1:19618'
    # addr2 = '127.0.0.1:19619'
    # addr3 = '127.0.0.1:19620'
    DebugLogger.setup_file_handler('./client-'+str(client_id)+'.log', level=1)
    client = Client(address_info=address_info, client_id=client_id, gfd_ip=gfd_ip)
    logger.info(client.server_addresses)
    client.start_client()