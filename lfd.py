 # Written by Reese Grimsley, Sept 6, 2021
# Modified by Ting Chen and Chia-Chia Chang, Oct 4, 2021

import socket, argparse, time
import DebugLogger, constants
from helper import is_valid_ipv4
import threading

import messages

logger = DebugLogger.get_logger('lfd')
server_response = 0
server_fail = False
#server_type = ""
primary_msg  = ""
def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.LOCAL_HOST, help='The IP address of the application server, which should always be a localhost', type=str)
    parser.add_argument('-gi', '--gip', metavar='gi', default=constants.ECE_CLUSTER_FOUR, help='The IP address of GFD', type=str)
    parser.add_argument('-gp', '--gport', metavar='gp', default=constants.DEFAULT_GFD_PORT, help='The port number of GFD', type=int)
    parser.add_argument('-l', '--lfd_id', metavar='l', default=1, help="A lfd identifier (an integer, for simplicity)", type=int)
    
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    return args.ip, args.port, args.heartbeat, args.gip, args.gport, args.lfd_id

def is_primary(msg, lfd_id):
    msg_list = msg.split()
    server_id = msg_list[0]
    
    return True if str(lfd_id) in server_id else False
    
def poke_server(client_socket, lfd_id):
    '''
    return: True if application server responded as expected
    '''
    #create a new socket every single time; restart from scratch
    success = False
    global server_response
    
    try: 
        lfd_message = messages.LFDMessage()
        lfd_bytes = lfd_message.serialize()
        
        client_socket.sendall(lfd_bytes)
        response_bytes = client_socket.recv(constants.MAX_MSG_SIZE)
        response_msg = messages.deserialize(response_bytes)
        
        if constants.MAGIC_MSG_LFD_RESPONSE in response_msg.data:
            logger.info('Received from S' + str(lfd_id) + ' : [%s]', response_msg.data)
            #server_primary = is_primary(response_msg.data)
            success = True
            server_response += 1
        
               
    except socket.timeout as st:
        logger.error("Heartbeat request timed out")
        success = False
    except Exception as e:
        logger.error(e)
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
          
    return success

def run_lfd(lfd_socket, period, lfd_id):
    num_failures = 0
    num_heartbeats = 0
    global server_fail
    global primary_msg 
    
    try: 
        while True:
            now = time.time()
            next_hb = now + period
            # server_good = True
            # make the request; should finish (timeout) before the next heartbeat needs to occur
            num_heartbeats += 1
            try: 
                server_good = poke_server(lfd_socket, lfd_id)
            except Exception as e:
                logger.debug('Exception caught; assume server is not good..')
                logger.debug(e)
                server_good = False

            if server_good:
                logger.info("Server responded correctly; waiting until next heartbeat")           
            else:
                num_failures += 1
                server_fail = True            # notify gfd
                logger.warning("Server failed to respond; %d failures", num_failures)
                '''num_failures += 1 #some light instrumentation
                logger.warning("Server failed to respond; %d failures (of %d heartbeats)", num_failures, num_heartbeats)
                '''
                #reset
                lfd_socket.close()
                lfd_socket = None
                break
                
            if primary_msg  != "":
                logger.info('Send S' + str(lfd_id) + ' : [%s]', primary_msg)
                lfd_message = messages.LFDMessage(primary_msg)
                lfd_bytes = lfd_message.serialize()
                lfd_socket.sendall(lfd_bytes) 
                primary_msg = ""
                
            time.sleep(period)

            # wait for the next period
            time_to_sleep = next_hb - time.time()
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    except Exception as e:
        logger.error(e)
        raise

def handle_gfd(lfd_socket, server_ip, lfd_id):
    global server_response
    global server_fail
    #global server_type
    global primary_msg 
    try:
        while True:
            response_bytes = lfd_socket.recv(constants.MAX_MSG_SIZE)
            response_msg = messages.deserialize(response_bytes)
            
            if not response_msg.data or response_msg.data is None:
                continue
                
            logger.info("Received from GFD: [%s]", response_msg.data)
            if server_response > 0 and server_response <= 2:
                logger.info("Send to GFD: [" + "LFD" + str(lfd_id) + " sends add S" + str(lfd_id) + " request to GFD]")
                lfd_response = messages.LFDGFDMessage(lfd_id, constants.LFD_ACTION_ADD_SERVER, server_ip)
                lfd_bytes = lfd_response.serialize()
                lfd_socket.sendall(lfd_bytes)
            if server_fail is True:
                logger.info("Send to GFD: [" + "LFD" + str(lfd_id) + " sends delete S" + str(lfd_id) + " request to GFD]")
                lfd_response = messages.LFDGFDMessage(lfd_id, constants.LFD_ACTION_RM_SERVER, server_ip)
                lfd_bytes = lfd_response.serialize()
                lfd_socket.sendall(lfd_bytes)
                server_fail = False
            if constants.MAGIC_MSG_GFD_REQUEST in response_msg.data:
                logger.info("Send to GFD: [LFD Heartbeat]")
                lfd_response = messages.LFDGFDMessage(lfd_id, constants.LFD_ACTION_HB, 0)
                lfd_bytes = lfd_response.serialize()
                lfd_socket.sendall(lfd_bytes)
            if constants.MAGIC_MSG_PRIMARY in response_msg.data:               
                '''if is_primary(response_msg.data, lfd_id) :
                    server_type = "Primary"
                else:
                    server_type = "Backup"'''
                primary_msg = response_msg.data        
                
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')

def start_server_conn(ip, port, period, recipient, lfdID):
    conn = False
    print(ip)
    print(port)
    while True:
        try:
            lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            lfd_socket.settimeout(period + 2)
            lfd_socket.connect((ip, port))
            logger.info('Connected to %s', ip)
            if recipient is "gfd":
                thread = threading.Thread(target=handle_gfd, args=[lfd_socket, server_ip, lfdID], daemon=1)
                thread.start()
                    
            elif recipient is "server":
                thread = threading.Thread(target=run_lfd, args=[lfd_socket, period, lfdID], daemon=1)
                thread.start()
                break
                
        except KeyboardInterrupt:
            logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
        except ConnectionRefusedError:
            logger.error("connection refused")
            time.sleep(3)
   
def start_gfd_conn(ip, port, period, recipient, lfdID):
    conn = False
    print(ip)
    print(port)
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.settimeout(period + 2)
        lfd_socket.connect((ip, port))
        logger.info('Connected to %s', ip)
        if recipient is "gfd":
            thread = threading.Thread(target=handle_gfd, args=[lfd_socket, server_ip, lfdID], daemon=1)
            thread.start()
                
        elif recipient is "server":
            thread = threading.Thread(target=run_lfd, args=[lfd_socket, period, lfdID], daemon=1)
            thread.start()
                

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    except ConnectionRefusedError:
        #start_conn(ip, port, period, recipient, lfdID)
        #lfd_socket.connect((ip, port))
        logger.error("connection refused")
        
if __name__ == "__main__":
    server_ip, server_port, heartbeat_period, gfd_ip, gfd_port, lfd_id = parse_args()
    try:
        start_gfd_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period, recipient="gfd", lfdID=lfd_id)
        start_server_conn(ip=server_ip, port=server_port, period=heartbeat_period, recipient="server", lfdID=lfd_id)
        
        while (True):
            pass
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    #run_lfd(ip=server_ip, port=server_port, period=heartbeat_period) #block forever (until KB interrupt)