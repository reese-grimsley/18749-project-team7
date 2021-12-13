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

server_ip = 0
server_port = 0
heartbeat_period = 0 
gfd_ip = 0
gfd_port = 0 
lfd_id = 0
flag = 0
temp_source_ip = 0
temp_dest_ip = 0


def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.LOCAL_HOST, help='The IP address of the application server, which should always be a localhost', type=str)
    parser.add_argument('-gi', '--gip', metavar='gi', default=constants.ECE_CLUSTER_ONE, help='The IP address of GFD', type=str)
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


def poke_server(client_socket, lfd_id):
    '''
    return: True if application server responded as expected
    '''
    #create a new socket every single time; restart from scratch
    success = False
    global server_response
    global temp_source_ip
    global temp_dest_ip
    global flag
    try: 
        if flag == 1:
            quiet_message = messages.QuietMessage(temp_source_ip, temp_dest_ip)
            quiet_bytes = quiet_message.serialize()
            client_socket.sendall(quiet_bytes)
            flag = 0
            
        else:   
            lfd_message = messages.LFDMessage()
            lfd_bytes = lfd_message.serialize()
            # client_socket.sendall(bytes(constants.MAGIC_MSG_LFD_REQUEST, encoding='utf-8'))
            client_socket.sendall(lfd_bytes)
            response_bytes = client_socket.recv(constants.MAX_MSG_SIZE)
            response_msg = messages.deserialize(response_bytes)
            if constants.MAGIC_MSG_LFD_RESPONSE in response_msg.data:
                logger.info('Received from S' + str(lfd_id) + ' : [%s]', response_msg.data)
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

'''def run_lfd(ip=constants.LOCAL_HOST, port=constants.DEFAULT_APP_SERVER_PORT, period=constants.DEFAULT_HEARTBEAT_PERIOD):
    logger.info('Running local fault detector... contacting port %d every %.2f seconds', port, period)
    num_failures = 0
    num_heartbeats = 0

    try: 
        client_socket = None
        while True:

            now = time.time()
            next_hb = now + period

            # server_good = True
            # make the request; should finish (timeout) before the next heartbeat needs to occur
            num_heartbeats += 1

            try: 
                if client_socket is None:
                    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_socket.settimeout(period/2)
                    client_socket.connect((ip, port))

                server_good = poke_server(client_socket)
            except Exception as e:
                logger.debug('Exception caught; assume server is not good..')
                logger.debug(e)
                server_good = False

            if server_good:
                logger.info("Server responded correctly; waiting until next heartbeat")
            else: 
                num_failures += 1 #some light instrumentation
                logger.warning("Server failed to respond; %d failures (of %d heartbeats)", num_failures, num_heartbeats)

                #reset
                client_socket.close()
                client_socket = None
                #TODO: notify global FD (if extant) of failure. 
                #TODO: trigger main server to reset?

            # wait for the next period
            time_to_sleep = next_hb - time.time()
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    except Exception as e:
        logger.error(e)
        raise 
'''

def run_lfd(lfd_socket, period, lfd_id):
    num_failures = 0
    num_heartbeats = 0
    global server_fail
    #global flag
    try: 
        while True:
            now = time.time()
            next_hb = now + period
            # server_good = True
            # make the request; should finish (timeout) before the next heartbeat needs to occur
            num_heartbeats += 1
            server_response = 0
            try: 
                server_good = poke_server(lfd_socket, lfd_id)
                #flag = 0
                
            except Exception as e:
                logger.debug('Exception caught; assume server is not good..')
                logger.debug(e)
                server_good = False
                #flag = 0

            if server_good:
                logger.info("Server responded correctly; waiting until next heartbeat")
                '''
                if flag == 0:
                    response = constants.MAGIC_MSG_SERVER_START + " at S" + str(lfd_id) + ": " + str(constants.LOCAL_HOST)
                    lfd_socket.sendall(str.encode(response))
                    logger.info("LFD sends respawn server request to GFD")
                    flag = 1
                '''    
                    

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
    global temp_source_ip
    global temp_dest_ip
    global flag
    try:
        while True:
            data = lfd_socket.recv(1024).decode(encoding='utf-8')
            if not data:
                continue
            logger.info("Received from GFD: [%s]", str(data))
            if constants.MAGIC_MSG_GFD_REQUEST in data:
                response = "LFD" + str(lfd_id) + ": lfd-heartbeat"
                lfd_socket.sendall(str.encode(response))

            if constants.MAGIC_MSG_QUIET in data:
                temp_msg = messages.deserialize(data)
                temp_source_ip = temp_msg.source_ip
                temp_dest_ip = temp_msg.dest_ip
                flag = 1


            if server_response == 1:
                logger.info("LFD sends add server request to GFD")
                response = constants.MAGIC_MSG_SERVER_START + " at S" + str(lfd_id) + ": " + str(server_ip)
                lfd_socket.sendall(str.encode(response))
            if server_fail is True:
                logger.info("LFD sends delete server request to GFD")
                response = constants.MAGIC_MSG_SERVER_FAIL + " at S" + str(lfd_id) + ": " + str(server_ip)
                lfd_socket.sendall(str.encode(response))
                server_fail = False
                make_conn_to_server(constants.LOCAL_HOST, constants.DEFAULT_APP_SERVER_PORT, constants.DEFAULT_HEARTBEAT_PERIOD, constants.ECE_CLUSTER_ONE, constants.DEFAULT_GFD_PORT, 1)
                
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    
def start_conn(ip, port, period, recipient, lfdID):
    conn = True
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
            conn = False

        return conn    
                

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    except ConnectionRefusedError:
      return True
      #  start_conn(ip, port, period, recipient)


def make_conn_to_server(server_ip, server_port, heartbeat_period, gfd_ip, gfd_port, lfd_id):

    global server_response
    server_response = 0
    try:
        start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period, recipient="gfd", lfdID=lfd_id)
        temp = True
        while(temp):
            temp = start_conn(ip=server_ip, port=server_port, period=heartbeat_period, recipient="server", lfdID=lfd_id)
        while (True):
            pass
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    #run_lfd(ip=server_ip, port=server_port, period=heartbeat_period) #block forever (until KB interrupt)

if __name__ == "__main__":

    #server_ip, server_port, heartbeat_period, gfd_ip, gfd_port, lfd_id = parse_args()
    make_conn_to_server(constants.LOCAL_HOST, constants.DEFAULT_APP_SERVER_PORT, constants.DEFAULT_HEARTBEAT_PERIOD, constants.ECE_CLUSTER_ONE, constants.DEFAULT_GFD_PORT, 1)


  