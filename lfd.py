# Written by Reese Grimsley, Sept 6, 2021
# Modified by Ting Chen and Chia-Chia Chang, Oct 4, 2021

import socket, argparse, time
import DebugLogger, constants
from helper import is_valid_ipv4
import threading

logger = DebugLogger.get_logger('lfd')
server_response = 0
server_fail = False

def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.LOCAL_HOST, help='The IP address of the application server, which should always be a localhost', type=str)
    parser.add_argument('-gi', '--gip', metavar='gi', default=constants.ECE_CLUSTER_ONE, help='The IP address of GFD', type=str)
    parser.add_argument('-gp', '--gport', metavar='gp', default=constants.DEFAULT_GFD_PORT, help='The port number of GFD', type=int)

    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')
    if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)

    return args.ip, args.port, args.heartbeat, args.gip, args.gport


def poke_server(client_socket):
    '''
    return: True if application server responded as expected
    '''
    #create a new socket every single time; restart from scratch
    success = False
    global server_response
    try: 
        client_socket.sendall(bytes(constants.MAGIC_MSG_LFD_REQUEST, encoding='utf-8'))
        data = client_socket.recv(1024).decode(encoding='utf-8')
        logger.debug('Received from socket: [%s]', data)
        if constants.MAGIC_MSG_LFD_RESPONSE in data:
            logger.info("Received from server: [%s]", str(data))
            server_response += 1
            success = True

    except socket.timeout as st:
        logger.error("Heartbeat request timed out")
        success = False
    except Exception as e:
        logger.error(e)
        
    return success

def run_lfd(ip=constants.LOCAL_HOST, port=constants.DEFAULT_APP_SERVER_PORT, period=constants.DEFAULT_HEARTBEAT_PERIOD):
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
                num_failures += 1
                server_fail = True
                logger.warning("Server failed to respond; %d failures", num_failures)
                '''num_failures += 1 #some light instrumentation
                logger.warning("Server failed to respond; %d failures (of %d heartbeats)", num_failures, num_heartbeats)
                '''
                #reset
                client_socket.close()
                client_socket = None
                break
                #TODO: notify global FD (if extant) of failure. 
                #TODO: trigger main server to reset?

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


def start_conn(ip, port, period, recipient):
    try:
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lfd_socket.settimeout(period + 2)
        lfd_socket.connect((ip, port))
        logger.info('Connected to %s', ip)
        if recipient is "gfd":
            thread = threading.Thread(target=handle_gfd, args=[lfd_socket, period, server_ip], daemon=1)
            thread.start()
        elif recipient is "server":
            thread = threading.Thread(target=run_lfd, args=[lfd_socket, period], daemon=1)
            thread.start()

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    '''except Exception as e:
        print(e)'''


'''def handle_server(lfd_socket, period):
    global server_fail
    num_failures = 0
    try:
        while True:
            server_status = poke_server(lfd_socket)
            if not server_status:
                num_failures += 1
                server_fail = True
                logger.warning("Server failed to respond; %d failures", num_failures)
                break

            time.sleep(period)

    except Exception as e:
        logger.debug(e)

    finally:
        lfd_socket.close()'''


def handle_gfd(lfd_socket, server_ip):
    global server_response
    global server_fail

    while True:
        data = lfd_socket.recv(1024).decode(encoding='utf-8')
        if not data:
            continue
        logger.info("Received from gfd: [%s]", str(data))
        if constants.MAGIC_MSG_GFD_REQUEST in data:
            response = "lfd-heartbeat"
            lfd_socket.sendall(str.encode(response))
        if server_response == 1:
            print("Register!!!!")
            logger.info("LFD sends add server request to GFD")
            response = constants.MAGIC_MSG_SERVER_START + " at " + str(server_ip)
            lfd_socket.sendall(str.encode(response))
        if server_fail is True:
            print("Cancel!!!!")
            logger.info("LFD sends delete server request to GFD")
            response = constants.MAGIC_MSG_SERVER_FAIL + " at " + str(server_ip)
            lfd_socket.sendall(str.encode(response))
            server_fail = False

    # except Exception as e:
    #    print(e)

if __name__ == "__main__":
    server_ip, server_port, heartbeat_period, gfd_ip, gfd_port= parse_args()
    start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period, recipient="gfd")
    start_conn(ip=server_ip, port=server_port, period=heartbeat_period, recipient="server")
    while (True):
        pass
    # run_lfd(ip=server_ip, port=server_port, period=heartbeat_period) #block forever (until KB interrupt)