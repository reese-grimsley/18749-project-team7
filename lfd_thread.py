# Written by Reese Grimsley, Sept 6, 2021

import socket, argparse, time
import DebugLogger, constants
from helper import is_valid_ipv4
import threading

logger = DebugLogger.get_logger('lfd')
DEFAULT_GFD_IP = 'ece001.ece.local.cmu.edu'
DEFAULT_GFD_PORT = 15213

def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.LOCAL_HOST, help='The IP address of the application server, which should always be a localhost', type=str)
    parser.add_argument('-gi', '--gip', metavar='gi', default=DEFAULT_GFD_IP, help='The IP address of GFD', type=str)
    parser.add_argument('-gp', '--gport', metavar='gp', default=DEFAULT_GFD_PORT, help='The port number of GFD', type=str)

    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')
    '''if not is_valid_ipv4(args.ip): 
        print(args.ip)
        raise ValueError('The IP address given [%s] is not a valid format', args.ip)
    '''
    return args.ip, args.port, args.heartbeat, args.gip, args.gport 

def serve(lfd_socket, period):
    message = "Hi, I am LFD."
    try:
        while True:
            lfd_socket.sendall(str.encode(message))
            gfd_request = lfd_socket.recv(1024)
            logger.info(gfd_request.decode('utf-8'))
            time.sleep(period)

    finally: 
        lfd_socket.close()
        logger.info('Closed connection')
        
def start_conn(ip, port, period):
    num_heartbeats = 0
    try: 
        lfd_socket = None
        try: 
            if lfd_socket is None:
                lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                lfd_socket.settimeout(period/2)
                lfd_socket.connect((ip, port))
                thread = threading.Thread(target=serve, args=[lfd_socket, period], daemon=1)
        except Exception as e:
            logger.debug(e)

    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    except Exception as e:
        logger.error(e)
        raise 

if __name__ == "__main__":
    server_ip, server_port, heartbeat_period, gfd_ip, gfd_port = parse_args()
    start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period)
    start_conn(ip=server_ip, port=server_port, period=heartbeat_period) #block forever (until KB interrupt)