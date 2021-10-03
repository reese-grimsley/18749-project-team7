import socket, argparse, time
import DebugLogger, constants
from helper import is_valid_ipv4
import threading

logger = DebugLogger.get_logger('lfd')
server_response = 0

def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_APP_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.ECE_CLUSTER_TWO, help='The IP address of the application server, which should always be a localhost', type=str)
    parser.add_argument('-gi', '--gip', metavar='gi', default=constants.ECE_CLUSTER_ONE, help='The IP address of GFD', type=str)
    parser.add_argument('-gp', '--gport', metavar='gp', default=constants.DEFAULT_GFD_PORT, help='The port number of GFD', type=int)

    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')

    return args.ip, args.port, args.heartbeat, args.gip, args.gport 

def handle_server(lfd_socket, period):
    global server_response
    try:
        while True:
            lfd_socket.sendall(str.encode(constants.MAGIC_MSG_LFD_REQUEST))
            logger.info("LFD sends heartbeat request to server")
            response = lfd_socket.recv(1024).decode(encoding='utf-8')
            if constants.MAGIC_MSG_LFD_RESPONSE in response:
                logger.info("Received from server: [%s]", str(response))
                server_response += 1
                
            time.sleep(period)
            
    except Exception as e:
        logger.debug(e)

def handle_gfd(lfd_socket, period, server_ip):
    global server_response
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
    #except Exception as e:
    #    print(e)
        
def start_conn(ip, port, period, recipient):
    try: 
        lfd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #lfd_socket.settimeout(period/2)
        lfd_socket.connect((ip, port))
        logger.info('Connected to %s', ip)
        if recipient is "gfd":
          thread = threading.Thread(target=handle_gfd, args=[lfd_socket, period, server_ip], daemon=1)
          thread.start()
        elif recipient is "server":
          thread = threading.Thread(target=handle_server, args=[lfd_socket, period], daemon=1)
          thread.start()
        
    except KeyboardInterrupt:
        logger.warning('Caught Keyboard Interrupt in local fault detector; exiting')
    '''except Exception as e:
        print(e)'''
        
if __name__ == "__main__":
    server_ip, server_port, heartbeat_period, gfd_ip, gfd_port = parse_args()
    start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period, recipient="gfd")
    start_conn(ip=server_ip, port=server_port, period=heartbeat_period, recipient="server")
    while (True):
        pass
        
