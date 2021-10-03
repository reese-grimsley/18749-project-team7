# based on lfd.py
import socket, argparse, time
import DebugLogger, constants
import threading
from helper import is_valid_ipv4

membership = []
logger = DebugLogger.get_logger('gfd')

def parse_args():
    parser = argparse.ArgumentParser(description="Global Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_GFD_PORT, help='The port that the gfd will be listening to LFD', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.ECE_CLUSTER_ONE, help='The IP address of the gfd', type=str)

    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')
    
    return args.ip, args.port, args.heartbeat


def register_membership(data):
    response = str(data)
    response_list = response.split()
    server_ip = response_list[len(response_list) - 1]
    logger.info("Add server " + str(server_ip) + " to membership")
    membership.append(server_ip) 

def cancel_membership(data):
    response = str(data)
    response_list = response.split()
    server_ip = response_list[len(response_list) - 1]
    logger.info("Remove " + str(server_ip) + " out membership")
    if server_ip in membership:
        membership.remove(server_ip)

def poke_lfd(conn, period):
    success = False
    try: 
        data = conn.recv(1024).decode(encoding='utf-8')
        conn.sendall(bytes(constants.MAGIC_MSG_GFD_REQUEST, encoding='utf-8'))
        success = True
        
    except socket.timeout as st:
        logger.error("Heartbeat request timed out")
        success = False
    except Exception as e:
        print(e)
        
    return success

def serve_lfd(conn, addr, period):
    try:
        lfd_status = True
        while True:
            #lfd_status = poke_lfd(conn, period)
            conn.sendall(bytes(constants.MAGIC_MSG_GFD_REQUEST, encoding='utf-8'))
            data = conn.recv(1024).decode(encoding='utf-8')
            logger.info('Received from socket: [%s]', str(data))

            if constants.MAGIC_MSG_RESPONSE_FROM_LFD in data:    # if receive lfd heartbeat
                success = True
            elif constants.MAGIC_MSG_SERVER_FAIL in data:        # if receive server fail message, cancel membership
                cancel_membership(data)
                success = True
            elif constants.MAGIC_MSG_SERVER_START in data:
                register_membership(data)
                success = True

            if not lfd_status:
                logger.debug("Something wrong with lfd")
                break
            time.sleep(period)
    except Exception as e:
        logger.info("in serve lfd")
        print(e)

    '''finally: 
        conn.close()
        logger.info('Closed connection for lfd at (%s)', addr)'''


def start_conn(ip, port, period):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gfd_socket:
        try:
            gfd_socket.bind((ip, port))
            gfd_socket.listen()

            while True:
                conn, address = gfd_socket.accept()
                logger.info('Connected by %s', str(address))
                thread = threading.Thread(target=serve_lfd, args=[conn, address, period], daemon=1)
                thread.start()

        except KeyboardInterrupt:
            logger.critical('Keyboard interrupt in server; exiting')
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    gfd_ip, gfd_port, heartbeat_period = parse_args()
    start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period) 
