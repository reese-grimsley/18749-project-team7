# based on lfd.py
import socket, argparse, time
import DebugLogger, constants
import threading
from helper import is_valid_ipv4

membership = []
logger = DebugLogger.get_logger('gfd')
# when this flag is set, gfd sends quiescence is done message
quiet_flag = False

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

def print_membership():
    global membership
    num = len(membership)
    members = ""
    for member in membership:
        members += "[" + member + "] "       
    logger.info("GFD: " + str(num) + " member(s) - " + members)
    
def register_membership(data):
    global membership
    response = str(data)
    response_list = response.split()
    server_ip = response_list[len(response_list) - 1]
    server_id = response_list[len(response_list) - 2]
    server = str(server_id) + str(server_ip)
    logger.info("Add " + server + " to membership")
    membership.append(server) 
    print_membership()
    
def cancel_membership(data):
    global membership
    response = str(data)
    response_list = response.split()
    server_ip = response_list[len(response_list) - 1]
    server_id = response_list[len(response_list) - 2]
    server = str(server_id) + str(server_ip)
    logger.info("Remove " + server + " out membership")
    if server in membership:
        membership.remove(server)
    print_membership()
    
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
    global membership
    global quiet_flag
    try:
        lfd_status = True
        while True:
            # if quiet_flag is true, we send quiet is done to all the LFD + servers
            if (quiet_flag):
                logger.info("Escaping quiescence...")
                quiet_message = messages.QuietMessage(membership[0], membership[-1], 2)
                quiet_bytes = quiet_message.serialize()
                conn.sendall(quiet_bytes)
                quiet_flag = false
            
            #lfd_status = poke_lfd(conn, period)
            conn.sendall(bytes(constants.MAGIC_MSG_GFD_REQUEST, encoding='utf-8'))
            data = conn.recv(1024).decode(encoding='utf-8')
            logger.info('Received from LFD %s: [%s]', str(addr), str(data))

            if constants.MAGIC_MSG_RESPONSE_FROM_LFD in data:    # if receive lfd heartbeat
                success = True
            elif constants.MAGIC_MSG_SERVER_FAIL in data:        # if receive server fail message, cancel membership
                cancel_membership(data)
                success = True
            elif constants.MAGIC_MSG_QUIESCENCE_DONE in data:
                quiet_flag = True
            elif constants.MAGIC_MSG_SERVER_START in data:
                register_membership(data)
                success = True

                if len(membership) >= 2:
                    #send a go to quiescence msg to all servers
                    logger.info("Entering quiescence...")
                    quiet_message = messages.QuietMessage(membership[0], membership[-1])
                    quiet_bytes = quiet_message.serialize()
                    conn.sendall(quiet_bytes)



            if not lfd_status:
                logger.debug("Something wrong with lfd")
                break
            time.sleep(period)
    except Exception as e:
        logger.info("in serve lfd")
        print(e)

    finally: 
        conn.close()
        logger.info('Closed connection for lfd at (%s)', addr)


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
            logger.critical('Keyboard interrupt in GFD; exiting')
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    gfd_ip, gfd_port, heartbeat_period = parse_args()
    start_conn(ip=gfd_ip, port=gfd_port, period=heartbeat_period) 
