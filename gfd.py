# based on lfd.py
import socket, argparse, time
import DebugLogger, constants
import threading
from helper import is_valid_ipv4
import messages

membership = []
client_membership = []
logger = DebugLogger.get_logger('gfd')

def parse_args():
    parser = argparse.ArgumentParser(description="Global Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=constants.DEFAULT_GFD_PORT, help='The port that the gfd will be listening to LFD', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=constants.DEFAULT_HEARTBEAT_PERIOD, help='The period between each heartbeat, in seconds', type=float)
    parser.add_argument('-i', '--ip', metavar='i', default=constants.ECE_CLUSTER_FOUR, help='The IP address of the gfd', type=str)

    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')
    
    return args.ip, args.port, args.heartbeat

def print_membership(list):
    num = len(list)
    members = ""
    for member in list:
        members += "[" + member + "] "       
    logger.info("GFD: " + str(num) + " member(s) - " + members)
    
def register_membership(data):
    response = str(data)
    response_list = response.split()                  # split based on spaces
    server_type = response_list[0]
    server_ip = response_list[len(response_list) - 1]
    server_id = response_list[len(response_list) - 3]
    server = str(server_type) + " " + str(server_id) + " : " + str(server_ip)
    if server not in membership:
        logger.info("Add " + server + " to membership")
        membership.append(server) 
        print_membership(membership)

def register_client(data):
    response = str(data)
    response_list = response.split()
    client_id = response_list[len(response_list) - 1]
    client = "C" + str(client_id)
    if client not in client_membership:
        logger.info("Add " + client + " to membership")
        client_membership.append(client)
        print_membership(client_membership)
    
def cancel_membership(data):
    response = str(data)
    response_list = response.split()
    server_type = response_list[0]
    server_ip = response_list[len(response_list) - 1]
    server_id = response_list[len(response_list) - 3]
    server = str(server_type) + " " + str(server_id) + " : " + str(server_ip)
    logger.info("Remove " + server + " out membership")
    if server in membership:
        membership.remove(server)
    print_membership(membership)

def parse_membership(member):
    # member format: Primary S1 : 172.19.137.180 
    member_list = member.split()    # split based on spaces
    server_type = member_list[0]
    print(str(server_type))
    server_ip = member_list[len(member_list) - 1]
    server_id = member_list[len(member_list) - 3][1]
    is_primary = True if "Primary" in server_type else False
    return server_ip, server_id, is_primary
    
def serve_client(conn):
    prev_list = []
    while True:
        if len(membership) > len(prev_list):
            for member in membership:
                if  member not in prev_list:
                    break
            #server_ip, sid, is_primary=None, action=constants.GFD_ACTION_NEW
            server_ip, server_id, is_primary = parse_membership(member)  
            
            gfd_msg = messages.GFDClientMessage(server_ip, server_id, is_primary, constants.GFD_ACTION_NEW)
            logger.info("GFD notify Client: [" + constants.MAGIC_MSG_ADD_NEW_SERVER + gfd_msg.data + "]")
            gfd_msg_bytes = gfd_msg.serialize()
            conn.sendall(gfd_msg_bytes)
            #msg = constants.MAGIC_MSG_ADD_NEW_SERVER + str(connID)
            #conn.sendall(bytes(msg, encoding='utf-8'))
            prev_list.append(member)
            time.sleep(1)
        elif len(membership) < len(prev_list):
            for member in prev_list:
                if member not in membership:
                    break
            server_ip, server_id, is_primary = parse_membership(member)  
            gfd_msg = messages.GFDClientMessage(server_ip, server_id, is_primary, constants.GFD_ACTION_DEAD)
            gfd_msg_bytes = gfd_msg.serialize()
            conn.sendall(gfd_msg_bytes)
            #msg = constants.MAGIC_MSG_REMOVE_SERVER + str(connID)
            #conn.sendall(bytes(msg, encoding='utf-8'))
            prev_list.remove(member)
            logger.info("GFD notify Client: [" + constants.MAGIC_MSG_REMOVE_SERVER + gfd_msg.data + "]")
        else:
            continue
        
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
            # send GFD request LFD heartbeat to LFD
            gfd_request = messages.GFDMessage()
            gfd_request_bytes = gfd_request.serialize()
            conn.sendall(gfd_request_bytes)
            
            response_bytes = conn.recv(constants.MAX_MSG_SIZE)
            response_msg = messages.deserialize(response_bytes)
            
            #logger.info('Received from LFD %s: [%s]', str(addr), response_msg.data)
            if constants.MAGIC_MSG_LFD_RESPONSE in response_msg.data:    # if receive lfd heartbeat
                logger.info('Received from LFD %s: [%s]', str(addr), response_msg.data)
                success = True
            elif constants.MAGIC_MSG_SERVER_FAIL in response_msg.data:        # if receive server fail message, cancel membership
                logger.info('Received from LFD %s: [%s]', str(addr), response_msg.data)
                cancel_membership(response_msg.data)
                success = True
            elif constants.MAGIC_MSG_SERVER_START in response_msg.data:
                logger.info('Received from LFD %s: [%s]', str(addr), response_msg.data)
                register_membership(response_msg.data)
                success = True
            elif constants.MAGIC_MSG_RESPONSE_FROM_CLIENT in response_msg.data:
                logger.info('Received from Client %s: [%s]', str(addr), response_msg.data)
                print("start register client")
                register_client(response_msg.data)
                print("start serve client")
                serve_client(conn)
                print("finish client")
                success = True
            
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