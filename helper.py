import socket 
import threading
import re
import constants
import DebugLogger

helper_logger = DebugLogger.get_logger('helper')

def is_valid_ipv4(ipv4):
    if re.search(constants.IPV4_REGEX, ipv4) is None:
        print(re.search(constants.IPV4_REGEX, ipv4))
        return False
    return True

def addr_to_ip_port(addr, userspace_port_only=True):
    '''
    Return the ip and port of an address using typical ipv4 convention of X.X.X.X:P
    addr: format in X.X.X.X:P form, e.g. 255.255.255.255:1001
    return: IP and port as first and second argument. IP is an ipv4 string, and port is an integer
    '''
    try:
        ip = addr.split(':')[0]
        assert is_valid_ipv4(ip), 'Invalid IP address! should be format like 255.255.255.255:1001; you gave: ' + addr
        port = int(addr.split(':')[1])
        if userspace_port_only and (port < 1024 or port > 65535):
            raise ValueError("Invalid port number! Must between 1024 and 65535!")
        return ip, port

    except Exception as e:
        helper_logger.error(e)
        return None, None 

def parse_addresses_file(path):
    '''
    Assumes the file at ``path`` is a set of lines, in which each line contains N IP:Port, where N is an integer, IP is an ipv4 address, and port is valid user-space port
    :param path: a path to the file to read.
    :return: a list of server address info. Each item is a tuple, in which the first value is the server ID, and second is the IP:port string.
    '''
    try: 
        with open(path, 'r') as f:
            server_info = []
            lines = f.readlines()
            for line in lines:
                if line[0] == '#': continue
                
                server_id = int(line.split(' ')[0])
                addr_str = line.split(' ')[1]
                server_info.append((server_id, addr_str))

            helper_logger.info(server_info)
            return server_info

    except Exception as e:
        helper_logger.error(e)
        raise e


def basic_server(handler_function, ip=constants.CATCH_ALL_IP, port=constants.DEFAULT_APP_SERVER_PORT, logger=helper_logger, reuse_addr=True, daemonic=True):
    '''
    Basic server application. Accepts new connections and forks a thread for that new socket

    handler_function: A callback function that accepts two arguments: a socket and an address. 
    ip: The ip this server will bind to. Recommended to use a catch-all (0.0.0.0)
    port: The port this server will bind to
    logger: for consitency sake, provide a logger from the module calling this basic_server
    reuse_addr: tell the server socket to reuse the address or not. If False, it may take 30-90 seconds for the OS to recycle the port.
    daemonic: boolean indicating if client-handling threads should be 'daemons' or not. The main distinction is that daemonic threads run in the background will be killed when all non-daemon are dead (helpful for removing straggler client threads when the main process shuts down)
    '''

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        try:
            if reuse_addr:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # More portable to use socket.SO_REUSEADDR than SO_REUSEPORT.

            server_socket.bind((ip, port))
            server_socket.listen()

            while True:
                client_socket, address = server_socket.accept()
                logger.info('Connected by %s', address)

                thread = threading.Thread(target=handler_function, args=[client_socket, address], daemon=daemonic)
                thread.start()

        except KeyboardInterrupt:
            logger.critical('Keyboard interrupt in server; exiting')
        except Exception as e:
            logger.error(e)



if __name__ == "__main__":
    ip_addr = '127.0.0.1'
    port = constants.DEFAULT_APP_SERVER_PORT

    def handler_f(cs, addr):
        helper_logger.info('I accepted a client')
        helper_logger.info(addr)
        while True:
            data = cs.recv(1024)
            helper_logger.debug(data)
            if len(data) == 0: break
        cs.close()

    basic_server(handler_f, ip_addr, port)

    
