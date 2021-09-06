# Written by Reese Grimsley, Sept 6, 2021

import socket, argparse, time
import DebugLogger

logger = DebugLogger.get_logger('lfd')

HOST = '127.0.0.1'
DEFAULT_SERVER_PORT = 19618
DEFAULT_HEARTBEAT = 1 # seconds
MAGIC_MSG_LFD_REQUEST = "lfd-heartbeat" #in case we want to differentiate based on message to the server. It should get a response, but must not modify state
MAGIC_MSG_LFD_RESPONSE = "lfd-heartbeat" #expect to get this information back exactly from the server


heartbeat_period = DEFAULT_HEARTBEAT
server_port = DEFAULT_SERVER_PORT


def parse_args():
    parser = argparse.ArgumentParser(description="Local Fault Detector")

    parser.add_argument('-p', '--port', metavar='p', default=DEFAULT_SERVER_PORT, help='The port that the server will be listening to and that this LFD will access', type=int)
    parser.add_argument('-hb', '--heartbeat', metavar='HB', default=DEFAULT_HEARTBEAT, help='The period between each heartbeat, in seconds', type=float)
    args = parser.parse_args()

    if args.port < 1024 or args.port > 65535:
        raise ValueError('The port must be between 1024 and 65535')
    if args.heartbeat <= 0: 
        raise ValueError('The heartbeat must be a positive value')

    return args.port, args.heartbeat


def poke_server(host=HOST, port=DEFAULT_SERVER_PORT, timeout=1):
    #create a new socket every single time; restart from scratch
    success = False
    try: 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(bytes(MAGIC_MSG_LFD_REQUEST, encoding='utf-8'))
            data = s.recv(1024).decode(encoding='utf-8')
            logger.debug('Received from socket: [%s]', data)
            if MAGIC_MSG_LFD_RESPONSE in data:
                success = True

    except socket.timeout as st:
        logger.error("Heartbeat request timed out")
        success = False
    except Exception as e:
        logger.error(e)
        
    return success

def run_lfd(port=DEFAULT_SERVER_PORT, period=heartbeat_period):
    logger.info('Running local fault detector... contacting port %d every %.2f seconds', port, period)
    num_failures = 0
    num_heartbeats = 0

    try: 
        while True:
            now = time.time()
            next_hb = now + period

            # server_good = True
            # make the request; should finish (timeout) before the next heartbeat needs to occur
            num_heartbeats += 1
            server_good = poke_server(host=HOST, port=port, timeout=period/2)

            if server_good:
                logger.info("Server responded correctly; waiting until next heartbeat")
            else: 
                num_failures += 1 #some light instrumentation
                logger.warning("Server failed to respond; %d failures (of %d heartbeats)", num_failures, num_heartbeats)

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


if __name__ == "__main__":
    server_port, heartbeat_period = parse_args()
    run_lfd(port=server_port, period=heartbeat_period) #block forever (until KB interrupt)