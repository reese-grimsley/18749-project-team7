'''Constant values for use across several files'''

LOCAL_HOST = '127.0.0.1'
CATCH_ALL_IP = '0.0.0.0' # Bind to this to accept a connection from anything that arrives to this machine
DEFAULT_APP_SERVER_PORT = 19618


MAGIC_MSG_LFD_REQUEST = "lfd-heartbeat" #in case we want to differentiate based on message to the server. It should get a response, but must not modify state
MAGIC_MSG_LFD_RESPONSE = "lfd-heartbeat" #expect to get this information back exactly from the server
DEFAULT_HEARTBEAT_PERIOD = 1 # seconds

IPV4_REGEX = '^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'

