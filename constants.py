'''Constant values for use across several files'''

LOCAL_HOST = '127.0.0.1'
ECE_CLUSTER_ONE = 'ece001.ece.local.cmu.edu'
ECE_CLUSTER_TWO = 'ece002.ece.local.cmu.edu'
ECE_CLUSTER_THREE = 'ece003.ece.local.cmu.edu'
CATCH_ALL_IP = '0.0.0.0' # Bind to this to accept a connection from anything that arrives to this machine
DEFAULT_APP_SERVER_PORT = 19618
DEFAULT_GFD_PORT = 15213


MAGIC_MSG_LFD_REQUEST = "lfd-heartbeat" #in case we want to differentiate based on message to the server. It should get a response, but must not modify state
MAGIC_MSG_LFD_RESPONSE = "lfd-heartbeat" #expect to get this information back exactly from the server
DEFAULT_HEARTBEAT_PERIOD = 3 # seconds

MAGIC_MSG_GFD_REQUEST = "gfd request lfd-heartbeat" 
MAGIC_MSG_RESPONSE_FROM_LFD = "lfd sends heartbeat to gfd"
MAGIC_MSG_SERVER_START = "Server started"
MAGIC_MSG_SERVER_FAIL = "Server failed"
MAGIC_MSG_LFD_REQUEST_SERVER = "Request for server heartbeat"

IPV4_REGEX = '^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'

QUEUE_TIMEOUT = 5.0 #seconds that reading from a queue will block for before returning 'None'
CLIENT_SERVER_TIMEOUT = 30.0 # seconds that will pass in 'recv' before exiting with a TimeoutError

NULL_SERVER_ID = -1 

MAX_MSG_SIZE = 1350