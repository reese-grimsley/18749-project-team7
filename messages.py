'''
A file to describe message formats. 

Each message may have a few parameters, like client ID, request number, etc. It's probably easiest to represent this as an object
Each message should have a predefined format when it's turned into a 'bytes' or 'bytearray' object prior to sending. Ergo, a 'serialize' and 'deserialize' function 
'''

import pickle
import constants
import DebugLogger

logger = DebugLogger.get_logger('messages')

def deserialize(data):
    if not isinstance(data, bytes) and not isinstance(data, bytearray):
        raise ValueError("Can only deserialize bytes or bytearray's")
    if (len(data)) > constants.MAX_MSG_SIZE:
        logger.warning('Message to be sent is larger than MAX SIZE [%d] for a single msg', constants.MAX_MSG_SIZE)
    if len(data) == 0:
        return None
    return pickle.loads(data)

class Message():

    def __init__(self, data=None):
        self.data = data
        pass

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        d =  pickle.dumps(self)
        if (len(d)) > constants.MAX_MSG_SIZE:
            logger.warning('Message to be sent is larger than MAX SIZE [%d] for a single msg', constants.MAX_MSG_SIZE)

        return d


class ClientRequestMessage(Message):
    '''
    Send from client to server as part of normal request-response flow
    '''

    def __init__(self, client_id, request_number, request_data, server_id):
        super().__init__()
        self.client_id = client_id
        self.request_number = request_number
        self.request_data = request_data
        self.server_id = server_id


    def copy_data_to_response(self):
        response = ClientResponseMessage(self.client_id, self.request_number, self.request_data, self.server_id)
        return response

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, byte_data):
        assert isinstance(byte_data, bytes) or isinstance(byte_data, bytearray), "We can only deserialize a byte array"
        #
        reqMessage = pickle.loads(byte_data)
        if isinstance(reqMessage, ClientRequestMessage): return reqMessage
        else: 
            return None

    def __repr__(self):
        return '{ClientRequestMessage: <C-(%d), S-(%d), req#%d, "%s">}' % (self.client_id, self.server_id, self.request_number, self.request_data)

class ClientResponseMessage(Message):
    '''
    Send from server to client as part of normal request-response flow
    '''

    def __init__(self, client_id, request_number, response_data, server_id):
        super().__init__()
        self.client_id = client_id
        self.request_number = request_number
        self.response_data = response_data
        self.server_id = server_id

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, byte_data):
        assert isinstance(byte_data, bytes) or isinstance(byte_data, bytearray), "We can only deserialize a byte array"
        #
        reqMessage = pickle.loads(byte_data)
        if isinstance(reqMessage, ClientResponseMessage): return reqMessage
        else: 
            return None

    def __repr__(self):
        return '{ClientResponseMessage: <C%d, S%d, %d, "%s">}' % (self.client_id, self.server_id, self.request_number, self.response_data)


class AckMessage(Message):
    '''
    Simple acknowledgement
    '''

    def __init__(self, ack_data=b''):
        super().__init__(data=ack_data)

class LFDMessage(Message):
    '''
    A message for the local fault detector to send to server replicas
    '''
    def __init__(self, data=constants.MAGIC_MSG_LFD_REQUEST):
        super().__init__(data=data)

class GFDClientMessage(Message):
    def __init__(self, server_ip, server_port, sid, is_primary=None, action=constants.GFD_ACTION_NEW):
        '''
        A message from the GFD to client to inform it of a change in the connections

        The server ip and port should point to an address that the client can reach to setup a connection to the server replica with ID 'sid'

        This message should indicate if the server is acting as a primary or not. If the active replication is being used, this should be 'None'

        The action tells the client if the message is indicating a new replica connection was added (GFD_ACTION_NEW), a replica was taken down (GFD_ACTION_DEAD), or if there is some change in the status, e.g. becoming the new primary (GFD_ACTION_UPDATE)
        
        '''
        self.server_ip = server_ip
        self.server_port = server_port
        self.sid = sid
        self.is_primary = is_primary
        self.action = action

class KillThreadMessage():

    '''
    Threads cnanot actually be killed from another thread; they have to be daemonic and exit on automagically when the main thread dies, or return/kill themselves. 


    This message tells the thread to kill itself, but probably close out all resources first. The message is not intended to be sent over a network connection

    This basically just exists so we can check for an instance of something, rather than some magic value or tuple format. Keeping it simple
    '''

    def __init__(self):
        pass



class CheckpointMessage(Message):
    '''
    Send from client to server as part of normal request-response flow
    '''

    def __init__(self, primary_server_id, x, y, z):
        super().__init__()
        self.x = x
        self.y = y
        self.z = z
        self.primary_server_id = primary_server_id


    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        return pickle.dumps(self)

    #? Not sure whether we need this.
    '''
    @classmethod
    def deserialize(cls, byte_data):
        assert isinstance(byte_data, bytes) or isinstance(byte_data, bytearray), "We can only deserialize a byte array"
        #
        reqMessage = pickle.loads(byte_data)
        if isinstance(reqMessage, ClientRequestMessage): return reqMessage
        else: 
            return None
    '''        

    def __repr__(self):
        return 'state_x: %d, state_y: %d, state_z: %d, primary_server_id: %d' % (self.x, self.y, self.z, self.primary_server_id)

# GFD, LFD messages?


