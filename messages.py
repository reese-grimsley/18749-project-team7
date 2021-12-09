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
        d = pickle.dumps(self)
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
        if isinstance(reqMessage, ClientRequestMessage):
            return reqMessage
        else:
            return None

    def __repr__(self):
        return '{ClientRequestMessage: <C-(%d), S-(%d), req#%d, "%s">}' % (
            self.client_id, self.server_id, self.request_number, self.request_data)

    def __eq__(self, other):
        return id(other) == id(self)

    def __lt__(self, other):
        # only necessary to satisify priority queues
        if not isinstance(other, ClientRequestMessage): return False
        return self.request_number < other.request_number


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
        if isinstance(reqMessage, ClientResponseMessage):
            return reqMessage
        else:
            return None

    def __repr__(self):
        return '{ClientResponseMessage: <C%d, S%d, %d, "%s">}' % (
            self.client_id, self.server_id, self.request_number, self.response_data)

    def __eq__(self, other):
        return id(other) == id(self)

    def __lt__(self, other):
        # only necessary to satisify priority queues
        if not isinstance(other, ClientResponseMessage): return False
        return self.request_number < other.request_number


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


class GFDMessage(Message):
    '''
    A message for the global fault detector to send to local fault detectors
    '''

    def __init__(self, data=constants.MAGIC_MSG_GFD_REQUEST):
        super().__init__(data=data)


class PrimaryMessage(Message):
    '''
    A message for the global fault detector to send to LFD
    '''
    '''def __init__(self, primary, backup, action):
        # message would be like: ACTION; PRIMARY; BACKUP1, BACKUP2, ...
        data = action + "; "
        
        for primary_id in primary:
            # Ex: "Primary S1 172.19.137.180 19620" means S1 is primary
            data += constants.MAGIC_MSG_PRIMARY + " " + str(primary_id) + " " + primary[primary_id] + " " + str(constants.DEFAULT_APP_BACKUP_SERVER_PORT)  
        data += "; "
        
        for backup_id in backup:
            # Ex: "Backup S2 172.19.137.181 19620" means S2 is backup
            data += constants.MAGIC_MSG_BACKUP + " " + str(backup_id) + " " + backup[backup_id] + " " + str(constants.DEFAULT_APP_BACKUP_SERVER_PORT)    
            data += ", "
            
        super().__init__(data=data)'''

    def __init__(self, primary, backup, server_action):
        super().__init__()
        self.server_action = server_action
        self.primary = primary
        self.backup = backup

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, byte_data):
        assert isinstance(byte_data, bytes) or isinstance(byte_data, bytearray), "We can only deserialize a byte array"
        #
        req_message = pickle.loads(byte_data)
        if isinstance(req_message, ClientResponseMessage):
            return req_message
        else:
            return None

    def __repr__(self):
        return '{ClientResponseMessage: <server_action-%s, primary-%s, backup-%s>}' % (
            self.server_action, self.primary, self.backup)


class LFDGFDMessage(Message):
    '''
    A message for the local fault detector to send to GFD
    There are three kinds od action:
    
    1. LFD sends heartbeat message to GFD
    2. LFD sends add Server request to GFD
    3. LFD sends delete Server request to GFD
    
    '''

    def __init__(self, lfd_id, action, server_ip):

        if action == constants.LFD_ACTION_HB:
            data = "LFD" + str(lfd_id) + ": " + constants.MAGIC_MSG_LFD_RESPONSE
        elif action == constants.LFD_ACTION_ADD_SERVER:
            data = constants.MAGIC_MSG_SERVER_START + " at S" + str(lfd_id) + " : " + str(server_ip)
        elif action == constants.LFD_ACTION_RM_SERVER:
            data = constants.MAGIC_MSG_SERVER_FAIL + " at S" + str(lfd_id) + " : " + str(server_ip)

        super().__init__(data=data)


class GFDClientMessage(Message):
    def __init__(self, server_ip, sid, is_primary=None, action=constants.GFD_ACTION_NEW):
        '''
        A message from the GFD to client to inform it of a change in the connections

        The server ip and port should point to an address that the client can reach to setup a connection to the server replica with ID 'sid'

        This message should indicate if the server is acting as a primary or not. If the active replication is being used, this should be 'None'

        The action tells the client if the message is indicating a new replica connection was added (GFD_ACTION_NEW), a replica was taken down (GFD_ACTION_DEAD), or if there is some change in the status, e.g. becoming the new primary (GFD_ACTION_UPDATE)
        
        '''
        # super().__init__()
        self.server_ip = server_ip
        # self.server_port = server_port
        self.sid = sid
        self.is_primary = is_primary
        self.action = action
        data = "S" + str(sid) + ": " + str(server_ip) + ", is primary: " + str(is_primary)
        super().__init__(data=data)


class KillThreadMessage():
    '''
    Threads cnanot actually be killed from another thread; they have to be daemonic and exit on automagically when the main thread dies, or return/kill themselves. 


    This message tells the thread to kill itself, but probably close out all resources first. The message is not intended to be sent over a network connection

    This basically just exists so we can check for an instance of something, rather than some magic value or tuple format. Keeping it simple
    '''

    def __init__(self):
        pass

    def __eq__(self, other):
        return id(self) == id(other)

    def __lt__(self, other):
        return True


class CheckpointMessage(Message):
    '''
    Send from client to server as part of normal request-response flow
    '''

    def __init__(self, x, y, z, primary_server_id, checkpoint_num):
        super().__init__()
        self.x = x
        self.y = y
        self.z = z
        self.primary_server_id = primary_server_id
        self.checkpoint_num = checkpoint_num
