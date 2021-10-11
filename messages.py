'''
A file to describe message formats. 

Each message may have a few parameters, like client ID, request number, etc. It's probably easiest to represent this as an object
Each message should have a predefined format when it's turned into a 'bytes' or 'bytearray' object prior to sending. Ergo, a 'serialize' and 'deserialize' function 
'''

import pickle
import constants

def deserialize(data):
    if not isinstance(data, bytes) and not isinstance(data, bytearray):
        raise ValueError("Can only deserialize bytes or bytearray's")
    return pickle.loads(data)

class Message():

    def __init__(self, data=None):
        self.data = data
        pass

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        if isinstance(self.data, bytes) or isinstance(self.data, bytearray):
            return self.data
        return pickle.dumps(self.data)


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
        return '<ClientRequestMessage: c_ID[%d], s_ID[%d], req #[%d], data: {%s}>' % (self.client_id, self.server_id, self.request_number, self.request_data)

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
        return '<ClientResponseMessage: c_ID[%d], s_ID[%d], req #[%d], data: {%s}>' % (self.client_id, self.server_id, self.request_number, self.response_data)


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

class KillThreadMessage():

    '''
    Threads cnanot actually be killed from another thread; they have to be daemonic and exit on automagically when the main thread dies, or return/kill themselves. 


    This message tells the thread to kill itself, but probably close out all resources first. The message is not intended to be sent over a network connection

    This basically just exists so we can check for an instance of something, rather than some magic value or tuple format. Keeping it simple
    '''

    def __init__(self):
        pass

# GFD, LFD messages?