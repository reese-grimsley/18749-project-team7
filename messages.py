'''
A file to describe message formats. 

Each message may have a few parameters, like client ID, request number, etc. It's probably easiest to represent this as an object
Each message should have a predefined format when it's turned into a 'bytes' or 'bytearray' object prior to sending. Ergo, a 'serialize' and 'deserialize' function 
'''

import pickle

class Message():

    def __init__(self, data=b''):
        self.data = data
        pass

    def serialize(self):
        '''
        return bytes or bytearray that can be directly send through a socket
        '''
        if isinstance(self.data, bytes) or isinstance(self.data, bytearray):
            return self.data
        return pickle.dumps(self.data)

    @classmethod
    def deserialize(byte_data):
        assert isinstance(byte_data, bytes) or isinstance(byte_data, bytearray), "We can only deserialize a byte array"
        #
        return Message(data=pickle.loads(byte_data))


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

class AckMessage(Message):
    '''
    Simple acknowledgement
    '''

    def __init__(self, ack_data=b''):
        super().__init__()
        self.ack_data = ack_data

# GFD, LFD messages?