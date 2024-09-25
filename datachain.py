from blockchain import Chain
import socket
import pandas as pd
import msgpack
import datetime

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))  # Bind to any free port available on the machine
        return s.getsockname()[1]  # Get the port number assigned

class Connection:
    def __init__(self,host=None,host_port=None,private_key=None,public_key=None):
        self.private_key=private_key
        self.public_key=public_key
        self.host=host
        self.host_port=host_port


class DataChain:
    def __init__(self, table_name, connection):
        self.table_name=table_name
        self.port=find_free_port()
        self.private_key=connection.private_key
        self.public_key=connection.public_key
        self.host_port=connection.host_port
        self.host=connection.host

        if self.host and self.host_port:
            self.chain=Chain(self.port,self.table_name,(self.host,self.host_port),self.private_key,self.public_key)
        else:
            self.chain=Chain(self.port,self.table_name,None,self.private_key,self.public_key)
        self.private_key=self.chain.private_key_pem
        self.public_key=self.chain.public_key_pem
        self.dir=self.chain.dir
        
    def select(self):
        data=self.chain.chain
        unnest = [x for xs in [i.get('data') for i in data[1:] if i.get('data')] for x in xs]
        
        conformed = [i for i in unnest if type(i) == dict]
        df=pd.DataFrame(conformed)
        df['META_BLOCK_ID'] = pd.Series([i.get('id') for i in data[1:] if i.get('id')] )
        df['META_INSERT_TIMESTAMP'] = pd.Series([datetime.datetime.fromtimestamp(i.get('timestamp')) for i in data[1:] if i.get('timestamp')] )
        return df
    def insert(self,row):
        if type(row) != dict:
            raise Exception("row argument must be type dict.")
        self.chain.block("row",row)
