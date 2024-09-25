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
            set_keys=False
        else:
            set_keys=True
            self.chain=Chain(self.port,self.table_name,None,self.private_key,self.public_key)
        self.private_key=self.chain.private_key_pem
        self.public_key=self.chain.public_key_pem
        self.dir=self.chain.dir

        if set_keys:
            self.set_ssh_keys([self.public_key])
        if not set_keys:
            if self.public_key not in self.get_ssh_keys():
                self.chain.block("event__invalid_auth",{'public_key':self.public_key})
                raise Exception("Your SSH key is not authorized, this incident has been recorded.")
    def select(self):
        data=self.chain.chain
        unnest = [x for xs in [i.get('data') for i in data if i.get('data') and i.get('name')=='row'] for x in xs]
        
        conformed = [i for i in unnest if type(i) == dict]
        df=pd.DataFrame(conformed)
        df['META_BLOCK_ID'] = pd.Series([i.get('id') for i in data if i.get('data') and i.get('name')=='row'] )
        df['META_INSERT_TIMESTAMP'] = pd.Series([datetime.datetime.fromtimestamp(i.get('timestamp')) for i in data if i.get('data') and i.get('name')=='row' and i.get('timestamp')] )
        return df
    def insert(self,row):
        if type(row) != dict:
            raise Exception("row argument must be type dict.")
        self.chain.block("row",row)
    def set_ssh_keys(self,keys):
        if type(keys) != list:
            raise Exception("Keys must be a list of strings.")
        for i in keys:
            if type(i) !=str:
                raise Exception("Key must be type string.")
        self.chain.block("ssh_keys",keys)
    def get_ssh_keys(self):
        data=self.chain.chain
        all_ssh_keys = [{'ssh_key':x} for xs in [i.get('data') for i in data[1:] if i.get('data') and i.get('name')=='ssh_keys'] for x in xs]
        
        df=pd.DataFrame(all_ssh_keys)
        df['META_BLOCK_ID'] = pd.Series([i.get('id') for i in data[1:] if i.get('id')] )
        df['META_INSERT_TIMESTAMP'] = pd.Series([datetime.datetime.fromtimestamp(i.get('timestamp')) for i in data[1:] if i.get('timestamp')])
        latest_look = df.loc[df.groupby('META_BLOCK_ID')['META_INSERT_TIMESTAMP'].idxmax()]
        temp=list(latest_look['ssh_key'])
        allowed_keys=[ x for xs in temp for x in xs]
        return allowed_keys
