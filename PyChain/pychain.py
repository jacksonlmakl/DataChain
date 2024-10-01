from blockchain import Chain,create_or_load_keys
import socket
import pandas as pd
import msgpack
import datetime
import threading
from blockchain import get_ip
import json
import msgpack
import sys

def is_serializable(value):
    try:
        # Try to serialize using msgpack
        msgpack.packb(value)
        return True
    except (TypeError, ValueError):
        return False


def exec_python(code):
    _locals={}
    _globals={}
    exec(code,_globals,local_locals)
    return { i:_locals[i] for i in _locals if is_serializable(_locals[i]) }
    
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


class PyChain:
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
            try:
                self.get_ssh_keys()
            except:
                self.set_ssh_keys([self.public_key])
        if not set_keys:
            if self.public_key not in self.get_ssh_keys():
                raise Exception("Your SSH key is not authorized.")
        self.allowed_ssh_keys = self.get_ssh_keys()
    
        self.executor = threading.Thread(target=self.exec_reciever)
        # Start the thread
        self.executor.start()
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
    def execute(self,code,host,port):
        if type(code) != str:
            raise Exception("row argument must be type string.")
        data={
            "code":code,
            "host":host,
            "port":port
        }
        self.chain.block("event__python",data)
        prev_chain_length=len(self.chain.chain)
        while True:
            chain=self.chain.chain
            chain_length=len(chain)
            if chain_length==prev_chain_length:
                prev_chain_length=chain_length
                continue
            python_results=[x for x in [i for i in chain if i.get('name')=='event__python_result'] if x]
            python_events_data=[i for i in self.chain.chain[1:] if i.get('name')=='event__python']
            current_block_data=[i.get('id') for i in python_events_data if i.get('data',[{}])[0].get('code')==code and i.get('data',[{}])[0].get('port')==port and i.get('data',[{}])[0].get('host')==host]
            current_block_id = current_block_data[0] if len(current_block_data)>0 else None
            if current_block_id:
                result=[i.get('data')[0]['result'] for i in python_results if i.get('data')[0]['block_id']==current_block_id]
                return result
            else:
                raise Exception("Error")
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
    def exec_reciever(self):
        prev_chain_length=len(self.chain.chain)
        while True:
            chain=self.chain.chain
            chain_length=len(chain)
            if chain_length==prev_chain_length:
                prev_chain_length=chain_length
                continue
            python_events= [x for x in [i for i in chain if i.get('name')=='event__python'] if x]
            # python_events_ids=[i.get('id') for i in chain if i.get('name')=='event__python']
            python_results=[x for x in [i for i in chain if i.get('name')=='event__python_result'] if x]
            python_results_ids= [x for x in [i.get('data',[{}])[0].get('block_id') for i in chain if i.get('name')=='event__python_result'] if x]
            
            python_events_not_run=[i for i in python_events if i.get('id') not in python_results_ids]
            current_ip=get_ip()
            current_port=self.chain.node.port
            for event in python_events_not_run:
                code=event.get('data')[0].get('code')
                port=event.get('data')[0].get('port')
                host=event.get('data')[0].get('host')
            
                
                
                if current_ip==host and current_port==port:
                    chain_data=self.chain.chain
                    sys.setrecursionlimit(10000)
                    # r=exec_python(code)
                    _globals={}
                    _locals={'chain': self.select()}
                    try:
                        exec(code,_globals,_locals)
                    except Exception as e_msg:
                        _locals={"error_msg": str(e_msg)}
                    r={ i:_locals[i] for i in _locals if is_serializable(_locals[i]) }
                    # In the resolve_conflicts method
                    result_data = {
                        "block_id": event.get('id'),
                        "port": port,
                        "host": host,
                        "result": r # This returns a dictionary of local variables
                    }
                    print(result_data) 
                    print(f"Current IP: {current_ip}\n Current Port: {current_port}")
                    print('\n')
                    if event.get('data')[0]:
                        # signature = self.blockchain.sign_transaction(data, self.private_key)
                        self.chain.block("event__python_result",result_data)
                        
    def daemon(self):
        print("Enter your Python code (press Enter twice to execute):")
        while True:
            print(">>")
            user_code = ""
            
            while True:
                line = input()
                if line == "":
                    break
                user_code += line + "\n"
        
            try:
                exec(user_code)
            except Exception as e:
                print(f"An error occurred: {e}")


class SSHKey:
    def __init__():
        private_key_str=None
        public_key_str=None
        r=create_or_load_keys(private_key_str=None, public_key_str=None)
        self.keys= {'private':r[2], 'public':r[3]}
