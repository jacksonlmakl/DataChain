
import socket
import pickle
from threading import Thread
from time import time
import hashlib
import os
from uuid import uuid4
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import msgpack
import socket
import pandas as pd
import msgpack
import datetime
import threading
import json
import msgpack
import sys


def hashmd5(string):
    md5_hash = hashlib.md5()
    md5_hash.update(string.encode('utf-8'))
    hash_md5 = md5_hash.hexdigest()
    return hash_md5


def get_ip():
    """
    Returns the local IP address of the machine.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable, just to get the local network IP
        s.connect(('10.254.254.254', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

# Helper functions for file-based blockchain storage using MessagePack
def save_block_to_disk(block, BASE_DIR):
    """
    Save a block to disk as a MessagePack file.
    Each block is stored in a file named after its block ID.
    """
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
    
    block_filename = os.path.join(BASE_DIR, f"block_{block['id']}.msgpack")
    with open(block_filename, 'wb') as block_file:
        packed_data = msgpack.packb(block)
        block_file.write(packed_data)
        
def load_block_from_disk(block_id, BASE_DIR):
    """
    Load a block from disk based on its block ID using MessagePack.
    """
    block_filename = os.path.join(BASE_DIR, f"block_{block_id}.msgpack")
    if os.path.exists(block_filename):
        with open(block_filename, 'rb') as block_file:
            packed_data = block_file.read()
            return msgpack.unpackb(packed_data, raw=False)
    return None

def load_chain_from_disk(BASE_DIR):
    """
    Load the entire blockchain from disk using MessagePack.
    This function will read the blockchain directory and load blocks in order.
    """
    chain = []
    if os.path.exists(BASE_DIR):
        for block_file in sorted(os.listdir(BASE_DIR)):
            if block_file.startswith("block_") and block_file.endswith(".msgpack"):
                with open(os.path.join(BASE_DIR, block_file), 'rb') as file:
                    packed_data = file.read()
                    block = msgpack.unpackb(packed_data, raw=False)
                    chain.append(block)
    return chain


class Blockchain:
    def __init__(self, port,table_name):
        self.dir = f'./pychain__{table_name}'
        self.chain = load_chain_from_disk(self.dir)  # Load the blockchain from disk on startup
        self.transactions = []
        self.nodes = set()
        self.node_identifier = str(uuid4()).replace('-', '')
        self.port = port
        print(self.node_identifier)
        # If the chain is empty, create the genesis block
        if len(self.chain) == 0:
            self.new_block(previous_hash='1', proof=100, public_key_pem='', key='GENESIS')

    def new_block(self, proof, key, public_key_pem, previous_hash=None):
        block = {
            'id': len(self.chain) + 1,
            'name': key,
            'data': self.transactions,
            'timestamp': time(),
            'proof': proof,
            'previous_hash': previous_hash or self.hash(self.chain[-1]),
            'signer_public_key': public_key_pem
        }

        self.transactions = []
        self.chain.append(block)
        
        # Save the new block to disk using MessagePack
        save_block_to_disk(block, self.dir)

        return block

    def new_transaction(self, transaction, signature, public_key):
        """
        Adds a new transaction if the signature is valid.
        """
        if self.verify_signature(transaction, signature, public_key):
            self.transactions.append(transaction)
            return self.last_block['id'] + 1
        else:
            return None  # Invalid transaction signature

    @staticmethod
    def hash(block):
        block_string = msgpack.packb(block, use_bin_type=True)
        return hashlib.sha256(block_string).hexdigest()

    @property
    def last_block(self):
        return self.chain[-1]

    def proof_of_work(self, last_proof):
        proof = 0
        while self.valid_proof(last_proof, proof) is False:
            proof += 1
        return proof

    @staticmethod
    def valid_proof(last_proof, proof):
        guess = f'{last_proof}{proof}'.encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        return guess_hash[:4] == "0000"

    def valid_chain(self, chain):
        last_block = chain[0]
        current_index = 1

        while current_index < len(chain):
            block = chain[current_index]
            if block['previous_hash'] != self.hash(last_block):
                return False
            if not self.valid_proof(last_block['proof'], block['proof']):
                return False
            last_block = block
            current_index += 1

        return True

    def resolve_conflicts(self):
        """
        Resolves conflicts by applying the longest valid chain in the network.
        Fetches the chain from all peers and applies the longest one if valid.
        """
        new_chain = None
        max_length = len(self.chain)

        for node in self.nodes:
            length, chain = self.get_chain_from_peer(node)
            if chain and length > max_length and self.valid_chain(chain):
                max_length = length
                new_chain = chain

        if new_chain:
            self.chain = new_chain
            # Save the entire chain to disk when conflicts are resolved
            for block in self.chain:
                save_block_to_disk(block, self.dir)
            return True
        return False

    def get_chain_from_peer(self, node):
        """
        Retrieve the blockchain from a peer node, fetching the data in chunks.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(node)
            s.send(b"GET_CHAIN")
            data = b""
            while True:
                part = s.recv(4096)
                if not part:
                    break
                data += part
            
            s.close()

            length, chain = pickle.loads(data)
            return length, chain
        except Exception as e:
            return None, None

    def register_node(self, address):
        self.nodes.add(address)

    def broadcast_block(self, block):
        for node in self.nodes:
            self.send_block_to_peer(node, block)

    def send_block_to_peer(self, node, block):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(node)
            data = pickle.dumps(block)
            s.send(data)
            s.close()
        except Exception as e:
            pass

    def sign_transaction(self, transaction, private_key):
        """
        Sign a transaction with the private key of the sender.
        """
        transaction_string = msgpack.packb(transaction, use_bin_type=True)
        signature = private_key.sign(
            transaction_string,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature

    def verify_signature(self, transaction, signature, public_key):
        """
        Verify the transaction signature using the sender's public key.
        """
        transaction_string = msgpack.packb(transaction, use_bin_type=True)
        try:
            public_key.verify(
                signature,
                transaction_string,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            return False


def handle_client(client_socket, blockchain):
    request = client_socket.recv(4096)
    
    if request == b"GET_CHAIN":
        response = pickle.dumps((len(blockchain.chain), blockchain.chain))
        client_socket.send(response)
    elif request == b"GET_NODES":
        response = pickle.dumps(list(blockchain.nodes))
        client_socket.send(response)
    else:
        try:
            data = pickle.loads(request)
            if isinstance(data, tuple) and len(data) == 2:
                blockchain.register_node(data)
            else:
                blockchain.chain.append(data)
        except Exception as e:
            pass
    
    client_socket.close()


def start_server(blockchain):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', blockchain.port))
    server.listen(5)

    while True:
        client, addr = server.accept()
        client_handler = Thread(target=handle_client, args=(client, blockchain))
        client_handler.start()

class Node:
    def __init__(self, port, table_name, master_node=None):
        self.blockchain = Blockchain(port,table_name)
        self.port = port
        self.table_name = table_name
        self.master_node = master_node

        server_thread = Thread(target=start_server, args=(self.blockchain,))
        server_thread.start()

        if master_node:
            self.auto_register_with_master(master_node)

    def auto_register_with_master(self, master_node):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(master_node)
            s.send(b"GET_NODES")
            response = s.recv(4096)
            s.close()
            nodes = pickle.loads(response)
        except Exception as e:
            return

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(master_node)
            s.send(pickle.dumps((get_ip(), self.port)))  
            s.close()
        except Exception as e:
            return

        for node in nodes:
            if node != (get_ip(), self.port):  
                self.register_node(node)

        self.register_node(master_node)
        self.resolve_conflicts()

    def register_node(self, node_address):
        self.blockchain.register_node(node_address)

    def add_transaction(self, transaction, signature, public_key):
        return self.blockchain.new_transaction(transaction, signature, public_key)

    def mine_block(self, key, public_key):
        last_proof = self.blockchain.last_block['proof']
        proof = self.blockchain.proof_of_work(last_proof)
        block = self.blockchain.new_block(proof=proof, key=key, public_key_pem=public_key)
        self.blockchain.broadcast_block(block)
        self.resolve_conflicts()

    def get_chain(self):
        self.resolve_conflicts()
        return self.blockchain.chain

    def resolve_conflicts(self):
        self.blockchain.resolve_conflicts()


def create_or_load_keys(private_key_str=None, public_key_str=None):
    """
    Create a new RSA key pair or load the keys from provided strings.
    If no strings are provided, generate a new key pair.
    """
    if private_key_str is None or public_key_str is None:
        # Generate new RSA key pair
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_key = private_key.public_key()

        # Convert keys to PEM strings for saving/loading
        private_key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()  # No password for simplicity
        ).decode('utf-8').replace("-----BEGIN RSA PRIVATE KEY-----\n", "").replace("\n-----END RSA PRIVATE KEY-----\n", "")

        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8').replace("-----BEGIN PUBLIC KEY-----\n", "").replace("\n-----END PUBLIC KEY-----\n", "")

        return private_key, public_key, private_key_pem, public_key_pem

    else:
        # Load private key from the provided string
        private_key_str_formatted = "-----BEGIN RSA PRIVATE KEY-----\n" + private_key_str + "\n-----END RSA PRIVATE KEY-----\n"
        private_key = serialization.load_pem_private_key(
            private_key_str_formatted.encode('utf-8'),
            password=None,  # No password in this example
            backend=default_backend()
        )

        # Load public key from the provided string
        public_key_str_formatted = "-----BEGIN PUBLIC KEY-----\n" + public_key_str + "\n-----END PUBLIC KEY-----\n"
        public_key = serialization.load_pem_public_key(
            public_key_str_formatted.encode('utf-8'),
            backend=default_backend()
        )

        return private_key, public_key, private_key_str, public_key_str

# Main class
class Chain:
    def __init__(self, port, table_name, master_node=None, private_key=None, public_key=None):
        if not private_key or not public_key:
            self.private_key, self.public_key, self.private_key_pem, self.public_key_pem = create_or_load_keys()
        else:
            self.private_key, self.public_key, self.private_key_pem, self.public_key_pem = create_or_load_keys(private_key_str=private_key, public_key_str=public_key)

        self.node = Node(port=port, master_node=master_node, table_name=table_name)
        self.chain = self.node.get_chain()
        self.dir = self.node.blockchain.dir

    def block(self, name, data):
        signature = self.node.blockchain.sign_transaction(data, self.private_key)
        self.node.add_transaction(data, signature, self.public_key)
        self.node.mine_block(key=name, public_key=self.public_key_pem)

    def keys(self):
        return {
            "private": self.private_key_pem,
            "public": self.public_key_pem
        }
        

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
    def __init__(self, table_name, connection,port=None):
        self.table_name=table_name
        if not port:
            self.port=find_free_port()
        else:
            self.port=port
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
        all_ssh_keys = [{'ssh_keys':x} for xs in [i.get('data') for i in data[1:] if i.get('data') and i.get('name')=='ssh_keys'] for x in xs]
        
        df=pd.DataFrame(all_ssh_keys)
        df['META_BLOCK_ID'] = pd.Series([i.get('id') for i in data[1:] if i.get('id')] )
        df['META_INSERT_TIMESTAMP'] = pd.Series([datetime.datetime.fromtimestamp(i.get('timestamp')) for i in data[1:] if i.get('timestamp')])
        latest_look = df.loc[df.groupby('META_BLOCK_ID')['META_INSERT_TIMESTAMP'].idxmax()]
        temp=list(latest_look['ssh_keys'])
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
    def __init__(self):
        private_key_str=None
        public_key_str=None
        r=create_or_load_keys(private_key_str=None, public_key_str=None)
        self.keys= {'private':r[2], 'public':r[3]}
