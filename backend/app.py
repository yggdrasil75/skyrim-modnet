import os
import socket
import sys
import uuid
import hashlib
import random
import requests
import threading
import time
from config import NetworkConfig, NodeConfig
from flask import Flask, request, jsonify, render_template

# --- Configuration & State ---

app = Flask(__name__, static_folder="../frontend", template_folder="../templates")

# Each node needs a unique ID and an address
NODE_ID = str(uuid.uuid4())
NODE_HOST = '127.0.0.1'
NODE_PORT = 5000 # Default, will be overridden by command line args
Config = NodeConfig(NODE_ID)
netConfig = NetworkConfig()

# A simple dictionary to store the addresses of other nodes (peers)
# Format: { 'node_id': 'http://host:port' }
PEERS = {}

# The path where this node stores its data chunks
DATA_STORE_PATH = f"./data_{NODE_PORT}"
if not os.path.exists(DATA_STORE_PATH):
    os.makedirs(DATA_STORE_PATH)

# --- Helper Functions ---

def get_data_key(content):
    """Generates a SHA256 hash to use as the data's unique key."""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def save_data_local(key, content):
    """Saves a piece of data to the node's local storage."""
    with open(os.path.join(DATA_STORE_PATH, key), 'w') as f:
        f.write(content)
    print(f"[{NODE_PORT}] Stored data with key: {key}")

def load_data_local(key):
    """Loads a piece of data from the node's local storage if it exists."""
    filepath = os.path.join(DATA_STORE_PATH, key)
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return f.read()
    return None

def peer_discovery_loop():
    """Periodically discover new peers using UDP hole punching"""
    while True:
        time.sleep(60)
        
        # Try UDP hole punching with known peers
        for peer_id, peer_address in list(PEERS.items()):
            host, port = peer_address.split(':')
            try:
                # Send UDP packet to trigger hole punching
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(b"HOLE_PUNCH", (host, int(port)))
            except Exception as e:
                print(f"UDP hole punching failed with {peer_id}: {e}")
                
        # Also maintain TCP fallback discovery
        current_peers = list(Config.config['peers'].items())
        if current_peers:
            peer_id, peer_address = random.choice(current_peers)
            try:
                response = requests.get(f"http://{peer_address}/peers", timeout=5)
                if response.status_code == 200:
                    new_peers = response.json()
                    for new_peer_id, new_peer_address in new_peers.items():
                        if (new_peer_id != NODE_ID and 
                            new_peer_id not in Config.config['peers'] and 
                            new_peer_id not in Config.config['blacklist']):
                            Config.add_peer(new_peer_id, new_peer_address)
            except requests.exceptions.RequestException:
                Config.update_peer_stats(peer_id, success=False)

def peer_health_check_loop():
    """Periodically check peer health and update stats"""
    while True:
        time.sleep(30)  # Run every 30 seconds
        
        for peer_id, peer_address in list(Config.config['peers'].items()):
            try:
                start_time = time.time()
                response = requests.get(f"{peer_address}/status", timeout=3)
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    Config.update_peer_stats(peer_id, response_time, success=True)
                else:
                    Config.update_peer_stats(peer_id, success=False)
            except requests.exceptions.RequestException:
                Config.update_peer_stats(peer_id, success=False)
                # If peer fails too many times, blacklist it
                if Config.config['peer_stats'][peer_id]['fail_count'] > 3:
                    Config.add_to_blacklist(peer_id)
                    print(f"[{NODE_PORT}] Blacklisted unresponsive peer: {peer_id}")

def send_message(peer_id, message, use_udp=True):
    """Send message to peer, trying UDP first then falling back to TCP"""
    peer_address = PEERS.get(peer_id)
    if not peer_address:
        return False
    
    if use_udp:
        try:
            # Try UDP first
            host, port = peer_address.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(message.encode(), (host, int(port)))
                return True
        except Exception as e:
            print(f"UDP send failed, falling back to TCP: {e}")
    
    # Fall back to TCP
    try:
        response = requests.post(f"http://{peer_address}/api", 
                               data=message, 
                               timeout=3)
        return response.status_code == 200
    except Exception as e:
        print(f"TCP send failed: {e}")
        return False

def udp_server_thread():
    """Handle incoming UDP messages"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', netConfig.local_udp_port))
    
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            threading.Thread(target=handle_udp_message, args=(data, addr)).start()
        except Exception as e:
            print(f"UDP server error: {e}")



def handle_peer_discovery(addr):
    """Handle incoming peer discovery message"""
    try:
        peer_host, peer_port = addr
        peer_address = f"{peer_host}:{peer_port}"
        
        # Check if we already know this peer
        known_peer = None
        for peer_id, known_addr in PEERS.items():
            if known_addr == peer_address:
                known_peer = peer_id
                break
        
        if known_peer:
            # Update last seen time
            Config.update_peer_stats(known_peer, success=True)
        else:
            # Register new peer
            peer_id = str(uuid.uuid4())  # In real implementation, peer would send its ID
            PEERS[peer_id] = peer_address
            Config.add_peer(peer_id, peer_address)
            print(f"[{NODE_PORT}] Discovered new peer via UDP: {peer_id} at {peer_address}")
            
    except Exception as e:
        print(f"Error handling peer discovery: {e}")

def handle_data_transfer(message, addr):
    """Handle incoming data transfer message"""
    try:
        parts = message.split('|')
        if len(parts) < 3:
            return
            
        command = parts[0]
        key = parts[1]
        content = '|'.join(parts[2:])  # Rejoin in case content contained pipes
        
        if command == "DATA_TRANSFER":
            # Save the received data
            save_data_local(key, content)
            print(f"[{NODE_PORT}] Received data via UDP: {key}")
            
        elif command == "REQUEST_DATA":
            # Check if we have the requested data
            local_content = load_data_local(key)
            if local_content:
                # Send response back to requester
                peer_host, peer_port = addr
                peer_address = f"{peer_host}:{peer_port}"
                send_data_udp(peer_address, f"DATA_RESPONSE|{key}|{local_content}", "")
                
    except Exception as e:
        print(f"Error handling data transfer: {e}")

# Update the send_message function to handle address formats consistently
def send_message(peer_id, message, use_udp=True):
    """Send message to peer, trying UDP first then falling back to TCP"""
    peer_address = PEERS.get(peer_id)
    if not peer_address:
        return False
    
    # Normalize address (remove http:// if present)
    if peer_address.startswith('http://'):
        peer_address = peer_address[7:]
    
    if use_udp:
        try:
            # Try UDP first
            host, port = peer_address.split(':')
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.sendto(message.encode(), (host, int(port)))
                return True
        except Exception as e:
            print(f"UDP send failed, falling back to TCP: {e}")
    
    # Fall back to TCP
    try:
        response = requests.post(f"http://{peer_address}/api", 
                               data=message, 
                               timeout=3)
        return response.status_code == 200
    except Exception as e:
        print(f"TCP send failed: {e}")
        return False

# Update the find_data_in_network function to properly handle UDP requests
def find_data_in_network(key):
    """Modified to try UDP first, then TCP"""
    # 1. Check local storage
    content = load_data_local(key)
    if content:
        return content

    # 2. Try UDP with prioritized peers
    peers_to_ask = Config.get_best_peers(3)
    
    for peer_id, peer_address in peers_to_ask:
        # Normalize address
        if peer_address.startswith('http://'):
            peer_address = peer_address[7:]
            
        # Send request
        if send_data_udp(peer_address, f"REQUEST_DATA|{key}", ""):
            # Wait briefly for response (simplified - in real impl would use async)
            time.sleep(0.5)
            # Check if we got the data while waiting
            content = load_data_local(key)
            if content:
                return content
    
    # 3. Fall back to TCP
    for peer_id, peer_address in peers_to_ask:
        try:
            response = requests.get(f"http://{peer_address}/data/{key}", timeout=3)
            if response.status_code == 200:
                return response.text
        except requests.exceptions.RequestException:
            continue
    
    return None

def handle_udp_message(data, addr):
    """Process incoming UDP message"""
    try:
        message = data.decode()
        # Process different message types (peer discovery, data transfer, etc.)
        if message.startswith("PEER_DISCOVERY"):
            handle_peer_discovery(addr)
        elif message.startswith("DATA_TRANSFER"):
            handle_data_transfer(message, addr)
        # Add other message types as needed
    except Exception as e:
        print(f"Error processing UDP message: {e}")

def send_data_udp(peer_address, key, content):
    """Send data via UDP if possible"""
    try:
        host, port = peer_address.split(':')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Simple protocol: <COMMAND>|<KEY>|<DATA>
            message = f"DATA_TRANSFER|{key}|{content}"
            sock.sendto(message.encode(), (host, int(port)))
            return True
    except Exception as e:
        print(f"UDP data transfer failed: {e}")
        return False


# --- Web UI Endpoints ---

@app.route('/', methods=['GET'])
def index():
    """
    Renders the primary index page with a search form.
    Also handles the search query submitted via the form.
    The term "mods" is used in the template as requested, referring to data.
    """
    search_key = request.args.get('key', '').strip()
    found_content = None

    if search_key:
        found_content = find_data_in_network(search_key)

    return render_template('index.html',
                           node_id=NODE_ID,
                           port=NODE_PORT,
                           peer_count=len(PEERS),
                           search_key=search_key,
                           data_content=found_content)

# --- API Endpoints ---
@app.route('/peers/friends/add', methods=['POST'])
def add_friend():
    """Add a peer to friends list"""
    data = request.get_json()
    peer_id = data.get('peer_id')
    if peer_id in Config.config['peers']:
        Config.add_friend(peer_id)
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Peer not found"}), 404

@app.route('/peers/blacklist/add', methods=['POST'])
def add_to_blacklist():
    """Add a peer to blacklist"""
    data = request.get_json()
    peer_id = data.get('peer_id')
    Config.add_to_blacklist(peer_id)
    return jsonify({"status": "success"})

@app.route('/peers/stats', methods=['GET'])
def get_peer_stats():
    """Get peer statistics"""
    return jsonify(Config.config['peer_stats'])

@app.route('/status')
def status():
    """A simple JSON status endpoint for the node."""
    return jsonify({
        "message": "This node is active.",
        "node_id": NODE_ID,
        "port": NODE_PORT,
        "known_peers": len(PEERS)
    })

@app.route('/peers', methods=['GET'])
def get_peers():
    """Returns the list of peers known to this node."""
    return jsonify(PEERS)

@app.route('/peers/register', methods=['POST'])
def register_peer():
    """
    The endpoint for a new node to join the network.
    A new node announces itself to an existing node.
    """
    data = request.get_json()
    peer_id = data.get('node_id')
    peer_address = data.get('address')

    if not peer_id or not peer_address:
        return "Invalid data", 400

    if peer_id == NODE_ID: # Don't add self
        return "Cannot register self", 200

    # Prepare response for the new peer BEFORE we modify our PEERS list
    # This ensures the new peer gets our current list, and we get theirs
    response_for_new_peer = {
        "message": "Peer registered successfully.",
        "known_peers": PEERS.copy()
    }
    # Also add the registering node itself to the list it receives back
    response_for_new_peer['known_peers'][NODE_ID] = f"http://{NODE_HOST}:{NODE_PORT}"

    # Add the new peer
    PEERS[peer_id] = peer_address
    print(f"[{NODE_PORT}] Registered new peer: {peer_id} at {peer_address}")

    # --- Announce the new peer to our existing peers (Network Propagation) ---
    # We announce the new peer to our old list of peers
    for existing_peer_id, existing_peer_address in response_for_new_peer['known_peers'].items():
        if existing_peer_id != NODE_ID: # Don't announce to self
            try:
                # Tell our other peers about the new node that just joined
                print(f"[{NODE_PORT}] Announcing new peer {peer_id} to {existing_peer_address}")
                requests.post(f"{existing_peer_address}/peers/announce", json={'node_id': peer_id, 'address': peer_address})
            except requests.exceptions.ConnectionError:
                print(f"[{NODE_PORT}] Could not connect to peer {existing_peer_address} to announce.")

    return jsonify(response_for_new_peer)

@app.route('/peers/announce', methods=['POST'])
def announce_peer():
    """An internal endpoint for peers to announce other peers they discovered."""
    data = request.get_json()
    peer_id = data.get('node_id')
    peer_address = data.get('address')

    if not peer_id or not peer_address:
        return "Invalid data", 400

    if peer_id not in PEERS and peer_id != NODE_ID:
        PEERS[peer_id] = peer_address
        print(f"[{NODE_PORT}] Learned about peer {peer_id} from another node.")

    return "OK", 200

@app.route('/data', methods=['POST'])
def add_data():
    """
    Adds a new piece of data to the network.
    The node receiving the request will store it and replicate it.
    """
    content = request.get_data(as_text=True)
    if not content:
        return "No data provided", 400

    key = get_data_key(content)
    
    # 1. Store the data locally
    save_data_local(key, content)

    # 2. Replicate the data to a subset of peers (e.g., 2)
    replication_count = 2
    if len(PEERS) > 0:
        peers_to_replicate = random.sample(list(PEERS.values()), min(len(PEERS), replication_count))
        for peer_address in peers_to_replicate:
            try:
                print(f"[{NODE_PORT}] Replicating data {key} to {peer_address}")
                requests.post(f"{peer_address}/data/replicate", json={'key': key, 'content': content}, timeout=3)
            except requests.exceptions.RequestException:
                print(f"[{NODE_PORT}] Failed to replicate data to {peer_address}")
                # In a real system, you might queue this for retry

    return jsonify({"message": "Data stored successfully", "key": key}), 201

@app.route('/data/replicate', methods=['POST'])
def replicate_data():
    """An internal endpoint for a node to receive a replica from another node."""
    data = request.get_json()
    key = data.get('key')
    content = data.get('content')
    save_data_local(key, content)
    return "Replica stored", 200


@app.route('/data/<key>', methods=['GET'])
def get_data(key):
    """
    API endpoint to retrieve data. Checks locally, then asks peers.
    Uses the refactored find_data_in_network function.
    """
    content = find_data_in_network(key)
    if content:
        return content, 200
    else:
        return "Data not found in the network", 404


# --- Main Execution ---

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python node.py <port> [bootstrap_address]")

    NODE_PORT = int(sys.argv[1])
    
    # Determine NAT type and public address
    netConfig.nat_type = netConfig.get_nat_type()
    print(f"NAT Type: {netConfig.nat_type}")
    
    # Start UDP server
    threading.Thread(target=udp_server_thread, daemon=True).start()
    
    # Start TCP server (Flask)
    threading.Thread(target=lambda: app.run(
        host=NODE_HOST, 
        port=NODE_PORT,
        threaded=True
    ), daemon=True).start()
    
    # Bootstrap if needed
    if len(sys.argv) > 2:
        bootstrap_address = sys.argv[2]
        print(f"Attempting NAT traversal with bootstrap node...")
        
        # Try UDP first
        if netConfig.public_udp_addr:
            host, port = bootstrap_address.split(':')
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(b"REGISTER_UDP", (host, int(port)))
            except Exception as e:
                print(f"UDP bootstrap failed: {e}")
        
        # Fall back to TCP
        try:
            my_address = f"{NODE_HOST}:{NODE_PORT}"
            response = requests.post(f"http://{bootstrap_address}/peers/register", 
                                   json={'node_id': NODE_ID, 'address': my_address},
                                   timeout=5)
            if response.status_code == 200:
                known_peers_data = response.json().get('known_peers', {})
                PEERS.update(known_peers_data)
        except requests.exceptions.RequestException as e:
            print(f"TCP bootstrap failed: {e}")

    # Start maintenance threads
    threading.Thread(target=peer_discovery_loop, daemon=True).start()
    threading.Thread(target=peer_health_check_loop, daemon=True).start()
    
    # Keep main thread alive
    while True:
        time.sleep(1)