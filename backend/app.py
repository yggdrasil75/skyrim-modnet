import os
import hashlib
import requests
import threading
import time
import sqlite3
from datetime import datetime
from flask import Flask, Response, render_template, request, redirect, url_for, send_file, jsonify, g, current_app
from werkzeug.utils import secure_filename
import socket
import time
import struct
import json

udpon = False

app = Flask(__name__, template_folder="../templates")

app.config['PEER_TYPES'] = {
    'friends': set(),      # Prioritized connections
    'peers': set(),        # Standard semi-permanent connections
    'strangers': set(),    # Limited connections
    'enemies': set()       # Blocked connections
}

# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024  # 16MB max
app.config['CHUNK_SIZE'] = 256 * 1024  # 256KB chunks
app.config['ALLOWED_EXTENSIONS'] = {'zip', 'rar', '7z', 'mod', 'jar'}
app.config['PEERS'] = set()  # Set to store peer addresses
app.config['DEFAULT_PEERS'] = {'http://www.themoddingtree.com:5000'}  # Default peers

def get_persistent_node_id():
    """Generate a persistent, cryptographically secure node ID that's 32 bytes long"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    node_id_file = os.path.join(app.config['UPLOAD_FOLDER'], 'node_id')
    
    try:
        with open(node_id_file, 'rb') as f:
            node_id = f.read()
            # Verify the node ID is the correct length (32 bytes)
            if len(node_id) == 32:
                return node_id
    except FileNotFoundError:
        pass
    
    # Generate a new node ID if none exists or it's invalid
    node_id = os.urandom(32)
    with open(node_id_file, 'wb') as f:
        f.write(node_id)
    
    return node_id

app.config['NODE_ID'] = get_persistent_node_id()
app.config['NODE_ID_HEX'] = app.config['NODE_ID'].hex()  # Hex representation for display
app.config['MAINTENANCE_INTERVAL'] = 60  # Seconds between maintenance checks
app.config['DATABASE'] = './database.db'

# Add to configuration section (after existing config)
app.config['NAT_TRAVERSAL_PORT'] = 42441  # Secondary port for hole punching
app.config['HOLE_PUNCHING_TIMEOUT'] = 5  # Seconds to wait for hole punching
app.config['STUN_SERVERS'] = [
    'stun.l.google.com:19302',
    'stun1.l.google.com:19302',
    'stun2.l.google.com:19302',
    'stun3.l.google.com:19302',
    'stun4.l.google.com:19302',
    'stun.stunprotocol.org:3478',
    'stun.voip.blackberry.com:3478',
    'stun.iptel.org:3478',
    'stun.ideasip.com:3478'
]

def download_chunk_with_relay(chunk_hash, file_hash=None):
    """Download a chunk using public node as relay if direct connection fails"""
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    
    # Try local chunk first
    if os.path.exists(chunk_path):
        with open(chunk_path, 'rb') as f:
            return f.read()
    
    # Try direct download from peers in priority order
    for peer_type in ['friends', 'peers', 'strangers']:
        for peer in current_app.config['PEER_TYPES'][peer_type]:
            try:
                response = requests.get(f"{peer}/chunk/{chunk_hash}", timeout=5)
                if response.status_code == 200:
                    chunk_data = response.content
                    with open(chunk_path, 'wb') as f:
                        f.write(chunk_data)
                    return chunk_data
            except requests.exceptions.RequestException:
                continue
    
    # If direct download fails, use public node as relay to find the chunk
    for public_peer in current_app.config['DEFAULT_PEERS']:
        try:
            # Ask public node to find which peers have this chunk
            response = requests.post(
                f"{public_peer}/find_chunk",
                json={'chunk_hash': chunk_hash},
                timeout=5
            )
            
            if response.status_code == 200:
                chunk_hosts = response.json().get('hosts', [])
                
                # Try each host that has the chunk
                for host_peer in chunk_hosts:
                    try:
                        # Use public node as relay for the request
                        relay_response = requests.get(
                            f"{public_peer}/relay_chunk/{chunk_hash}",
                            params={'source_peer': host_peer},
                            timeout=10
                        )
                        
                        if relay_response.status_code == 200:
                            chunk_data = relay_response.content
                            with open(chunk_path, 'wb') as f:
                                f.write(chunk_data)
                            return chunk_data
                    except requests.exceptions.RequestException:
                        continue
        except requests.exceptions.RequestException:
            continue
    
    raise FileNotFoundError(f"Missing chunk {chunk_hash}")

# Add this function to handle chunk downloads with hole punching fallback
def download_chunk_with_fallback(chunk_hash, file_hash=None):
    """Download a chunk with hole punching fallback"""
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    
    # Try local chunk first
    if os.path.exists(chunk_path):
        with open(chunk_path, 'rb') as f:
            return f.read()
    
    # Try to fetch chunk from peers - friends first, then peers, then strangers
    for peer_type in ['friends', 'peers', 'strangers']:
        for peer in current_app.config['PEER_TYPES'][peer_type]:
            try:
                # First try direct HTTP download
                response = requests.get(f"{peer}/chunk/{chunk_hash}", timeout=5)
                if response.status_code == 200:
                    chunk_data = response.content
                    # Save chunk locally for future requests
                    with open(chunk_path, 'wb') as f:
                        f.write(chunk_data)
                    return chunk_data
            except requests.exceptions.RequestException:
                # HTTP failed, try hole punching
                try:
                    if coordinate_hole_punching(peer, file_hash, chunk_hash):
                        # If hole punching succeeded, check if chunk is now available locally
                        if os.path.exists(chunk_path):
                            with open(chunk_path, 'rb') as f:
                                return f.read()
                except Exception as e:
                    print(f"Hole punching failed for {peer}: {e}")
                    continue
    
    raise FileNotFoundError(f"Missing chunk {chunk_hash}")

# Add an endpoint to check if we have a specific chunk
@app.route('/has_chunk/<chunk_hash>')
def has_chunk(chunk_hash):
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    if os.path.exists(chunk_path):
        return jsonify({'has_chunk': True})
    
    # Also check database
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        'SELECT 1 FROM chunks WHERE chunk_hash = ?',
        (chunk_hash,)
    )
    if cursor.fetchone():
        return jsonify({'has_chunk': True})
    
    return jsonify({'has_chunk': False})

@app.route('/find_chunk', methods=['POST'])
def find_chunk():
    """Find which peers have a specific chunk"""
    data = request.get_json()
    chunk_hash = data.get('chunk_hash')
    
    if not chunk_hash:
        return jsonify({'error': 'Missing chunk_hash'}), 400
    
    hosts = []
    
    # Check all known peers (friends first, then peers, then strangers)
    for peer_type in ['friends', 'peers', 'strangers']:
        for peer in current_app.config['PEER_TYPES'][peer_type]:
            try:
                response = requests.get(
                    f"{peer}/has_chunk/{chunk_hash}",
                    timeout=3
                )
                if response.status_code == 200 and response.json().get('has_chunk'):
                    hosts.append(peer)
            except requests.exceptions.RequestException:
                continue
    
    return jsonify({'chunk_hash': chunk_hash, 'hosts': hosts})

@app.route('/relay_chunk/<chunk_hash>')
def relay_chunk(chunk_hash):
    """Relay a chunk request from one peer to another"""
    source_peer = request.args.get('source_peer')
    
    if not source_peer:
        return jsonify({'error': 'Missing source_peer parameter'}), 400
    
    try:
        # Forward the request to the source peer
        response = requests.get(
            f"{source_peer}/chunk/{chunk_hash}",
            timeout=10,
            stream=True
        )
        
        if response.status_code == 200:
            # Stream the response back to the requester
            def generate():
                for chunk in response.iter_content(chunk_size=8192):
                    yield chunk
            
            return Response(generate(), mimetype='application/octet-stream')
        else:
            return jsonify({'error': 'Chunk not found on source peer'}), 404
            
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Relay failed: {str(e)}'}), 500
    
# Add this function to get public IP and port mapping
def get_public_endpoint(stun_servers=None):
    """Get public IP and port using STUN protocol"""
    
    if stun_servers is None:
        stun_servers = app.config['STUN_SERVERS'][0]
    
    for stun_server in stun_servers:
        try:
            server, port = stun_server.split(':')
            port = int(port)
            
            # STUN binding request
            stun_binding_request = bytes([
                0x00, 0x01, 0x00, 0x00,  # STUN method: Binding, length: 0
                0x21, 0x12, 0xA4, 0x42   # Magic cookie
            ]) + os.urandom(12)  # Transaction ID
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(2)
            
            sock.sendto(stun_binding_request, (server, port))
            response, addr = sock.recvfrom(1024)
            
            # Parse STUN response
            if len(response) >= 20:
                # Look for XOR-MAPPED-ADDRESS attribute (0x0020)
                pos = 20
                while pos + 4 <= len(response):
                    attr_type = struct.unpack('!H', response[pos:pos+2])[0]
                    attr_length = struct.unpack('!H', response[pos+2:pos+4])[0]
                    
                    if attr_type == 0x0020 and pos + 4 + attr_length <= len(response):
                        family = struct.unpack('!B', response[pos+5:pos+6])[0]
                        if family == 0x01:  # IPv4
                            xport = struct.unpack('!H', response[pos+6:pos+8])[0]
                            xip = struct.unpack('!I', response[pos+8:pos+12])[0]
                            
                            magic_cookie = 0x2112A442
                            port = xport ^ (magic_cookie >> 16)
                            ip = xip ^ magic_cookie
                            
                            public_ip = socket.inet_ntoa(struct.pack('!I', ip))
                            return f"{public_ip}:{port}"
                    
                    pos += 4 + attr_length
            
            return f"{addr[0]}:{addr[1]}"
            
        except (socket.timeout, socket.error):
            continue
        finally:
            try:
                sock.close()
            except:
                pass
    
    return None

# Update the coordinate_hole_punching function to handle chunk transfers
def coordinate_hole_punching(target_peer, file_hash=None, chunk_hash=None):
    """Coordinate hole punching with another peer and transfer chunk if needed"""
    try:
        # Get our public endpoint
        public_endpoint = get_public_endpoint()
        if not public_endpoint:
            return False
        
        # Contact target peer to initiate hole punching
        response = requests.post(
            f"{target_peer}/initiate_punch",
            json={
                'requester': public_endpoint,
                'file_hash': file_hash,
                'chunk_hash': chunk_hash,
                'timestamp': time.time()
            },
            timeout=3
        )
        
        if response.status_code == 200:
            data = response.json()
            target_public = data.get('public_endpoint')
            
            if target_public:
                # Now both peers know each other's public endpoints
                # Try to establish direct connection and transfer chunk
                if chunk_hash:
                    return transfer_chunk_via_p2p(target_public, chunk_hash)
                return attempt_direct_connection(target_public, file_hash, chunk_hash)
        
        return False
        
    except requests.exceptions.RequestException:
        return False

def attempt_direct_connection(target_endpoint, file_hash=None, chunk_hash=None):
    """Attempt to establish direct UDP connection"""
    
    try:
        ip, port = target_endpoint.split(':')
        port = int(port)
        
        # Create UDP socket for hole punching
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(app.config['HOLE_PUNCHING_TIMEOUT'])
        
        # Send punch packet
        punch_data = json.dumps({
            'type': 'punch',
            'node_id': app.config['NODE_ID_HEX'],
            'file_hash': file_hash,
            'chunk_hash': chunk_hash
        }).encode()
        
        sock.sendto(punch_data, (ip, port))
        
        # Wait for response
        try:
            data, addr = sock.recvfrom(1024)
            if data:
                response = json.loads(data.decode())
                if response.get('type') == 'punch_ack':
                    print(f"Hole punching successful with {target_endpoint}")
                    return True
        except socket.timeout:
            pass
        
        return False
        
    except (ValueError, socket.error, json.JSONDecodeError):
        return False

# Enhance the transfer_chunk_via_p2p function
def transfer_chunk_via_p2p(target_endpoint, chunk_hash):
    """Transfer chunk directly via established P2P connection"""
    
    try:
        ip, port = target_endpoint.split(':')
        port = int(port)
        
        chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
        if not os.path.exists(chunk_path):
            return False
        
        # Read chunk data
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
        
        # Create socket and send chunk
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        
        # Send metadata first
        metadata = json.dumps({
            'type': 'chunk_transfer',
            'chunk_hash': chunk_hash,
            'size': len(chunk_data),
            'timestamp': time.time()
        }).encode()
        
        sock.sendto(metadata, (ip, port))
        
        # Wait for acknowledgment
        try:
            data, addr = sock.recvfrom(1024)
            if data:
                response = json.loads(data.decode())
                if response.get('type') == 'ready':
                    # Send chunk data in multiple packets if needed
                    chunk_size = len(chunk_data)
                    max_packet_size = 1024  # Adjust based on your network MTU
                    
                    for i in range(0, chunk_size, max_packet_size):
                        end_idx = min(i + max_packet_size, chunk_size)
                        packet = chunk_data[i:end_idx]
                        sock.sendto(packet, (ip, port))
                    
                    # Send completion signal
                    complete_signal = json.dumps({
                        'type': 'complete',
                        'chunk_hash': chunk_hash
                    }).encode()
                    sock.sendto(complete_signal, (ip, port))
                    
                    # Wait for final acknowledgment
                    data, addr = sock.recvfrom(1024)
                    if data and json.loads(data.decode()).get('type') == 'complete_ack':
                        return True
        except socket.timeout:
            pass
        
        return False
        
    except (ValueError, socket.error, json.JSONDecodeError, IOError):
        return False

# Database setup with node ID as salt
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
        db.row_factory = sqlite3.Row
        
        # Apply node_id as salt for database operations
        cursor = db.cursor()
        cursor.execute(f"PRAGMA key = '{app.config['NODE_ID_HEX']}'")
        cursor.execute("PRAGMA cipher_compatibility = 3")
        cursor.execute("PRAGMA kdf_iter = 256000")  # High iteration count for security
        cursor.execute("PRAGMA cipher_page_size = 4096")
    
    return db

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_hash TEXT PRIMARY KEY,
            original_name TEXT NOT NULL,
            display_name TEXT NOT NULL,
            game TEXT NOT NULL,
            description TEXT,
            password_hash TEXT NOT NULL,
            origin_node TEXT NOT NULL,
            size INTEGER NOT NULL,
            owner TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_hash TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            FOREIGN KEY (file_hash) REFERENCES files (file_hash)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS hosts (
            file_hash TEXT NOT NULL,
            node_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (file_hash, node_id),
            FOREIGN KEY (file_hash) REFERENCES files (file_hash)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS peers (
            peer_address TEXT PRIMARY KEY,
            peer_type TEXT NOT NULL CHECK(peer_type IN ('friend', 'peer', 'stranger', 'enemy')),
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        db.commit()

def close_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)

# Helper functions with node ID as salt for cryptographic operations
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def hash_with_node_id(data):
    """Hash data with the node ID as salt for consistent hashing"""
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha256(data + app.config['NODE_ID']).hexdigest()

def split_file(filepath, file_hash):
    """Split file into chunks and return chunk hashes"""
    with app.app_context():
        chunks = []
        sequence = 0
        db = get_db()
        cursor = db.cursor()
        
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(current_app.config['CHUNK_SIZE'])
                if not chunk:
                    break
                # Use node ID as salt for chunk hashing
                chunk_hash = hash_with_node_id(chunk)
                chunk_filename = f"{chunk_hash}.chunk"
                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], chunk_filename)
                
                # Save chunk (in reality this would be distributed)
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                
                # Store chunk in database
                cursor.execute(
                    'INSERT OR IGNORE INTO chunks (chunk_hash, file_hash, sequence) VALUES (?, ?, ?)',
                    (chunk_hash, file_hash, sequence)
                )
                
                chunks.append(chunk_hash)
                sequence += 1
        
        db.commit()
        return chunks
    
def reassemble_file(file_hash, output_path):
    """Reassemble file from chunks"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Get all chunks for this file in order
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
            (file_hash,)
        )
        chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunk_hashes:
            raise ValueError("No chunks found for this file")
        
        with open(output_path, 'wb') as outfile:
            for chunk_hash in chunk_hashes:
                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                
                # Try local chunk first
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as chunk_file:
                        outfile.write(chunk_file.read())
                    continue
                    
                # Try to fetch chunk from peers
                chunk_found = False
                for peer in current_app.config['PEERS']:
                    try:
                        response = requests.get(f"{peer}/chunk/{chunk_hash}", timeout=5)
                        if response.status_code == 200:
                            outfile.write(response.content)
                            # Save chunk locally for future requests
                            with open(chunk_path, 'wb') as chunk_file:
                                chunk_file.write(response.content)
                            chunk_found = True
                            break
                    except requests.exceptions.RequestException:
                        continue
                
                if not chunk_found:
                    raise FileNotFoundError(f"Missing chunk {chunk_hash} for file {file_hash}")
        
        return True

def broadcast_to_peers(data, endpoint):
    """Broadcast data to all known peers"""
    with app.app_context():
        for peer in current_app.config['PEERS']:
            try:
                requests.post(f"{peer}/{endpoint}", json=data, timeout=2)
            except requests.exceptions.RequestException:
                continue

def register_hosting(file_hash, node_id=None):
    """Register that this node is hosting a file"""
    with app.app_context():
        if node_id is None:
            node_id = current_app.config['NODE_ID']
        
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO hosts (file_hash, node_id) VALUES (?, ?)',
            (file_hash, node_id)
        )
        db.commit()

def get_file_hosts(file_hash):
    """Get all nodes hosting a specific file"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'SELECT node_id FROM hosts WHERE file_hash = ?',
            (file_hash,)
        )
        return [row['node_id'] for row in cursor.fetchall()]

def get_shared_files():
    """Get all files shared in the network with health scores"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            SELECT *
            FROM files
        ''')
        files = cursor.fetchall()
        
        # Add health score to each file
        result = []
        for file in files:
            file_dict = dict(file)
            file_dict['health'] = calculate_file_health(file['file_hash'])
            result.append(file_dict)
        
        return result

# Update the peer loading function
def load_peers_from_db():
    """Load known peers from database at startup and categorize them"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('SELECT peer_address, peer_type FROM peers')
        
        # Clear all peer sets
        for peer_type in current_app.config['PEER_TYPES']:
            current_app.config['PEER_TYPES'][peer_type].clear()
        
        # Add peers from database to their respective sets
        for row in cursor.fetchall():
            peer_type = row['peer_type'] + 's'  # Convert to plural form
            if peer_type in current_app.config['PEER_TYPES']:
                current_app.config['PEER_TYPES'][peer_type].add(row['peer_address'])
        
        # Add default peers as regular peers if they're not already present
        for peer in current_app.config['DEFAULT_PEERS']:
            if not any(peer in peers for peers in current_app.config['PEER_TYPES'].values()):
                current_app.config['PEER_TYPES']['peers'].add(peer)
                try:
                    cursor.execute(
                        'INSERT OR IGNORE INTO peers (peer_address, peer_type) VALUES (?, ?)',
                        (peer, 'peer')
                    )
                    db.commit()
                except sqlite3.Error as e:
                    print(f"Error adding default peer to database: {e}")

# New function to categorize a peer
def categorize_peer(peer_address, peer_type):
    """Categorize a peer into one of the types"""
    valid_types = ['friend', 'peer', 'stranger', 'enemy']
    if peer_type not in valid_types:
        raise ValueError(f"Invalid peer type. Must be one of {valid_types}")
    
    with app.app_context():
        # Remove from all categories first
        for pt in current_app.config['PEER_TYPES']:
            if peer_address in current_app.config['PEER_TYPES'][pt]:
                current_app.config['PEER_TYPES'][pt].remove(peer_address)
        
        # Add to the specified category
        plural_type = peer_type + 's'
        if plural_type in current_app.config['PEER_TYPES']:
            current_app.config['PEER_TYPES'][plural_type].add(peer_address)
        
        # Update database
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO peers (peer_address, peer_type) VALUES (?, ?)',
            (peer_address, peer_type)
        )
        db.commit()

# Add these routes after existing routes

@app.route('/initiate_punch', methods=['POST'])
def initiate_punch():
    """Initiate hole punching process"""
    data = request.get_json()
    if not data or 'requester' not in data:
        return jsonify({'status': 'error'}), 400
    
    requester_endpoint = data['requester']
    
    # Get our public endpoint
    public_endpoint = get_public_endpoint()
    if not public_endpoint:
        return jsonify({'status': 'error'}), 500
    
    # Start hole punching thread
    thread = threading.Thread(
        target=perform_hole_punching,
        args=(requester_endpoint, data.get('file_hash'), data.get('chunk_hash'))
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'status': 'success',
        'public_endpoint': public_endpoint
    })

def perform_hole_punching(target_endpoint, file_hash=None, chunk_hash=None):
    """Perform the actual hole punching"""
    
    try:
        ip, port = target_endpoint.split(':')
        port = int(port)
        
        # Create UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(app.config['HOLE_PUNCHING_TIMEOUT'])
        
        # Send multiple punch packets (NATs may drop first packets)
        for i in range(3):
            punch_data = json.dumps({
                'type': 'punch_ack',
                'node_id': app.config['NODE_ID_HEX'],
                'file_hash': file_hash,
                'chunk_hash': chunk_hash
            }).encode()
            
            sock.sendto(punch_data, (ip, port))
            time.sleep(0.5)
        
        # If we have a chunk to transfer, do it now
        if chunk_hash and file_hash:
            chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
            if os.path.exists(chunk_path):
                transfer_chunk_via_p2p(target_endpoint, chunk_hash)
        
        return True
        
    except (ValueError, socket.error):
        return False

@app.route('/udp_listener', methods=['POST'])
def start_udp_listener():
    """Start UDP listener for hole punching (optional endpoint)"""
    thread = threading.Thread(target=udp_listener)
    thread.daemon = True
    thread.start()
    return jsonify({'status': 'listener_started'})

# Update the UDP listener to handle chunk transfers
def udp_listener():
    global udpon
    if udpon:
        return
    """UDP listener for hole punching and direct transfers"""
    
    # Create application context for this thread
    with app.app_context():
        updon = True
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0', current_app.config['NAT_TRAVERSAL_PORT']))
        sock.settimeout(1)
        
        print(f"UDP listener started on port {current_app.config['NAT_TRAVERSAL_PORT']}")
        
        # Buffer for reassembling chunks
        chunk_buffers = {}
        
        while True:
            try:
                data, addr = sock.recvfrom(65535)
                if not data:
                    continue
                    
                try:
                    # Try to parse as JSON message first
                    message = json.loads(data.decode())
                    message_type = message.get('type')
                    
                    if message_type == 'punch':
                        # Acknowledge punch
                        ack = json.dumps({
                            'type': 'punch_ack',
                            'node_id': current_app.config['NODE_ID_HEX']
                        }).encode()
                        sock.sendto(ack, addr)
                        
                    elif message_type == 'chunk_transfer':
                        # Prepare to receive chunk
                        chunk_hash = message['chunk_hash']
                        chunk_size = message['size']
                        
                        # Initialize buffer for this chunk
                        chunk_buffers[chunk_hash] = {
                            'data': bytearray(),
                            'size': chunk_size,
                            'received': 0,
                            'addr': addr,
                            'last_received': time.time()
                        }
                        
                        # Send ready signal
                        ready = json.dumps({'type': 'ready'}).encode()
                        sock.sendto(ready, addr)
                        
                    elif message_type == 'complete':
                        # Finalize chunk transfer
                        chunk_hash = message['chunk_hash']
                        if chunk_hash in chunk_buffers:
                            buffer_info = chunk_buffers[chunk_hash]
                            
                            # Save chunk to file
                            chunk_path = os.path.join(
                                current_app.config['UPLOAD_FOLDER'], 
                                f"{chunk_hash}.chunk"
                            )
                            with open(chunk_path, 'wb') as f:
                                f.write(buffer_info['data'])
                            
                            # Send completion acknowledgment
                            complete_ack = json.dumps({'type': 'complete_ack'}).encode()
                            sock.sendto(complete_ack, addr)
                            
                            # Remove from buffer
                            del chunk_buffers[chunk_hash]
                            
                            print(f"Successfully received chunk {chunk_hash} via UDP")
                        
                except json.JSONDecodeError:
                    # This is likely chunk data, not a JSON message
                    # Check if it belongs to any ongoing transfer
                    udpon = False
                    for chunk_hash, buffer_info in chunk_buffers.items():
                        if addr == buffer_info['addr']:
                            buffer_info['data'].extend(data)
                            buffer_info['received'] += len(data)
                            buffer_info['last_received'] = time.time()
                            break
            
            except socket.timeout:
                udpon = False
                # Clean up stale buffers
                current_time = time.time()
                stale_chunks = []
                for chunk_hash, buffer_info in chunk_buffers.items():
                    if current_time - buffer_info.get('last_received', 0) > 30:  # 30 second timeout
                        stale_chunks.append(chunk_hash)
                
                for chunk_hash in stale_chunks:
                    del chunk_buffers[chunk_hash]
                    
            except Exception as e:
                udpon = False
                print(f"UDP listener error: {e}")
                time.sleep(1)

@app.route('/add_peer', methods=['POST'])
def add_peer():
    peer_address = request.form.get('peer_address')
    peer_type = request.form.get('peer_type', 'peer')  # Default to 'peer'
    
    if not peer_address:
        return "Invalid peer address", 400
        
    # Ensure the peer address has http:// prefix if not present
    if not peer_address.startswith(('http://', 'https://')):
        peer_address = f"http://{peer_address}"
        
    # Remove trailing slash if present
    peer_address = peer_address.rstrip('/')
    
    # Don't allow modification of default peers
    if peer_address in current_app.config['DEFAULT_PEERS']:
        return "Cannot modify default peers", 400
    
    try:
        categorize_peer(peer_address, peer_type)
        
        # Notify the new peer about our existence (only if friend or peer)
        if peer_type in ['friend', 'peer']:
            try:
                our_address = request.host_url.rstrip('/')
                if not our_address.startswith('http'):
                    our_address = f"http://{our_address}"
                requests.post(f"{peer_address}/add_peer", 
                             json={
                                 'peer_address': our_address,
                                 'peer_type': 'peer'  # Default to peer when notifying
                             },
                             timeout=2)
            except requests.exceptions.RequestException as e:
                print(f"Failed to notify peer {peer_address}: {e}")
            
        return redirect(url_for('index'))
    except ValueError as e:
        return str(e), 400

# API endpoint to change peer type
@app.route('/set_peer_type', methods=['POST'])
def set_peer_type():
    peer_address = request.form.get('peer_address')
    peer_type = request.form.get('peer_type')
    
    if not peer_address or not peer_type:
        return "Missing parameters", 400
    
    try:
        categorize_peer(peer_address, peer_type)
        return redirect(url_for('index'))
    except ValueError as e:
        return str(e), 400

def maintenance_check():
    """Periodically check file health and use relay if needed"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    with app.app_context():
        while True:
            time.sleep(current_app.config['MAINTENANCE_INTERVAL'])
            
            # Get all peers in priority order: friends first, then peers, then strangers
            all_peers = (
                list(current_app.config['PEER_TYPES']['friends']) +
                list(current_app.config['PEER_TYPES']['peers']) +
                list(current_app.config['PEER_TYPES']['strangers'])
            )
            
            # Check each file's health
            db = get_db()
            cursor = db.cursor()
            cursor.execute('SELECT file_hash, original_name, display_name, owner FROM files')
            files = cursor.fetchall()
            
            for file in files:
                file_hash = file['file_hash']
                file_name = file['display_name']
                owner = file['owner']
                
                try:
                    health = calculate_file_health(file_hash)
                    print(f"File {file_name} has health score: {health:.1f}")
                    
                    # If health is below 3.0, we need to share more chunks
                    if health < 3.0:
                        print(f"File {file_name} needs health improvement (current: {health:.1f})")
                        
                        # Get vulnerable chunks
                        cursor.execute('''
                            SELECT c.chunk_hash, COUNT(DISTINCT h.node_id) as host_count
                            FROM chunks c
                            LEFT JOIN hosts h ON c.file_hash = h.file_hash
                            WHERE c.file_hash = ?
                            GROUP BY c.chunk_hash
                            HAVING host_count < 3
                            ORDER BY host_count ASC
                            LIMIT 5
                        ''', (file_hash,))
                        vulnerable_chunks = cursor.fetchall()
                        
                        if not vulnerable_chunks:
                            continue
                            
                        vulnerable_chunk_hashes = [row['chunk_hash'] for row in vulnerable_chunks]
                            
                        candidates = []
                        for peer in all_peers:
                            try:
                                response = requests.post(
                                    f"{peer}/find_chunk",
                                    json={'chunk_hash': vulnerable_chunk_hashes},
                                    timeout=3
                                )
                                
                                if response.status_code == 200:
                                    hosts = response.json().get('hosts', [])
                                    if hosts:
                                        # We found potential sources through relay
                                        print(f"Found {len(hosts)} potential sources for chunk {vulnerable_chunk_hashes} via {peer}")
                                        break
                            except requests.exceptions.RequestException:
                                continue
                            try:
                                # First check if peer already has these chunks
                                response = requests.post(
                                    f"{peer}/check_chunks",
                                    json={'chunks': vulnerable_chunk_hashes},
                                    timeout=2
                                )
                                
                                if response.status_code == 200:
                                    data = response.json()
                                    # If peer is missing any of these chunks, they're a candidate
                                    if len(data['available_chunks']) < len(vulnerable_chunk_hashes):
                                        candidates.append(peer)
                            except requests.exceptions.RequestException:
                                continue
                            
                        # Share vulnerable chunks with peers that need them
                        needed = min(3, len(candidates))  # Share with up to 3 new peers
                        for i in range(needed):
                            peer = candidates[i]
                            try:
                                # First check which chunks this peer is missing
                                response = requests.post(
                                    f"{peer}/check_chunks",
                                    json={'chunks': vulnerable_chunk_hashes},
                                    timeout=2
                                )
                                
                                if response.status_code != 200:
                                    continue
                                    
                                data = response.json()
                                missing_chunks = set(vulnerable_chunk_hashes) - set(data['available_chunks'])
                                
                                if not missing_chunks:
                                    continue
                                
                                # Send file info first
                                cursor.execute(
                                    'SELECT * FROM files WHERE file_hash = ?',
                                    (file_hash,)
                                )
                                file_info = dict(cursor.fetchone())
                                file_info['chunks'] = list(missing_chunks)
                                
                                response = requests.post(
                                    f"{peer}/register_file",
                                    json={
                                        'file_hash': file_hash,
                                        'file_info': file_info
                                    },
                                    timeout=5
                                )
                                response.raise_for_status()
                                
                                for chunk_hash in missing_chunks:
                                    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                                    if os.path.exists(chunk_path):
                                        try:
                                            with open(chunk_path, 'rb') as f:
                                                files = {'chunk': (f"{chunk_hash}.chunk", f)}
                                                response = requests.post(
                                                    f"{peer}/upload_chunk",
                                                    files=files,
                                                    timeout=10
                                                )
                                                response.raise_for_status()
                                                success = True
                                        except requests.exceptions.RequestException:
                                            # HTTP failed, try hole punching
                                            if coordinate_hole_punching(peer, file_hash, chunk_hash):
                                                success = True
                                
                                if success:
                                    # Register that this peer is now hosting the file
                                    response = requests.post(
                                        f"{peer}/register_hosting",
                                        json={
                                            'file_hash': file_hash,
                                            'node_id': current_app.config['NODE_ID_HEX']
                                        },
                                        timeout=2
                                    )
                                    print(f"Successfully shared vulnerable chunks of {file_name} with {peer}")
                                    
                            except requests.exceptions.RequestException as e:
                                print(f"Failed to share with {peer}: {str(e)}")
                    
                    # If health is very high (>8.0) and we're not the owner, consider stopping hosting
                    elif health > 8.0 and owner != current_app.config['NODE_ID_HEX']:
                        print(f"File {file_name} has excellent health ({health:.1f}), considering stopping hosting")
                        # Maybe remove some chunks we're hosting for this file
                
                
                except Exception as e:
                    print(f"Maintenance error for {file_name}: {str(e)}")
            
            # Automatic peer discovery through public nodes
            for peer_type in ['friends', 'peers']:
                for peer in current_app.config['PEER_TYPES'][peer_type]:
                    try:
                        # Get list of files from public peer
                        response = requests.get(f"{peer}/shared_files", timeout=5)
                        if response.status_code == 200:
                            peer_files = response.json()
                            for file_hash, file_info in peer_files.items():
                                # Check if we already have this file
                                cursor.execute(
                                    'SELECT 1 FROM files WHERE file_hash = ?',
                                    (file_hash,)
                                )
                                if not cursor.fetchone():
                                    # Register new file discovered through public peer
                                    cursor.execute(
                                        'INSERT INTO files (file_hash, original_name, display_name, game, description, password_hash, origin_node, size, owner) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                        (file_hash, file_info.get('original_name'), file_info.get('display_name'), 
                                        file_info.get('game'), file_info.get('description'), file_info.get('password_hash'),
                                        file_info.get('origin_node'), file_info.get('size'), file_info.get('owner'))
                                    )
                                    db.commit()
                                    print(f"Discovered new file via {peer}: {file_info.get('display_name')}")
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to check for files from {peer}: {str(e)}")
                        continue

def calculate_file_health(file_hash):
    """Calculate file health score (1.0 = all chunks have 1 host, 10.0 = all have 10 hosts)"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Get all chunks for this file
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ?',
            (file_hash,)
        )
        chunks = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunks:
            return 0.0  # No chunks means unhealthy file
        
        # Get host count for each chunk
        chunk_host_counts = []
        for chunk_hash in chunks:
            cursor.execute(
                'SELECT COUNT(DISTINCT node_id) as host_count ' \
                'FROM hosts h '
                'JOIN chunks c ON h.file_hash = c.file_hash '
                'WHERE c.chunk_hash = ?',
                (chunk_hash,)
            )
            count = cursor.fetchone()['host_count']
            chunk_host_counts.append(count)
        
        min_hosts = min(chunk_host_counts)
        if min_hosts == 0:
            return 0.0  # At least one chunk has no hosts
        
        # Calculate health score based on min hosts (most vulnerable chunk)
        health = min(min_hosts, 10.0)  # Each additional host adds 2.0 to health, capped at 10.0
        
        # Apply small bonus for average hosts
        avg_hosts = sum(chunk_host_counts) / len(chunk_host_counts)
        health = min(health + (avg_hosts - min_hosts) * 0.5, 10.0)
        
        return health

# Routes
@app.route('/')
def index():
    with app.app_context():
        files = get_shared_files()
        files_data = {file['file_hash']: file for file in files}
        return render_template('index.html', 
                            files=files_data, 
                            peer_types=current_app.config['PEER_TYPES'])

@app.route('/check_chunks', methods=['POST'])
def check_chunks():
    """Check which chunks this node already has"""
    data = request.get_json()
    if not data or 'chunks' not in data:
        return jsonify({'status': 'error', 'message': 'Missing chunks list'}), 400
    
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Check which chunks we have locally
        available_chunks = []
        for chunk_hash in data['chunks']:
            chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
            if os.path.exists(chunk_path):
                available_chunks.append(chunk_hash)
            else:
                # Also check database in case it's registered but file is missing
                cursor.execute(
                    'SELECT 1 FROM chunks WHERE chunk_hash = ?',
                    (chunk_hash,))
                if cursor.fetchone():
                    available_chunks.append(chunk_hash)
        
        return jsonify({
            'status': 'success',
            'available_chunks': available_chunks
        })

# Update the generate_download_stream function to use the new download method
def generate_download_stream(file_hash):
    with app.app_context():
        # Get all chunks for this file in order
        cursor = get_db().cursor()
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
            (file_hash,)
        )
        chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunk_hashes:
            raise ValueError("No chunks found for this file")
        
        for chunk_hash in chunk_hashes:
            try:
                chunk_data = download_chunk_with_fallback(chunk_hash, file_hash)
                yield chunk_data
            except FileNotFoundError:
                raise FileNotFoundError(f"Missing chunk {chunk_hash} for file {file_hash}")

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    
    required_fields = ['display_name', 'game', 'password']
    for field in required_fields:
        if not request.form.get(field):
            return f"Missing required field: {field}", 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Calculate file hash
        file_hash = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
        
        # Hash the password with node ID as salt
        password_hash = hash_with_node_id(request.form['password'])
        
        # Split file into chunks
        chunk_hashes = split_file(filepath, file_hash)
        
        # Store file metadata in database
        with app.app_context():
            db = get_db()
            cursor = db.cursor()
            cursor.execute(
                'INSERT INTO files (file_hash, original_name, display_name, game, description, password_hash, origin_node, size, owner) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (file_hash, 
                 filename,
                 request.form['display_name'],
                 request.form['game'],
                 request.form.get('description', ''),
                 password_hash,
                 current_app.config['NODE_ID_HEX'],
                 os.path.getsize(filepath), 
                 current_app.config['NODE_ID_HEX'])
            )
            db.commit()
            
            # Register that we're hosting this file
            register_hosting(file_hash)
            
            # Broadcast file metadata to peers
            broadcast_to_peers({
                'file_hash': file_hash,
                'file_info': {
                    'original_name': filename,
                    'display_name': request.form['display_name'],
                    'game': request.form['game'],
                    'description': request.form.get('description', ''),
                    'password_hash': password_hash,
                    'origin_node': current_app.config['NODE_ID_HEX'],
                    'size': os.path.getsize(filepath),
                    'chunks': chunk_hashes,
                    'owner': current_app.config['NODE_ID_HEX']
                }
            }, 'register_file')
            
            # Clean up original file (we only keep chunks)
            os.remove(filepath)
            
            # Show success message with password reminder
            return render_template('upload_success.html', 
                                file_name=request.form['display_name'],
                                password=request.form['password'])
    
    return redirect(request.url)

@app.route('/download/<file_hash>')
def download_file(file_hash):
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Check if we have the file in our database
        cursor.execute(
            'SELECT original_name, display_name, size FROM files WHERE file_hash = ?',
            (file_hash,)
        )
        file_info = cursor.fetchone()
        
        if not file_info:
            # Check if any peers have this file (friends first)
            for peer_type in ['friends', 'peers', 'strangers']:
                for peer in current_app.config['PEER_TYPES'][peer_type]:
                    try:
                        response = requests.get(f"{peer}/file_info/{file_hash}")
                        if response.status_code == 200:
                            file_data = response.json()
                            # Store the file info in our database
                            cursor.execute(
                                'INSERT INTO files (file_hash, original_name, display_name, size, owner) VALUES (?, ?, ?, ?, ?)',
                                (file_hash, 
                                 file_data.get('original_name', file_data.get('name', 'unknown')),
                                 file_data.get('display_name', file_data.get('name', 'unknown')),
                                 file_data['size'], 
                                 file_data['owner'])
                            )
                            db.commit()
                            file_info = {
                                'original_name': file_data.get('original_name', file_data.get('name', 'unknown')),
                                'display_name': file_data.get('display_name', file_data.get('name', 'unknown')),
                                'size': file_data['size']
                            }
                            break
                    except requests.exceptions.RequestException:
                        continue
                else:
                    continue
                break
            else:
                return "File not found", 404

        # Create a generator to stream the file
        def generate():
            # Get all chunks for this file in order
            cursor.execute(
                'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
                (file_hash,)
            )
            chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
            
            if not chunk_hashes:
                raise ValueError("No chunks found for this file")
            
            for chunk_hash in chunk_hashes:
                try:
                    chunk_data = download_chunk_with_relay(chunk_hash, file_hash)
                    yield chunk_data
                except FileNotFoundError:
                    raise FileNotFoundError(f"Missing chunk {chunk_hash}")
                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                
                # Try local chunk first
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as chunk_file:
                        yield chunk_file.read()
                    continue
                    
                # Try to fetch chunk from peers - friends first, then peers, then strangers
                chunk_found = False
                for peer_type in ['friends', 'peers', 'strangers']:
                    for peer in current_app.config['PEER_TYPES'][peer_type]:
                        try:
                            response = requests.get(f"{peer}/chunk/{chunk_hash}", stream=True, timeout=5)
                            if response.status_code == 200:
                                yield from response.iter_content(chunk_size=8192)
                                # Save chunk locally for future requests
                                with open(chunk_path, 'wb') as f:
                                    response.raw.decode_content = True
                                    f.write(response.raw.read())
                                chunk_found = True
                                break
                        except requests.exceptions.RequestException:
                            continue
                    if chunk_found:
                        break
                
                if not chunk_found:
                    raise FileNotFoundError(f"Missing chunk {chunk_hash} for file {file_hash}")

        response = Response(
            generate(),
            mimetype='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename="{file_info["original_name"]}"',
                'Content-Length': file_info['size']
            }
        )
        
        return response

@app.route('/register_file', methods=['POST'])
def register_file():
    # First try to get JSON data
    if request.content_type == 'application/json':
        data = request.get_json()
        
        # Handle JSON registration
        if 'file_hash' in data and 'file_info' in data:
            with app.app_context():
                db = get_db()
                cursor = db.cursor()
                
                # Check if we already know about this file
                cursor.execute(
                    'SELECT 1 FROM files WHERE file_hash = ?',
                    (data['file_hash'],)
                )
                if not cursor.fetchone():
                    # Store the file metadata
                    cursor.execute(
                        'INSERT INTO files (file_hash, original_name, display_name, game, description, password_hash, origin_node, size, owner, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (data['file_hash'], 
                         data['file_info'].get('original_name', data['file_info'].get('name', 'unknown')),
                         data['file_info'].get('display_name', data['file_info'].get('name', 'unknown')),
                         data['file_info'].get('game', 'Unknown'),
                         data['file_info'].get('description', ' '),
                         data['file_info'].get('password_hash', 'INVALID HASH'),
                         data['file_info'].get('origin_node'),
                         data['file_info']['size'], 
                         data['file_info']['owner'],
                         data['file_info'].get('timestamp')
                         )
                    )
                    
                    # Store the chunks
                    for sequence, chunk_hash in enumerate(data['file_info']['chunks']):
                        cursor.execute(
                            'INSERT OR IGNORE INTO chunks (chunk_hash, file_hash, sequence) VALUES (?, ?, ?)',
                            (chunk_hash, data['file_hash'], sequence)
                        )
                    
                    db.commit()
                
                return jsonify({'status': 'success'})
        
        return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400
    
    # Handle file upload
    elif 'file' in request.files:
        file = request.files['file']
        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'No file selected'}), 400
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Calculate file hash
            file_hash = hash_with_node_id(open(filepath, 'rb').read())
            
            # Split file into chunks
            chunk_hashes = split_file(filepath, file_hash)
            
            # Store file metadata in database
            with app.app_context():
                db = get_db()
                cursor = db.cursor()
                cursor.execute(
                    'INSERT INTO files (file_hash, name, size, owner) VALUES (?, ?, ?, ?)',
                    (file_hash, filename, os.path.getsize(filepath), current_app.config['NODE_ID'])
                )
                db.commit()
                
                # Register that we're hosting this file
                register_hosting(file_hash)
                
                # Clean up original file
                os.remove(filepath)
                
                return jsonify({
                    'status': 'success',
                    'file_hash': file_hash,
                    'name': filename,
                    'size': os.path.getsize(filepath),
                    'chunks': chunk_hashes
                })
    
    return jsonify({'status': 'error', 'message': 'Invalid request format'}), 400

@app.route('/update_file/<file_hash>', methods=['POST'])
def update_file(file_hash):
    if 'file' not in request.files:
        return "No file provided", 400
    
    password = request.form.get('password')
    if not password:
        return "Password required for updates", 401
    
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Verify password
        cursor.execute(
            'SELECT password_hash FROM files WHERE file_hash = ?',
            (file_hash,)
        )
        result = cursor.fetchone()
        if not result:
            return "File not found", 404
            
        # Verify password hash matches
        provided_hash = hash_with_node_id(password)
        if provided_hash != result['password_hash']:
            return "Invalid password", 403
        
        # Proceed with update
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Calculate new file hash
            new_file_hash = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
            
            # Split file into chunks
            chunk_hashes = split_file(filepath, new_file_hash)
            
            # Update file metadata in database
            cursor.execute(
                '''UPDATE files SET 
                    file_hash = ?,
                    original_name = ?,
                    size = ?,
                    timestamp = CURRENT_TIMESTAMP
                WHERE file_hash = ?''',
                (new_file_hash, filename, os.path.getsize(filepath), file_hash)
            )
            
            # Remove old chunks and add new ones
            cursor.execute('DELETE FROM chunks WHERE file_hash = ?', (file_hash,))
            for sequence, chunk_hash in enumerate(chunk_hashes):
                cursor.execute(
                    'INSERT INTO chunks (chunk_hash, file_hash, sequence) VALUES (?, ?, ?)',
                    (chunk_hash, new_file_hash, sequence)
                )
            
            db.commit()
            
            # Broadcast update to peers
            broadcast_to_peers({
                'old_file_hash': file_hash,
                'new_file_hash': new_file_hash,
                'file_info': {
                    'original_name': filename,
                    'size': os.path.getsize(filepath),
                    'chunks': chunk_hashes
                }
            }, 'update_file')
            
            # Clean up original file
            os.remove(filepath)
            
            return redirect(url_for('index'))
    
    return "Invalid file", 400

@app.route('/file_info/<file_hash>')
def get_file_info(file_hash):
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute(
            'SELECT file_hash, original_name, display_name, size, owner FROM files WHERE file_hash = ?',
            (file_hash,)
        )
        file_info = cursor.fetchone()
        
        if file_info:
            # Get the chunks
            cursor.execute(
                'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
                (file_hash,)
            )
            chunks = [row['chunk_hash'] for row in cursor.fetchall()]
            
            return jsonify({
                'file_hash': file_info['file_hash'],
                'original_name': file_info['original_name'],
                'display_name': file_info['display_name'],
                'size': file_info['size'],
                'owner': file_info['owner'],
                'chunks': chunks
            })
        return "File not found", 404

@app.route('/chunk/<chunk_hash>')
def get_chunk(chunk_hash):
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    if os.path.exists(chunk_path):
        return send_file(chunk_path, mimetype='application/octet-stream')
    
    # If we don't have the chunk locally, try to get it from other peers with hole punching
    try:
        # Get file_hash from database if possible
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'SELECT file_hash FROM chunks WHERE chunk_hash = ?',
            (chunk_hash,)
        )
        result = cursor.fetchone()
        file_hash = result['file_hash'] if result else None
        
        chunk_data = download_chunk_with_fallback(chunk_hash, file_hash)
        return Response(chunk_data, mimetype='application/octet-stream')
    except FileNotFoundError:
        return "Chunk not found", 404

# Update the upload_chunk endpoint to support hole punching
@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    if 'chunk' not in request.files:
        # Check if this is a hole punching transfer
        data = request.get_json()
        if data and data.get('type') == 'chunk_transfer_request':
            chunk_hash = data.get('chunk_hash')
            if chunk_hash:
                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                if os.path.exists(chunk_path):
                    # Prepare for direct transfer
                    return jsonify({
                        'status': 'ready',
                        'chunk_size': os.path.getsize(chunk_path)
                    })
                return jsonify({'status': 'not_found'}), 404
        return jsonify({'status': 'error', 'message': 'No chunk provided'}), 400
    
    # Regular HTTP upload
    chunk_file = request.files['chunk']
    chunk_hash = os.path.splitext(chunk_file.filename)[0]
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    
    try:
        # Save the chunk
        chunk_file.save(chunk_path)
        
        # Verify the chunk was saved correctly
        if not os.path.exists(chunk_path):
            return jsonify({'status': 'error', 'message': 'Failed to save chunk'}), 500
            
        return jsonify({'status': 'success', 'chunk_hash': chunk_hash})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/shared_files')
def list_shared_files():
    with app.app_context():
        files = get_shared_files()
        return jsonify({file['file_hash']: dict(file) for file in files})

@app.route('/register_hosting', methods=['POST'])
def api_register_hosting():
    data = request.json
    if data and 'file_hash' in data:
        register_hosting(data['file_hash'])
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 400

def start_maintenance_thread():
    """Start the background maintenance thread"""
    thread = threading.Thread(target=maintenance_check)
    thread.daemon = True
    thread.start()

if __name__ == '__main__':
    # Create upload directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Initialize database and load peers within application context
    with app.app_context():
        init_db()
        load_peers_from_db()
    
    # Start maintenance thread
    start_maintenance_thread()
    
    # Start UDP listener for hole punching
    # udp_thread = threading.Thread(target=udp_listener)
    # udp_thread.daemon = True
    # udp_thread.start()
    
    app.run(debug=True, host='0.0.0.0', port=5000)