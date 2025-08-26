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
from networking import register_hosting
from peers import categorize_peer, load_peers_from_db, peerblueprint, broadcast_to_peers
from database import get_db, init_db, close_db
from files import calculate_file_health, fileblueprint, get_shared_files

udpon = False

app = Flask(__name__, template_folder="../templates")


# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024
app.config['CHUNK_SIZE'] = 64 * 1024 * 1024
app.config['ALLOWED_EXTENSIONS'] = {'zip', 'rar', '7z', 'mod', 'jar'}

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
app.config['HOLE_PUNCHING_TIMEOUT'] = 10  # Seconds to wait for hole punching
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

app.config['PEER_TYPES'] = {
    'friends': set(),      # Prioritized connections
    'peers': set(),        # Standard semi-permanent connections
    'strangers': set(),    # Limited connections
    'enemies': set()       # Blocked connections
}
app.config['PEERS'] = set()  # Set to store peer addresses
app.config['DEFAULT_PEERS'] = {'http://www.themoddingtree.com:5000'}  # Default peers

# Add this function to handle chunk downloads with hole punching fallback
def download_chunk(chunk_hash, file_hash=None):
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
                print("download chunk with fallback failed at fail point 1")
                # HTTP failed, try hole punching - THIS IS THE CRITICAL MISSING PART
                try:
                    print(f"HTTP failed for {peer}, attempting hole punching...")
                    if coordinate_hole_punching(peer, file_hash, chunk_hash):
                        # If hole punching succeeded, check if chunk is now available locally
                        time.sleep(5)  # Give time for transfer to complete
                        if os.path.exists(chunk_path):
                            with open(chunk_path, 'rb') as f:
                                return f.read()
                        else:
                            print(f"Hole punching succeeded but chunk {chunk_hash} not received")
                except Exception as e:
                    print("download chunk with fallback failed at fail point 2")
                    print(f"Hole punching failed for {peer}: {e}")
                    continue
    
    raise FileNotFoundError(f"Missing chunk {chunk_hash}")

# Add an endpoint to check chunk hosting
@app.route('/has_chunk/<chunk_hash>')
def has_chunk(chunk_hash):
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    if os.path.exists(chunk_path):
        return jsonify({'has_chunk': True})
    
    # Check database for chunk hosting registration
    db = get_db(app.config['NODE_ID_HEX'])
    cursor = db.cursor()
    cursor.execute(
        'SELECT 1 FROM chunk_hosts WHERE chunk_hash = ? AND node_id = ?',
        (chunk_hash, current_app.config['NODE_ID_HEX'])
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
                print("find_chunk failed at fail point 1")
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
        print("relay_chunk failed at fail point 1")

        return jsonify({'error': f'Relay failed: {str(e)}'}), 500
    
# Add this function to get public IP and port mapping
def get_public_endpoint(stun_servers=None):
    """Get public IP and port using STUN protocol"""
    
    if stun_servers is None:
        stun_servers = app.config['STUN_SERVERS']
    
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
            sock.settimeout(app.config['HOLE_PUNCHING_TIMEOUT'])
            
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
        print("coordinate_hole_punching failed at fail point 1")

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
        sock.settimeout(app.config['HOLE_PUNCHING_TIMEOUT'])
        
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
        sock.settimeout(app.config['HOLE_PUNCHING_TIMEOUT'])
        
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

# Add a function to get hosts for specific chunks
def get_chunk_hosts(chunk_hash):
    """Get all nodes hosting a specific chunk"""
    with app.app_context():
        db = get_db(app.config['NODE_ID_HEX'])
        cursor = db.cursor()
        cursor.execute(
            'SELECT node_id FROM chunk_hosts WHERE chunk_hash = ?',
            (chunk_hash,)
        )
        return [row['node_id'] for row in cursor.fetchall()]

@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)

def maintenance_check():
    """Periodically check file health and pull chunks from remote servers"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    with app.app_context():
        while True:
            time.sleep(current_app.config['MAINTENANCE_INTERVAL'])
            
            db = get_db(app.config['NODE_ID_HEX'])
            cursor = db.cursor()

            try:
                print("Maintenance: Verifying local chunk storage consistency...")
                cursor.execute(
                    'SELECT chunk_hash FROM chunk_hosts WHERE node_id = ?',
                    (current_app.config['NODE_ID_HEX'],)
                )
                
                local_chunks_in_db = [row['chunk_hash'] for row in cursor.fetchall()]
                missing_chunks = []
                
                for chunk_hash in local_chunks_in_db:
                    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                    if not os.path.exists(chunk_path):
                        missing_chunks.append(chunk_hash)
                
                if missing_chunks:
                    print(f"Found {len(missing_chunks)} chunks in DB but not on disk. Pruning records.")
                    placeholders = ','.join(['?'] * len(missing_chunks))
                    delete_query = f'DELETE FROM chunk_hosts WHERE node_id = ? AND chunk_hash IN ({placeholders})'
                    params = [current_app.config['NODE_ID_HEX']] + missing_chunks
                    cursor.execute(delete_query, params)
                    db.commit()
                    print("Pruning complete.")

            except Exception as e:
                print(f"Error during local chunk verification: {e}")

            # Get all peers in priority order: friends first, then peers, then strangers
            all_peers = (
                list(current_app.config['PEER_TYPES']['friends']) +
                list(current_app.config['PEER_TYPES']['peers']) +
                list(current_app.config['PEER_TYPES']['strangers'])
            )
            
            # Verify mutual peer relationships
            for peer_address in list(current_app.config['PEER_TYPES']['peers']):
                try:
                    # Skip default peers
                    if peer_address in current_app.config['DEFAULT_PEERS']:
                        continue
                    
                    # Check if peer is responsive
                    response = requests.get(f"{peer_address}/", timeout=3)
                    if response.status_code != 200:
                        print(f"Peer {peer_address} is unresponsive, demoting to stranger")
                        categorize_peer(peer_address, 'stranger')
                        continue
                        
                    # Verify they have us as a peer too (optional health check)
                    our_address = f"http://{socket.gethostbyname(socket.gethostname())}:5000"
                    response = requests.post(
                        f"{peer_address}/check_peer",
                        json={'peer_address': our_address},
                        timeout=3
                    )
                    
                    if not (response.status_code == 200 and response.json().get('is_peer')):
                        print(f"Peer {peer_address} doesn't have us as peer, demoting to stranger")
                        categorize_peer(peer_address, 'stranger')
                        
                except requests.exceptions.RequestException:
                    print(f"Peer {peer_address} is unresponsive, demoting to stranger")
                    categorize_peer(peer_address, 'stranger')
            
            # Check each file's health
            cursor.execute('SELECT file_hash, original_name, display_name, owner FROM files')
            files = cursor.fetchall()
            
            for file in files:
                file_hash = file['file_hash']
                file_name = file['display_name']
                owner = file['owner']
                
                try:
                    health = calculate_file_health(file_hash)
                    print(f"File '{file_name}' (owned by: {owner[:10]}...) has health score: {health:.1f}")
                    
                    if health < 3.0:
                        print(f"File '{file_name}' needs health improvement (current: {health:.1f})")
                    
                        # Find vulnerable chunks with few hosts
                        cursor.execute('''
                            SELECT c.chunk_hash, COUNT(DISTINCT ch.node_id) as host_count
                            FROM chunks c
                            LEFT JOIN chunk_hosts ch ON c.chunk_hash = ch.chunk_hash
                            WHERE c.file_hash = ?
                            GROUP BY c.chunk_hash
                            HAVING host_count < 3
                            ORDER BY host_count ASC, RANDOM()
                            LIMIT 5
                        ''', (file_hash,))
                        vulnerable_chunks = cursor.fetchall()
                        
                        if not vulnerable_chunks:
                            print(f"Health for '{file_name}' is low, but no specific vulnerable chunks found in local DB.")
                            continue
                            
                        vulnerable_chunk_hashes = [row['chunk_hash'] for row in vulnerable_chunks]

                        # Check if we're already hosting this file
                        cursor.execute(
                            'SELECT 1 FROM chunk_hosts WHERE file_hash = ? AND node_id = ? LIMIT 1',
                            (file_hash, current_app.config['NODE_ID_HEX'])
                        )
                        is_hosting_this_file = cursor.fetchone() is not None

                        if not is_hosting_this_file:
                            print(f"This node does not host '{file_name}'. Attempting to download a chunk to help.")
                            
                            chunk_to_acquire = vulnerable_chunk_hashes[0]
                            chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_to_acquire}.chunk")

                            if os.path.exists(chunk_path):
                                print(f"Logic error: Not registered as host, but chunk {chunk_to_acquire[:10]} exists. Re-registering.")
                                register_hosting(file_hash, chunk_hashes=[chunk_to_acquire])
                                continue

                            try:
                                print(f"Attempting to download chunk {chunk_to_acquire[:10]}...")
                                download_chunk(chunk_to_acquire, file_hash)
                                print(f"Successfully downloaded chunk {chunk_to_acquire[:10]}. Becoming a new host.")
                                register_hosting(file_hash, chunk_hashes=[chunk_to_acquire])
                                broadcast_to_peers({
                                    'file_hash': file_hash,
                                    'node_id': current_app.config['NODE_ID_HEX'],
                                    'chunk_hashes': [chunk_to_acquire]
                                }, 'register_hosting')

                            except FileNotFoundError:
                                print(f"Could not find any peer with chunk {chunk_to_acquire[:10]}. Cannot help at this time.")
                            except Exception as e:
                                print(f"An error occurred while trying to download chunk {chunk_to_acquire[:10]}: {e}")

                        else:
                            print(f"This node hosts '{file_name}'. Attempting to share vulnerable chunks.")
                            
                            peers_already_shared_with = set()

                            for chunk_to_share in vulnerable_chunk_hashes:
                                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_to_share}.chunk")
                                if not os.path.exists(chunk_path):
                                    print(f"Found vulnerable chunk {chunk_to_share[:10]}, but it's not stored locally. Cannot share.")
                                    continue

                                candidate_peer = None
                                for peer in all_peers:
                                    if peer in peers_already_shared_with:
                                        continue

                                    try:
                                        response = requests.post(
                                            f"{peer}/check_chunks",
                                            json={'chunks': [chunk_to_share]},
                                            timeout=2
                                        )
                                        # ### FIX: Robustly handle response printing
                                        try:
                                            print(f"Peer {peer} response: {response.json()}")
                                        except json.JSONDecodeError:
                                            print(f"Peer {peer} sent non-JSON response.")

                                        if response.status_code == 200:
                                            data = response.json()
                                            if chunk_to_share not in data.get('available_chunks', []):
                                                candidate_peer = peer
                                                break # Found a suitable peer for THIS chunk
                                    except requests.exceptions.RequestException as e:
                                        print(f"Request exception checking chunk with {peer}: {e}")
                                        continue
                                
                                if not candidate_peer:
                                    print(f"Could not find any new peer that needs the vulnerable chunk {chunk_to_share[:10]}.")
                                    continue 

                                try:
                                    print(f"Sharing chunk {chunk_to_share[:10]} of '{file_name}' with peer {candidate_peer}")
                                    
                                    cursor.execute('SELECT * FROM files WHERE file_hash = ?', (file_hash,))
                                    file_info = dict(cursor.fetchone())
                                    cursor.execute('SELECT chunk_hash FROM chunks WHERE file_hash = ?', (file_hash,))
                                    file_info['chunks'] = [r['chunk_hash'] for r in cursor.fetchall()]

                                    requests.post(
                                        f"{candidate_peer}/register_file",
                                        json={'file_hash': file_hash, 'file_info': file_info},
                                        timeout=5
                                    ).raise_for_status()

                                    with open(chunk_path, 'rb') as f:
                                        files = {'chunk': (f"{chunk_to_share}.chunk", f)}
                                        requests.post(
                                            f"{candidate_peer}/upload_chunk",
                                            files=files,
                                            timeout=10
                                        ).raise_for_status()
                                    
                                    requests.post(
                                        f"{candidate_peer}/register_hosting",
                                        json={'file_hash': file_hash, 'chunk_hashes': [chunk_to_share]},
                                        timeout=2
                                    ).raise_for_status()

                                    print(f"Successfully shared chunk with {candidate_peer}")
                                    peers_already_shared_with.add(candidate_peer)

                                except requests.exceptions.RequestException as e:
                                    print(f"Failed to share chunk with {candidate_peer}: {e}")
                            
                    elif health > 8.0 and owner != current_app.config['NODE_ID_HEX']:
                        print(f"File {file_name} has excellent health ({health:.1f}), considering stopping hosting")
                        pass

                except Exception as e:
                    print(f"Maintenance error for file '{file_name}' (hash: {file_hash}): {str(e)}")

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
        available_chunks = []
        
        for chunk_hash in data['chunks']:
            chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
            if os.path.exists(chunk_path):
                available_chunks.append(chunk_hash)
                
        return jsonify({
            'status': 'success',
            'available_chunks': available_chunks
        })

# Update the generate_download_stream function to use the new download method
def generate_download_stream(file_hash):
    with app.app_context():
        # Get all chunks for this file in order
        cursor = get_db(app.config['NODE_ID_HEX']).cursor()
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
            (file_hash,)
        )
        chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunk_hashes:
            raise ValueError("No chunks found for this file")
        
        for chunk_hash in chunk_hashes:
            try:
                chunk_data = download_chunk(chunk_hash, file_hash)
                yield chunk_data
            except FileNotFoundError:
                raise FileNotFoundError(f"Missing chunk {chunk_hash} for file {file_hash}")

@app.route('/chunk/<chunk_hash>')
def get_chunk(chunk_hash):
    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    if os.path.exists(chunk_path):
        return send_file(chunk_path, mimetype='application/octet-stream')
    
    # If we don't have the chunk locally, try to get it from other peers with hole punching
    try:
        # Get file_hash from database if possible
        db = get_db(app.config['NODE_ID_HEX'])
        cursor = db.cursor()
        cursor.execute(
            'SELECT file_hash FROM chunks WHERE chunk_hash = ?',
            (chunk_hash,)
        )
        result = cursor.fetchone()
        file_hash = result['file_hash'] if result else None
        
        chunk_data = download_chunk(chunk_hash, file_hash)
        return Response(chunk_data, mimetype='application/octet-stream')
    except FileNotFoundError:
        return "Chunk not found", 404

# --- MODIFIED: More robust chunk upload handling ---
@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    if 'chunk' not in request.files:
        # This part handles hole punching requests, which is separate
        data = request.get_json()
        if data and data.get('type') == 'chunk_transfer_request':
            chunk_hash = data.get('chunk_hash')
            if chunk_hash:
                chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                if os.path.exists(chunk_path):
                    return jsonify({'status': 'ready', 'chunk_size': os.path.getsize(chunk_path)})
                return jsonify({'status': 'not_found'}), 404
        return jsonify({'status': 'error', 'message': 'No chunk provided'}), 400
    
    # Regular HTTP upload from maintenance PUSH strategy
    chunk_file = request.files['chunk']
    # The filename sent is like "hash.chunk", so we get the "hash" part
    chunk_hash = os.path.splitext(secure_filename(chunk_file.filename))[0]
    
    if not chunk_hash:
        return jsonify({'status': 'error', 'message': 'Invalid chunk filename format'}), 400

    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    
    try:
        print(f"Receiving chunk {chunk_hash} to be saved at {chunk_path}")
        chunk_file.save(chunk_path)
        
        # Verify the chunk was saved correctly
        if not os.path.exists(chunk_path):
            print(f"ERROR: Failed to save chunk {chunk_hash} at {chunk_path}")
            return jsonify({'status': 'error', 'message': 'Failed to save chunk on server'}), 500
            
        print(f"Successfully saved chunk {chunk_hash}")
        return jsonify({'status': 'success', 'chunk_hash': chunk_hash})
    except Exception as e:
        print(f"Exception during chunk upload for {chunk_hash}: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/register_hosting', methods=['POST'])
def api_register_hosting():
    data = request.json
    if data and 'file_hash' in data:
        chunk_hashes = data.get('chunk_hashes')  # Optional: specific chunks
        register_hosting(data['file_hash'], data.get('node_id'), chunk_hashes)
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
        init_db(app.config['NODE_ID_HEX'])
        load_peers_from_db(app.config['NODE_ID_HEX'])
        app.register_blueprint(peerblueprint)
        app.register_blueprint(fileblueprint)
        
    # Start maintenance thread
    start_maintenance_thread()
    
    app.run(debug=True, host='0.0.0.0', port=5000)
