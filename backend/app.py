import os
import hashlib
import requests
import threading
import time
import sqlite3
from datetime import datetime
from flask import Flask, Response, render_template, request, redirect, url_for, send_file, jsonify, g, current_app
from werkzeug.utils import secure_filename

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
    """Get all files shared in the network"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            SELECT f.file_hash, f.original_name, f.display_name, f.size, f.owner, 
                   COUNT(h.node_id) as host_count
            FROM files f
            LEFT JOIN hosts h ON f.file_hash = h.file_hash
            GROUP BY f.file_hash
        ''')
        return cursor.fetchall()

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
    """Periodically check file health and redistribute as needed"""
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    with app.app_context():
        while True:
            time.sleep(current_app.config['MAINTENANCE_INTERVAL'])
            print(f"[{datetime.now()}] Running maintenance check...")
            
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
                file_name = file['display_name']  # Using display_name instead of name
                owner = file['owner']
                
                try:
                    # Get all hosts for this file
                    hosts = get_file_hosts(file_hash)
                    host_count = len(hosts)
                    
                    print(f"File {file_name} has {host_count} hosts")
                    
                    # If we have too few hosts (less than 3), share with peers
                    if host_count < 3:
                        print(f"File {file_name} needs more hosts (only {host_count})")
                        # Find peers that don't have this file yet
                        candidates = []
                        for peer in all_peers:
                            peer_id = peer.split('//')[-1].replace(':', '_')  # Create a simple ID from the peer address
                            if peer_id not in hosts:
                                candidates.append(peer)
                        
                        # Share with enough peers to reach minimum, prioritizing friends
                        needed = min(3 - host_count, len(candidates))
                        for i in range(needed):
                            peer = candidates[i]
                            try:
                                # First send the file info
                                cursor.execute(
                                    'SELECT * FROM files WHERE file_hash = ?',
                                    (file_hash,)
                                )
                                file_info = dict(cursor.fetchone())
                                
                                # Get chunks
                                cursor.execute(
                                    'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
                                    (file_hash,)
                                )
                                file_info['chunks'] = [row['chunk_hash'] for row in cursor.fetchall()]
                                
                                # Ensure the peer URL is properly formatted
                                register_url = f"{peer}/register_file"
                                print(f"Sending file info to {register_url}")
                                response = requests.post(register_url, json={
                                    'file_hash': file_hash,
                                    'file_info': file_info
                                }, timeout=5)
                                response.raise_for_status()
                                
                                # Then send all chunks as binary data
                                for chunk_hash in file_info['chunks']:
                                    chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                                    if os.path.exists(chunk_path):
                                        with open(chunk_path, 'rb') as f:
                                            upload_url = f"{peer}/upload_chunk"
                                            print(f"Sending chunk to {upload_url}")
                                            files = {'chunk': (f"{chunk_hash}.chunk", f)}
                                            response = requests.post(upload_url, files=files, timeout=5)
                                            response.raise_for_status()
                                
                                # Register that this peer is now hosting the file
                                host_url = f"{peer}/register_hosting"
                                print(f"Registering hosting with {host_url}")
                                response = requests.post(host_url, json={
                                    'file_hash': file_hash,
                                    'node_id': current_app.config['NODE_ID_HEX']  # Using hex representation for JSON
                                }, timeout=2)
                                response.raise_for_status()
                                
                                print(f"Successfully shared {file_name} with {peer}")
                            except requests.exceptions.RequestException as e:
                                print(f"Failed to share with {peer}: {str(e)}")
                    
                    # If we have too many hosts (more than 10), consider removing our copy
                    elif host_count > 10 and owner == current_app.config['NODE_ID_HEX']:
                        print(f"File {file_name} has enough hosts ({host_count}), we can stop hosting")
                
                except Exception as e:
                    print(f"Error during maintenance for {file_name}: {str(e)}")
            
            # Check for new files from peers (only friends and regular peers)
            for peer_type in ['friends', 'peers']:
                for peer in current_app.config['PEER_TYPES'][peer_type]:
                    try:
                        files_url = f"{peer}/shared_files"
                        print(f"Checking for new files at {files_url}")
                        response = requests.get(files_url, timeout=2)
                        if response.status_code == 200:
                            peer_files = response.json()
                            for file_hash, file_info in peer_files.items():
                                # Check if we already know about this file
                                cursor.execute(
                                    'SELECT 1 FROM files WHERE file_hash = ?',
                                    (file_hash,)
                                )
                                if not cursor.fetchone():
                                    # New file discovered
                                    cursor.execute(
                                        'INSERT INTO files (file_hash, name, size, owner) VALUES (?, ?, ?, ?)',
                                        (file_hash, file_info['name'], file_info['size'], file_info['owner'])
                                    )
                                    db.commit()
                                    print(f"Discovered new file from {peer}: {file_info['name']}")
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to check for files from {peer}: {str(e)}")
                        continue

# Routes
@app.route('/')
def index():
    with app.app_context():
        files = get_shared_files()
        # Convert SQLite rows to dictionaries
        files_data = {row['file_hash']: dict(row) for row in files}
        return render_template('index.html', 
                            files=files_data, 
                            peer_types=current_app.config['PEER_TYPES'])
    
# Updated download function to prioritize friends for chunk retrieval
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
                            for chunk in response.iter_content(chunk_size=8192):
                                yield chunk
                            # Save chunk locally for future requests
                            chunk_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
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
                        'INSERT INTO files (file_hash, original_name, display_name, size, owner) VALUES (?, ?, ?, ?, ?)',
                        (data['file_hash'], 
                         data['file_info'].get('original_name', data['file_info'].get('name', 'unknown')),
                         data['file_info'].get('display_name', data['file_info'].get('name', 'unknown')),
                         data['file_info']['size'], 
                         data['file_info']['owner'])
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
    return "Chunk not found", 404

@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    if 'chunk' not in request.files:
        return jsonify({'status': 'error', 'message': 'No chunk provided'}), 400
    
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
    
    app.run(debug=True, host='0.0.0.0', port=5000)