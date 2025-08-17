import os
import hashlib
import requests
import threading
import time
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, g
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder="../templates")

# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024  # 16MB max
app.config['CHUNK_SIZE'] = 256 * 1024  # 256KB chunks
app.config['ALLOWED_EXTENSIONS'] = {'zip', 'rar', '7z', 'mod', 'jar'}
app.config['PEERS'] = set()  # Set to store peer addresses
app.config['NODE_ID'] = hashlib.sha256(os.urandom(32)).hexdigest()[:8]  # Unique node ID
app.config['MAINTENANCE_INTERVAL'] = 60  # Seconds between maintenance checks
app.config['DATABASE'] = './database.db'

# Database setup
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(app.config['DATABASE'])
        db.row_factory = sqlite3.Row
    return db

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_hash TEXT PRIMARY KEY,
            name TEXT NOT NULL,
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

# Helper functions
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def split_file(filepath, file_hash):
    """Split file into chunks and return chunk hashes"""
    chunks = []
    sequence = 0
    db = get_db()
    cursor = db.cursor()
    
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(app.config['CHUNK_SIZE'])
            if not chunk:
                break
            chunk_hash = hashlib.sha256(chunk).hexdigest()
            chunk_filename = f"{chunk_hash}.chunk"
            chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], chunk_filename)
            
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
    db = get_db()
    cursor = db.cursor()
    
    # Get all chunks for this file in order
    cursor.execute(
        'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
        (file_hash,)
    )
    chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
    
    with open(output_path, 'wb') as outfile:
        for chunk_hash in chunk_hashes:
            chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as chunk_file:
                    outfile.write(chunk_file.read())
            else:
                # Try to fetch chunk from peers
                for peer in app.config['PEERS']:
                    try:
                        response = requests.get(f"{peer}/chunk/{chunk_hash}")
                        if response.status_code == 200:
                            outfile.write(response.content)
                            # Save chunk locally for future requests
                            with open(chunk_path, 'wb') as chunk_file:
                                chunk_file.write(response.content)
                            break
                    except requests.exceptions.RequestException:
                        continue

def broadcast_to_peers(data, endpoint):
    """Broadcast data to all known peers"""
    for peer in app.config['PEERS']:
        try:
            requests.post(f"{peer}/{endpoint}", json=data, timeout=2)
        except requests.exceptions.RequestException:
            continue

def register_hosting(file_hash, node_id=None):
    """Register that this node is hosting a file"""
    if node_id is None:
        node_id = app.config['NODE_ID']
    
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO hosts (file_hash, node_id) VALUES (?, ?)',
        (file_hash, node_id)
    )
    db.commit()

def get_file_hosts(file_hash):
    """Get all nodes hosting a specific file"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        'SELECT node_id FROM hosts WHERE file_hash = ?',
        (file_hash,)
    )
    return [row['node_id'] for row in cursor.fetchall()]

def get_shared_files():
    """Get all files shared in the network"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        SELECT f.file_hash, f.name, f.size, f.owner, 
               COUNT(h.node_id) as host_count
        FROM files f
        LEFT JOIN hosts h ON f.file_hash = h.file_hash
        GROUP BY f.file_hash
    ''')
    return cursor.fetchall()

def maintenance_check():
    """Periodically check file health and redistribute as needed"""
    while True:
        time.sleep(app.config['MAINTENANCE_INTERVAL'])
        print(f"[{datetime.now()}] Running maintenance check...")
        
        # Check each file's health
        db = get_db()
        cursor = db.cursor()
        cursor.execute('SELECT file_hash, name, owner FROM files')
        files = cursor.fetchall()
        
        for file in files:
            file_hash = file['file_hash']
            file_name = file['name']
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
                    for peer in app.config['PEERS']:
                        if peer not in hosts:
                            candidates.append(peer)
                    
                    # Share with enough peers to reach minimum
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
                            
                            requests.post(f"{peer}/register_file", json={
                                'file_hash': file_hash,
                                'file_info': file_info
                            }, timeout=5)
                            
                            # Then send all chunks
                            for chunk_hash in file_info['chunks']:
                                chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
                                if os.path.exists(chunk_path):
                                    with open(chunk_path, 'rb') as f:
                                        requests.post(f"{peer}/upload_chunk", files={
                                            'chunk': (f"{chunk_hash}.chunk", f)
                                        }, timeout=5)
                            
                            # Register that this peer is now hosting the file
                            requests.post(f"{peer}/register_hosting", json={
                                'file_hash': file_hash
                            }, timeout=2)
                            
                            print(f"Shared {file_name} with {peer}")
                        except requests.exceptions.RequestException as e:
                            print(f"Failed to share with {peer}: {e}")
                
                # If we have too many hosts (more than 10), consider removing our copy
                elif host_count > 10 and owner == app.config['NODE_ID']:
                    print(f"File {file_name} has enough hosts ({host_count}), we can stop hosting")
                    # In a real implementation, we might remove our copy here
                    # But for this example, we'll just log it
            
            except Exception as e:
                print(f"Error during maintenance for {file_name}: {e}")
        
        # Check for new files from peers
        for peer in app.config['PEERS']:
            try:
                response = requests.get(f"{peer}/shared_files", timeout=2)
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
            except requests.exceptions.RequestException:
                continue

# Routes
@app.route('/')
def index():
    files = get_shared_files()
    return render_template('index.html', files=files, peers=app.config['PEERS'])

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Calculate file hash
        file_hash = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
        
        # Split file into chunks
        chunk_hashes = split_file(filepath, file_hash)
        
        # Store file metadata in database
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'INSERT INTO files (file_hash, name, size, owner) VALUES (?, ?, ?, ?)',
            (file_hash, filename, os.path.getsize(filepath), app.config['NODE_ID'])
        )
        db.commit()
        
        # Register that we're hosting this file
        register_hosting(file_hash)
        
        # Broadcast file metadata to peers
        broadcast_to_peers({
            'file_hash': file_hash,
            'file_info': {
                'name': filename,
                'size': os.path.getsize(filepath),
                'chunks': chunk_hashes,
                'owner': app.config['NODE_ID']
            }
        }, 'register_file')
        
        # Clean up original file (we only keep chunks)
        os.remove(filepath)
        
        return redirect(url_for('index'))
    
    return redirect(request.url)

@app.route('/download/<file_hash>')
def download_file(file_hash):
    db = get_db()
    cursor = db.cursor()
    
    # Check if we have the file in our database
    cursor.execute(
        'SELECT name FROM files WHERE file_hash = ?',
        (file_hash,)
    )
    file_info = cursor.fetchone()
    
    if not file_info:
        # Check if any peers have this file
        for peer in app.config['PEERS']:
            try:
                response = requests.get(f"{peer}/file_info/{file_hash}")
                if response.status_code == 200:
                    file_data = response.json()
                    # Store the file info in our database
                    cursor.execute(
                        'INSERT INTO files (file_hash, name, size, owner) VALUES (?, ?, ?, ?)',
                        (file_hash, file_data['name'], file_data['size'], file_data['owner'])
                    )
                    db.commit()
                    file_info = {'name': file_data['name']}
                    break
            except requests.exceptions.RequestException:
                continue
        else:
            return "File not found", 404
    
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{file_info['name']}")
    
    # Reassemble file from chunks
    reassemble_file(file_hash, temp_path)
    
    # Send file to user
    response = send_file(temp_path, as_attachment=True, download_name=file_info['name'])
    
    # Clean up temporary file
    os.remove(temp_path)
    
    return response

@app.route('/add_peer', methods=['POST'])
def add_peer():
    peer_address = request.form.get('peer_address')
    if peer_address and peer_address not in app.config['PEERS']:
        app.config['PEERS'].add(peer_address)
        
        # Store peer in database
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO peers (peer_address) VALUES (?)',
            (peer_address,)
        )
        db.commit()
        
        # Notify the new peer about our existence
        try:
            requests.post(f"{peer_address}/add_peer", json={'peer_address': request.host_url})
        except requests.exceptions.RequestException:
            pass
        
        return redirect(url_for('index'))
    return "Invalid peer address", 400

@app.route('/register_file', methods=['POST'])
def register_file():
    data = request.json
    if data and 'file_hash' in data and 'file_info' in data:
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
                'INSERT INTO files (file_hash, name, size, owner) VALUES (?, ?, ?, ?)',
                (data['file_hash'], data['file_info']['name'], 
                 data['file_info']['size'], data['file_info']['owner'])
            )
            
            # Store the chunks
            for sequence, chunk_hash in enumerate(data['file_info']['chunks']):
                cursor.execute(
                    'INSERT OR IGNORE INTO chunks (chunk_hash, file_hash, sequence) VALUES (?, ?, ?)',
                    (chunk_hash, data['file_hash'], sequence)
                )
            
            db.commit()
        
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 400

@app.route('/file_info/<file_hash>')
def get_file_info(file_hash):
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute(
        'SELECT file_hash, name, size, owner FROM files WHERE file_hash = ?',
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
            'name': file_info['name'],
            'size': file_info['size'],
            'owner': file_info['owner'],
            'chunks': chunks
        })
    return "File not found", 404

@app.route('/chunk/<chunk_hash>')
def get_chunk(chunk_hash):
    chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    if os.path.exists(chunk_path):
        return send_file(chunk_path)
    return "Chunk not found", 404

@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    if 'chunk' not in request.files:
        return "No chunk provided", 400
    
    chunk_file = request.files['chunk']
    chunk_hash = os.path.splitext(chunk_file.filename)[0]
    chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
    
    chunk_file.save(chunk_path)
    return jsonify({'status': 'success', 'chunk_hash': chunk_hash})

@app.route('/shared_files')
def list_shared_files():
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

def load_peers_from_db():
    """Load known peers from database at startup"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute('SELECT peer_address FROM peers')
    for row in cursor.fetchall():
        app.config['PEERS'].add(row['peer_address'])

if __name__ == '__main__':
    # Create upload directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Initialize database
    init_db()
    
    # Load known peers from database
    load_peers_from_db()
    
    # Start maintenance thread
    start_maintenance_thread()
    
    app.run(debug=True, host='0.0.0.0', port=5000)