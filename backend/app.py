import os
import hashlib
import requests
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify
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

# Simulated distributed storage (in reality this would be across nodes)
shared_files = {}
file_chunks = {}

# Helper functions
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def split_file(filepath):
    """Split file into chunks and return chunk hashes"""
    chunks = []
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
            
            chunks.append(chunk_hash)
    return chunks

def reassemble_file(chunk_hashes, output_path):
    """Reassemble file from chunks"""
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

def maintenance_check():
    """Periodically check file health and redistribute as needed"""
    while True:
        time.sleep(app.config['MAINTENANCE_INTERVAL'])
        print(f"[{datetime.now()}] Running maintenance check...")
        
        # Check each file's health
        for file_hash, file_info in list(shared_files.items()):
            try:
                # Get peer reports for this file
                peer_reports = []
                for peer in app.config['PEERS']:
                    try:
                        response = requests.get(f"{peer}/file_info/{file_hash}", timeout=2)
                        if response.status_code == 200:
                            peer_reports.append(response.json())
                    except requests.exceptions.RequestException:
                        continue
                
                # Count unique hosts for this file (including ourselves)
                host_count = 1  # We have it
                for report in peer_reports:
                    if report.get('owner') != app.config['NODE_ID']:
                        host_count += 1
                
                print(f"File {file_info['name']} has {host_count} hosts")
                
                # If we have too few hosts (less than 3), share with peers
                if host_count < 3:
                    print(f"File {file_info['name']} needs more hosts (only {host_count})")
                    # Find peers that don't have this file yet
                    candidates = []
                    for peer in app.config['PEERS']:
                        peer_has_file = any(r.get('owner') == peer for r in peer_reports)
                        if not peer_has_file:
                            candidates.append(peer)
                    
                    # Share with enough peers to reach minimum
                    needed = min(3 - host_count, len(candidates))
                    for i in range(needed):
                        peer = candidates[i]
                        try:
                            # First send the file info
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
                            print(f"Shared {file_info['name']} with {peer}")
                        except requests.exceptions.RequestException as e:
                            print(f"Failed to share with {peer}: {e}")
                
                # If we have too many hosts (more than 10), consider removing our copy
                elif host_count > 10 and file_info['owner'] == app.config['NODE_ID']:
                    print(f"File {file_info['name']} has enough hosts ({host_count}), we can stop hosting")
                    # In a real implementation, we might remove our copy here
                    # But for this example, we'll just log it
            
            except Exception as e:
                print(f"Error during maintenance for {file_info.get('name', 'unknown')}: {e}")
        
        # Check for new files from peers
        for peer in app.config['PEERS']:
            try:
                response = requests.get(f"{peer}/shared_files", timeout=2)
                if response.status_code == 200:
                    peer_files = response.json()
                    for file_hash, file_info in peer_files.items():
                        if file_hash not in shared_files:
                            shared_files[file_hash] = file_info
                            print(f"Discovered new file from {peer}: {file_info['name']}")
            except requests.exceptions.RequestException:
                continue

# Routes
@app.route('/')
def index():
    return render_template('index.html', files=shared_files.items(), peers=app.config['PEERS'])

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
        
        # Split file into chunks
        chunk_hashes = split_file(filepath)
        
        # Store file metadata
        file_hash = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
        shared_files[file_hash] = {
            'name': filename,
            'size': os.path.getsize(filepath),
            'chunks': chunk_hashes,
            'owner': app.config['NODE_ID']
        }
        
        # Broadcast file metadata to peers
        broadcast_to_peers({
            'file_hash': file_hash,
            'file_info': shared_files[file_hash]
        }, 'register_file')
        
        # Clean up original file (we only keep chunks)
        os.remove(filepath)
        
        return redirect(url_for('index'))
    
    return redirect(request.url)

@app.route('/download/<file_hash>')
def download_file(file_hash):
    if file_hash not in shared_files:
        # Check if any peers have this file
        for peer in app.config['PEERS']:
            try:
                response = requests.get(f"{peer}/file_info/{file_hash}")
                if response.status_code == 200:
                    shared_files[file_hash] = response.json()
                    break
            except requests.exceptions.RequestException:
                continue
        else:
            return "File not found", 404
    
    file_info = shared_files[file_hash]
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{file_info['name']}")
    
    # Reassemble file from chunks
    reassemble_file(file_info['chunks'], temp_path)
    
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
        if data['file_hash'] not in shared_files:
            shared_files[data['file_hash']] = data['file_info']
        return jsonify({'status': 'success'})
    return jsonify({'status': 'error'}), 400

@app.route('/file_info/<file_hash>')
def get_file_info(file_hash):
    if file_hash in shared_files:
        return jsonify(shared_files[file_hash])
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
def get_shared_files():
    return jsonify(shared_files)

def start_maintenance_thread():
    """Start the background maintenance thread"""
    thread = threading.Thread(target=maintenance_check)
    thread.daemon = True
    thread.start()

if __name__ == '__main__':
    # Create upload directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Start maintenance thread
    start_maintenance_thread()
    
    app.run(debug=True, host='0.0.0.0', port=5000)