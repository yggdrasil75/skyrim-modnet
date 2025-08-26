import os
import hashlib
import requests
import threading
import time
import sqlite3
from datetime import datetime
from flask import Blueprint, Flask, Response, render_template, request, redirect, url_for, send_file, jsonify, g, current_app
from werkzeug.utils import secure_filename
import socket
import time
import struct
import json

from database import get_db
from peers import broadcast_to_peers
from networking import register_hosting, download_chunk_with_relay, render_template

fileblueprint = Blueprint('files', __name__)

# Helper functions with node ID as salt for cryptographic operations
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']

def hash_with_node_id(data):
    """Hash data with the node ID as salt for consistent hashing"""
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha256(data + current_app.config['NODE_ID']).hexdigest()

def split_file(filepath, file_hash):
    """Split file into chunks and return chunk hashes"""
    with current_app.app_context():
        chunks = []
        sequence = 0
        db = get_db(current_app.config['NODE_ID_HEX'])
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
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
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
                        print("reassemble_file failed at fail point 1")
                        continue
                
                if not chunk_found:
                    raise FileNotFoundError(f"Missing chunk {chunk_hash} for file {file_hash}")
        
        return True

# Update the get_file_hosts function to use chunk-level information
def get_file_hosts(file_hash):
    """Get all nodes hosting a specific file, based on chunk availability"""
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
        cursor = db.cursor()
        
        # Get all chunks for this file
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ?',
            (file_hash,)
        )
        chunks = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunks:
            return []
        
        # For each chunk, find hosts and calculate file-level hosting
        chunk_hosts = {}
        for chunk_hash in chunks:
            cursor.execute(
                'SELECT node_id FROM chunk_hosts WHERE chunk_hash = ?',
                (chunk_hash,)
            )
            hosts = [row['node_id'] for row in cursor.fetchall()]
            chunk_hosts[chunk_hash] = hosts
        
        # A node is considered to host the file if it has at least one chunk
        all_hosts = set()
        for hosts in chunk_hosts.values():
            all_hosts.update(hosts)
        
        return list(all_hosts)

def get_shared_files():
    """Get all files shared in the network with health scores"""
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
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

# Update the calculate_file_health function to use chunk-level hosting data
def calculate_file_health(file_hash):
    """Calculate file health score based on chunk-level hosting"""
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
        cursor = db.cursor()
        
        # Get all chunks for this file
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ?',
            (file_hash,)
        )
        chunks = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunks:
            return 0.0  # No chunks means unhealthy file
        
        # Get host count for each chunk from chunk_hosts table
        chunk_host_counts = []
        for chunk_hash in chunks:
            cursor.execute(
                'SELECT COUNT(DISTINCT node_id) as host_count FROM chunk_hosts WHERE chunk_hash = ?',
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

@fileblueprint.route('/upload', methods=['POST'])
def upload_file():
    with current_app.app_context():
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
            
            db = get_db(current_app.config['NODE_ID_HEX'])
            cursor = db.cursor()

            # Proactively check if the file hash already exists
            cursor.execute(
                'SELECT display_name FROM files WHERE file_hash = ?',
                (file_hash,)
            )
            existing_file = cursor.fetchone()

            if existing_file:
                # If it exists, clean up the temp file and show the duplicate page
                os.remove(filepath)
                return render_template(
                    'duplicate_file.html', 
                    existing_name=existing_file['display_name']
                )

            # If we reach here, the file is not a duplicate. Proceed with the upload.
            try:
                # Hash the password with node ID as salt
                password_hash = hash_with_node_id(request.form['password'])
                
                # Split file into chunks
                chunk_hashes = split_file(filepath, file_hash)
                
                # Store file metadata in database
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

            except sqlite3.IntegrityError:
                # This is a fallback, in case a race condition occurs.
                # The proactive check above should catch 99.9% of cases.
                db.rollback()
                os.remove(filepath)
                # We can't be sure what the existing name is here, so give a generic message.
                return "A duplicate file was detected during a race condition. Please try again.", 409
            

            # Clean up original file (we only keep chunks)
            os.remove(filepath)
            
            # Show success message with password reminder
            return render_template('upload_success.html', 
                                file_name=request.form['display_name'],
                                password=request.form['password'])
        
        return redirect(request.url)

@fileblueprint.route('/download/<file_hash>')
def download_file(file_hash):
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
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
                        print("download file failed at breakpoint 1")
                        continue
                else:
                    continue
                break
            else:
                return "File not found", 404

        # Get all chunk hashes before creating the generator
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ? ORDER BY sequence',
            (file_hash,)
        )
        chunk_hashes = [row['chunk_hash'] for row in cursor.fetchall()]
        
        if not chunk_hashes:
            return "No chunks found for this file", 404

        # Create a generator to stream the file
        def generate():
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
                                # Stream the chunk content
                                for chunk in response.iter_content(chunk_size=8192):
                                    yield chunk
                                # Save chunk locally for future requests
                                with open(chunk_path, 'wb') as f:
                                    # Need to re-fetch since we already consumed the stream
                                    chunk_response = requests.get(f"{peer}/chunk/{chunk_hash}", timeout=5)
                                    if chunk_response.status_code == 200:
                                        f.write(chunk_response.content)
                                chunk_found = True
                                break
                        except requests.exceptions.RequestException:
                            print("download file failed at breakpoint generate.1")
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

@fileblueprint.route('/register_file', methods=['POST'])
def register_file():
    ''' First try to get JSON data'''
    with current_app.app_context():
        if request.content_type == 'application/json':
            data = request.get_json()
            
            # Handle JSON registration
            if 'file_hash' in data and 'file_info' in data:
                db = get_db(current_app.config['NODE_ID_HEX'])
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
                db = get_db(current_app.config['NODE_ID_HEX'])
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

@fileblueprint.route('/update_file/<file_hash>', methods=['POST'])
def update_file(file_hash):
    with current_app.app_context():
        if 'file' not in request.files:
            return "No file provided", 400
        
        password = request.form.get('password')
        if not password:
            return "Password required for updates", 401
        
        db = get_db(current_app.config['NODE_ID_HEX'])
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

@fileblueprint.route('/file_info/<file_hash>')
def get_file_info(file_hash):
    with current_app.app_context():
        db = get_db(current_app.config['NODE_ID_HEX'])
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

@fileblueprint.route('/shared_files')
def list_shared_files():
    files = get_shared_files()
    return jsonify({file['file_hash']: dict(file) for file in files})