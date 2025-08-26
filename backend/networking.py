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
from database import get_db, init_db, close_db

def register_hosting(file_hash, node_id=None, chunk_hashes=None):
    """Register that this node is hosting a file or specific chunks"""
    if node_id is None:
        node_id = current_app.config['NODE_ID_HEX']  # Use hex representation
    
    db = get_db(current_app.config['NODE_ID_HEX'])
    cursor = db.cursor()
    
    if chunk_hashes is None:
        # Register hosting for all chunks of the file (backward compatibility)
        cursor.execute(
            'INSERT OR REPLACE INTO hosts (file_hash, node_id) VALUES (?, ?)',
            (file_hash, node_id)
        )
        
        # Also register for all chunks individually
        cursor.execute(
            'SELECT chunk_hash FROM chunks WHERE file_hash = ?',
            (file_hash,)
        )
        chunks = [row['chunk_hash'] for row in cursor.fetchall()]
        
        for chunk_hash in chunks:
            cursor.execute(
                'INSERT OR REPLACE INTO chunk_hosts (chunk_hash, node_id, file_hash) VALUES (?, ?, ?)',
                (chunk_hash, node_id, file_hash)
            )
    else:
        # Register hosting for specific chunks only
        for chunk_hash in chunk_hashes:
            cursor.execute(
                'INSERT OR REPLACE INTO chunk_hosts (chunk_hash, node_id, file_hash) VALUES (?, ?, ?)',
                (chunk_hash, node_id, file_hash)
            )
    
    db.commit()

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
                print("download chunk with relay failed at fail point 1")
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
                        print("download chunk with relay failed at fail point 2")
                        continue
                    
        except requests.exceptions.RequestException:
            print("download chunk with relay failed at fail point 3")
            continue
    
    raise FileNotFoundError(f"Missing chunk {chunk_hash}")
