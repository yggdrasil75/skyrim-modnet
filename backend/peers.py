import requests
import sqlite3
from flask import request, redirect, url_for, jsonify, current_app, Blueprint

from database import get_db

peerblueprint = Blueprint('peers', __name__)

# Store config keys as blueprint attributes for easy access
peerblueprint.PEER_TYPES_KEY = 'PEER_TYPES'
peerblueprint.PEERS_KEY = 'PEERS'
peerblueprint.DEFAULT_PEERS_KEY = 'DEFAULT_PEERS'

@peerblueprint.record_once
def on_load(state):
    """Initialize configuration when blueprint is registered"""
    app = state.app
    # Set default configuration if not already set
    if peerblueprint.PEER_TYPES_KEY not in app.config:
        app.config[peerblueprint.PEER_TYPES_KEY] = {
            'friends': set(),      # Prioritized connections
            'peers': set(),        # Standard semi-permanent connections
            'strangers': set(),    # Limited connections
            'enemies': set()       # Blocked connections
        }
    if peerblueprint.PEERS_KEY not in app.config:
        app.config[peerblueprint.PEERS_KEY] = set()
    if peerblueprint.DEFAULT_PEERS_KEY not in app.config:
        app.config[peerblueprint.DEFAULT_PEERS_KEY] = {'http://www.themoddingtree.com:5000'}

# Helper function to access app config through current_app
def get_peer_config(key):
    return current_app.config.get(key)

def get_peer_types():
    return get_peer_config(peerblueprint.PEER_TYPES_KEY)

def get_peers():
    return get_peer_config(peerblueprint.PEERS_KEY)

def get_default_peers():
    return get_peer_config(peerblueprint.DEFAULT_PEERS_KEY)

# API endpoint to change peer type
@peerblueprint.route('/set_peer_type', methods=['POST'])
def set_peer_type():
    peer_address = request.form.get('peer_address')
    peer_type = request.form.get('peer_type')
    
    if not peer_address or not peer_type:
        return "Missing parameters", 400
    
    try:
        categorize_peer(peer_address, peer_type)
        return redirect(url_for('peers'))  # You might want to change this to a blueprint-specific endpoint
    except ValueError as e:
        return str(e), 400

# Add endpoint to check peer status
@peerblueprint.route('/check_peer', methods=['POST'])
def check_peer():
    """Check if we have a specific peer and what type"""
    data = request.get_json()
    if not data or 'peer_address' not in data:
        return jsonify({'status': 'error'}), 400
    
    peer_address = data['peer_address']
    
    db = get_db()  # You'll need to handle database access appropriately
    cursor = db.cursor()
    cursor.execute(
        'SELECT peer_type FROM peers WHERE peer_address = ?',
        (peer_address,)
    )
    result = cursor.fetchone()
    
    if result:
        return jsonify({
            'is_peer': True,
            'peer_type': result['peer_type']
        })
    else:
        return jsonify({'is_peer': False})

def broadcast_to_peers(data, endpoint):
    """Broadcast data to all known peers"""
    for peer in get_peers():
        try:
            requests.post(f"{peer}/{endpoint}", json=data, timeout=2)
        except requests.exceptions.RequestException:
            print("broadcast_to_peers failed at fail point 1")
            continue

# Update the peer loading function
def load_peers_from_db(nodeid):
    """Load known peers from database at startup and categorize them"""
    db = get_db(nodeid)
    cursor = db.cursor()
    cursor.execute('SELECT peer_address, peer_type FROM peers')
    
    # Clear all peer sets
    peer_types = get_peer_types()
    for peer_type in peer_types:
        peer_types[peer_type].clear()
    
    # Add peers from database to their respective sets
    for row in cursor.fetchall():
        peer_type = row['peer_type'] + 's'  # Convert to plural form
        if peer_type in peer_types:
            peer_types[peer_type].add(row['peer_address'])
    
    # Add default peers as regular peers if they're not already present
    default_peers = get_default_peers()
    for peer in default_peers:
        if not any(peer in peers for peers in peer_types.values()):
            peer_types['peers'].add(peer)
            try:
                cursor.execute(
                    'INSERT OR IGNORE INTO peers (peer_address, peer_type) VALUES (?, ?)',
                    (peer, 'peer')
                )
                db.commit()
            except sqlite3.Error as e:
                print(f"Error adding default peer to database: {e}")

# Update the categorize_peer function to handle mutual relationships properly
def categorize_peer(peer_address, peer_type):
    """Categorize a peer into one of the types"""
    valid_types = ['friend', 'peer', 'stranger', 'enemy']
    if peer_type not in valid_types:
        raise ValueError(f"Invalid peer type. Must be one of {valid_types}")
    
    # Remove from all categories first
    peer_types = get_peer_types()
    for pt in peer_types:
        if peer_address in peer_types[pt]:
            peer_types[pt].remove(peer_address)
    
    # Add to the specified category
    plural_type = peer_type + 's'
    if plural_type in peer_types:
        peer_types[plural_type].add(peer_address)
    
    # Update database
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO peers (peer_address, peer_type) VALUES (?, ?)',
        (peer_address, peer_type)
    )
    db.commit()

# Add a new endpoint for remote peer addition (to avoid circular notifications)
@peerblueprint.route('/add_peer_remote', methods=['POST'])
def add_peer_remote():
    """Endpoint for other nodes to add us as a peer (prevents infinite loops)"""
    data = request.get_json()
    if not data or 'peer_address' not in data:
        return jsonify({'status': 'error', 'message': 'Missing peer_address'}), 400
    
    peer_address = data['peer_address']
    peer_type = data.get('peer_type', 'peer')  # Default to peer
    
    # Don't allow remote addition of friends - only peers
    if peer_type != 'peer':
        return jsonify({'status': 'error', 'message': 'Can only add peers remotely'}), 400
    
    # Don't add ourselves
    our_address = request.host_url.rstrip('/')
    if not our_address.startswith('http'):
        our_address = f"http://{our_address}"
    
    if peer_address == our_address:
        return jsonify({'status': 'error', 'message': 'Cannot add self'}), 400
    
    # Don't allow modification of default peers
    default_peers = get_default_peers()
    if peer_address in default_peers:
        return jsonify({'status': 'error', 'message': 'Cannot modify default peers'}), 400
    
    try:
        # Check if we already have this peer
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            'SELECT peer_type FROM peers WHERE peer_address = ?',
            (peer_address,)
        )
        existing = cursor.fetchone()
        
        # Only add if not already present or if it's currently an enemy
        if not existing or existing['peer_type'] == 'enemy':
            categorize_peer(peer_address, 'peer')
            return jsonify({'status': 'success', 'message': 'Peer added'})
        else:
            return jsonify({'status': 'success', 'message': 'Peer already exists'})
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Update the add_peer function to make peer relationships mutual
@peerblueprint.route('/add_peer', methods=['POST'])
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
    default_peers = get_default_peers()
    if peer_address in default_peers:
        return "Cannot modify default peers", 400
    
    try:
        categorize_peer(peer_address, peer_type)
        
        # For peer relationships (not friends), make it mutual by notifying the other node
        if peer_type == 'peer':
            try:
                our_address = request.host_url.rstrip('/')
                if not our_address.startswith('http'):
                    our_address = f"http://{our_address}"
                
                # Notify the peer to add us as a peer (not friend)
                requests.post(f"{peer_address}/add_peer_remote", 
                             json={
                                 'peer_address': our_address,
                                 'peer_type': 'peer'
                             },
                             timeout=5)
            except requests.exceptions.RequestException as e:
                print(f"Failed to notify peer {peer_address}: {e}")
                # Still continue even if notification fails
            
        return redirect(url_for('peers'))  # You might want to change this
    except ValueError as e:
        return str(e), 400