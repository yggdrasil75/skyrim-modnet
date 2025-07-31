import os
import json
import socket
import struct
import time
from collections import defaultdict

import requests

class NodeConfig:
    def __init__(self, node_id, config_path='node_config.json'):
        self.node_id = node_id
        self.config_path = config_path
        self.config = self._load_config()
        self.save_config()
        
    def _load_config(self):
        """Load configuration from file or create default if doesn't exist"""
        default_config = {
            'peers': {},
            'friends': [],
            'blacklist': [],
            'peer_stats': defaultdict(lambda: {
                'last_seen': 0,
                'response_time': 0,
                'success_count': 0,
                'fail_count': 0,
                'last_speed': 0
            })
        }
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    loaded = json.load(f)
                    # Convert peer_stats back to defaultdict
                    loaded['peer_stats'] = defaultdict(
                        lambda: default_config['peer_stats']['__default__'],
                        loaded.get('peer_stats', {})
                    )
                    return loaded
        except (json.JSONDecodeError, IOError):
            pass
            
        return default_config
    
    def save_config(self):
        """Save current configuration to file"""
        with open(self.config_path, 'w') as f:
            # Convert defaultdict to dict for JSON serialization
            temp_config = self.config.copy()
            temp_config['peer_stats'] = dict(temp_config['peer_stats'])
            json.dump(temp_config, f, indent=2)
    
    def add_peer(self, peer_id, address):
        """Add a new peer to the configuration"""
        if peer_id not in self.config['blacklist'] and peer_id != self.node_id:
            self.config['peers'][peer_id] = address
            self.save_config()
    
    def remove_peer(self, peer_id):
        """Remove a peer from configuration"""
        if peer_id in self.config['peers']:
            del self.config['peers'][peer_id]
            self.save_config()
    
    def add_friend(self, peer_id):
        """Add a peer to friends list (prioritized)"""
        if peer_id in self.config['peers'] and peer_id not in self.config['friends']:
            self.config['friends'].append(peer_id)
            self.save_config()
    
    def add_to_blacklist(self, peer_id):
        """Blacklist a problematic peer"""
        if peer_id in self.config['peers']:
            self.remove_peer(peer_id)
        if peer_id in self.config['friends']:
            self.config['friends'].remove(peer_id)
        if peer_id not in self.config['blacklist']:
            self.config['blacklist'].append(peer_id)
            self.save_config()
    
    def update_peer_stats(self, peer_id, response_time=None, success=True):
        """Update statistics for a peer"""
        stats = self.config['peer_stats'][peer_id]
        stats['last_seen'] = time.time()
        
        if response_time is not None:
            stats['response_time'] = (stats['response_time'] * stats['success_count'] + response_time) / (stats['success_count'] + 1)
            stats['last_speed'] = response_time
            
        if success:
            stats['success_count'] += 1
        else:
            stats['fail_count'] += 1
        
        self.save_config()
    
    def get_best_peers(self, count=3):
        """Get the best peers based on statistics"""
        peers = []
        
        # First try friends
        for peer_id in self.config['friends']:
            if peer_id in self.config['peers']:
                peers.append((peer_id, self.config['peers'][peer_id]))
                if len(peers) >= count:
                    return peers
        
        # Then try fastest peers
        remaining = count - len(peers)
        if remaining > 0:
            all_peers = [
                (peer_id, address, self.config['peer_stats'][peer_id]['last_speed'])
                for peer_id, address in self.config['peers'].items()
                if peer_id not in self.config['friends'] and peer_id not in self.config['blacklist']
            ]
            # Sort by last speed (fastest first)
            all_peers.sort(key=lambda x: x[2])
            peers.extend([(peer_id, address) for peer_id, address, _ in all_peers[:remaining]])
        
        return peers
    
class NetworkConfig:
    def __init__(self):
        self.udp_socket = None
        self.tcp_socket = None
        self.stun_servers = [
            ('stun.l.google.com', 19302),
            ('stun1.l.google.com', 19302),
            ('stun2.l.google.com', 19302)
        ]
        self.trackers = {
            '45.79.195.214': 5000
        }
        self.public_udp_addr = None
        self.local_udp_port = None
        self.local_tcp_port = None
        self.nat_type = None
        
    def get_nat_type(self):
        """Determine NAT type using STUN servers"""
        # This is simplified - real implementation would test multiple STUN servers
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(('0.0.0.0', 0))
            local_port = sock.getsockname()[1]
            
            for stun_server, stun_port in self.stun_servers:
                try:
                    # Send binding request
                    sock.sendto(b'\x00\x01\x00\x00\x21\x12\xa4\x42' + os.urandom(12), 
                            (stun_server, stun_port))
                    sock.settimeout(2)
                    data, addr = sock.recvfrom(1024)
                    
                    # Parse STUN response to get mapped address
                    if len(data) > 20 and data[0:2] == b'\x01\x01':
                        port = struct.unpack('!H', data[26:28])[0]
                        ip_bytes = data[28:32]
                        ip = '.'.join([str(b) for b in ip_bytes])
                        self.public_udp_addr = (ip, port)
                        self.local_udp_port = local_port
                        return "Cone NAT"  # Simplified
                except socket.timeout:
                    continue
            
            return "Symmetric NAT"  # Most restrictive
        except Exception as e:
            print(f"STUN error: {e}")
            return "Unknown"
        
    def get_nodes_from_tracker(self):
        """Request list of nodes from the tracker"""
        for tracker_addr, tracker_port in self.trackers.items():
            try:
                response = requests.get(f"http://{tracker_addr}:{tracker_port}/nodes", timeout=5)
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Failed to contact tracker at {tracker_addr}:{tracker_port}: {e}")
        return {}