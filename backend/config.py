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
    