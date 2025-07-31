from collections import defaultdict
import json
import os
import socket
import struct
import requests
import time
from datetime import datetime, timedelta
from typing import Optional

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
            'https://www.themoddingtree.com/'
        }
        self.public_udp_addr = None
        self.local_udp_port = None
        self.local_tcp_port = None
        self.nat_type = None
        self.config_path = 'netConf.json'
        
        # Peer management
        self.nat_peers: dict[str, tuple[str, int]] = {}  # {peer_id: (ip, port)}
        self.holepunched_peers: dict[str, tuple[str, int]] = {}  # {peer_id: (ip, port)}
        self.friends: set[str] = set()  # set of peer_ids
        self.blacklist: set[str] = set()  # set of peer_ids
        self.temp_offline_peers: dict[str, dict] = {}  # {peer_id: {last_attempt: timestamp, attempts: int}}
        
        # Initialize sockets
        self.init_sockets()
        
    def init_sockets(self):
        """Initialize UDP and TCP sockets"""
        try:
            # UDP socket
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(('0.0.0.0', 0))
            self.local_udp_port = self.udp_socket.getsockname()[1]
            
            # TCP socket
            self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_socket.bind(('0.0.0.0', 0))
            self.local_tcp_port = self.tcp_socket.getsockname()[1]
            self.tcp_socket.listen(5)
        except socket.error as e:
            print(f"Socket initialization failed: {e}")
            raise

    def save_config(self):
        """Save the current network configuration to file"""
        config = {
            'nat_type': self.nat_type,
            'nat_peers': self.nat_peers,
            'holepunched_peers': self.holepunched_peers,
            'friends': list(self.friends),
            'blacklist': list(self.blacklist),
            'temp_offline_peers': self.temp_offline_peers,
            'public_udp_addr': self.public_udp_addr,
            'last_updated': datetime.now().isoformat()
        }
        
        try:
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)
        except IOError as e:
            print(f"Failed to save config: {e}")

    def load_config(self):
        """Load network configuration from file and validate peers"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                
            self.nat_type = config.get('nat_type')
            self.nat_peers = config.get('nat_peers', {})
            self.holepunched_peers = config.get('holepunched_peers', {})
            self.friends = set(config.get('friends', []))
            self.blacklist = set(config.get('blacklist', []))
            self.temp_offline_peers = config.get('temp_offline_peers', {})
            self.public_udp_addr = config.get('public_udp_addr')
            
            # Clean up temporary offline peers that are now friends
            for peer_id in list(self.temp_offline_peers.keys()):
                if peer_id in self.friends:
                    del self.temp_offline_peers[peer_id]
            
            # Validate peer health
            self.validate_peers()
            
        except FileNotFoundError:
            print("No existing config file, starting fresh")
        except json.JSONDecodeError:
            print("Config file corrupted, starting fresh")
        except Exception as e:
            print(f"Error loading config: {e}")

    def validate_peers(self):
        """Check health of stored peers and update their status"""
        current_time = time.time()
        offline_threshold = timedelta(hours=1).total_seconds()
        
        # Check NAT peers
        for peer_id, (ip, port) in list(self.nat_peers.items()):
            if peer_id in self.friends:
                continue  # Friends are always considered online
                
            if not self.check_peer_health(ip, port):
                if peer_id in self.temp_offline_peers:
                    self.temp_offline_peers[peer_id]['attempts'] += 1
                    self.temp_offline_peers[peer_id]['last_attempt'] = current_time
                else:
                    self.temp_offline_peers[peer_id] = {
                        'last_attempt': current_time,
                        'attempts': 1
                    }
                del self.nat_peers[peer_id]
        
        # Check holepunched peers
        for peer_id, (ip, port) in list(self.holepunched_peers.items()):
            if peer_id in self.friends:
                continue
                
            if not self.check_peer_health(ip, port):
                if peer_id in self.temp_offline_peers:
                    self.temp_offline_peers[peer_id]['attempts'] += 1
                    self.temp_offline_peers[peer_id]['last_attempt'] = current_time
                else:
                    self.temp_offline_peers[peer_id] = {
                        'last_attempt': current_time,
                        'attempts': 1
                    }
                del self.holepunched_peers[peer_id]
        
        # Clean up old temporary offline peers
        for peer_id in list(self.temp_offline_peers.keys()):
            peer_data = self.temp_offline_peers[peer_id]
            if current_time - peer_data['last_attempt'] > offline_threshold:
                if peer_data['attempts'] > 3:  # If failed multiple times
                    print(f"Peer {peer_id} seems permanently offline, removing from temp list")
                    del self.temp_offline_peers[peer_id]
                else:
                    # Move back to active lists to retry
                    if peer_id in self.nat_peers:
                        pass  # Already in the right place
                    elif peer_id in self.holepunched_peers:
                        pass  # Already in the right place
                    else:
                        # Try to determine which list it belongs in
                        # This would depend on your application logic
                        pass

    def check_peer_health(self, ip: str, port: int) -> bool:
        """Check if a peer is responsive"""
        try:
            # Try UDP ping first
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(2)
                s.sendto(b'PING', (ip, port))
                data, _ = s.recvfrom(4)
                if data == b'PONG':
                    return True
        except socket.timeout:
            pass
        except Exception:
            pass
        
        # If UDP failed, try TCP
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((ip, port))
                s.sendall(b'PING')
                data = s.recv(4)
                if data == b'PONG':
                    return True
        except Exception:
            pass
        
        return False

    def add_friend(self, peer_id: str):
        """Add a peer to friends list"""
        self.friends.add(peer_id)
        # Remove from temp offline if present
        if peer_id in self.temp_offline_peers:
            del self.temp_offline_peers[peer_id]
        self.save_config()

    def add_to_blacklist(self, peer_id: str):
        """Add a peer to blacklist"""
        self.blacklist.add(peer_id)
        # Remove from all other lists
        self.nat_peers.pop(peer_id, None)
        self.holepunched_peers.pop(peer_id, None)
        self.temp_offline_peers.pop(peer_id, None)
        self.friends.discard(peer_id)
        self.save_config()

    def add_nat_peer(self, peer_id: str, ip: str, port: int):
        """Add a NAT peer"""
        if peer_id not in self.blacklist:
            self.nat_peers[peer_id] = (ip, port)
            # Remove from temp offline if present
            if peer_id in self.temp_offline_peers:
                del self.temp_offline_peers[peer_id]
            self.save_config()

    def add_holepunched_peer(self, peer_id: str, ip: str, port: int):
        """Add a holepunched peer"""
        if peer_id not in self.blacklist:
            self.holepunched_peers[peer_id] = (ip, port)
            # Remove from temp offline if present
            if peer_id in self.temp_offline_peers:
                del self.temp_offline_peers[peer_id]
            self.save_config()

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
        for tracker_addr in self.trackers:
            try:
                response = requests.get(f"{tracker_addr}/nodes", timeout=5)
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Failed to contact tracker at {tracker_addr}: {e}")
        return {}
