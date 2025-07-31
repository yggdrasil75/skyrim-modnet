from collections import defaultdict
import json
import os
import socket
import struct
import requests
import time
from datetime import datetime, timedelta
from typing import Optional
import threading

LoggingLevel: int = 3  # 0: "normal". 3: "excessive"

class NetworkConfig:
    def __init__(self):
        if LoggingLevel > 0:
            print("Initializing NetworkConfig")
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
        
        if LoggingLevel > 2:
            print("Initialized all attributes, initializing sockets")

        # Initialize sockets
        self.init_sockets()
        if LoggingLevel > 0:
            print("NetworkConfig initialized")

    def init_sockets(self):
        """Initialize UDP and TCP sockets"""
        if LoggingLevel > 1:
            print("Initializing sockets")
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
            
            if LoggingLevel > 2:
                print(f"UDP socket bound to port {self.local_udp_port}")
                print(f"TCP socket bound to port {self.local_tcp_port}")
            if LoggingLevel > 0:
                print("Sockets initialized successfully")
        except socket.error as e:
            print(f"Socket initialization failed: {e}")
            if LoggingLevel > 0:
                print("Socket initialization failed, raising exception")
            raise

    def save_config(self):
        """Save the current network configuration to file"""
        if LoggingLevel > 1:
            print("Saving configuration to file")
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
                if LoggingLevel > 2:
                    print(f"Configuration saved to {self.config_path}")
                elif LoggingLevel > 0:
                    print("Configuration saved successfully")
        except IOError as e:
            print(f"Failed to save config: {e}")
            if LoggingLevel > 0:
                print("Config save operation failed")

    def load_config(self):
        """Load network configuration from file and validate peers"""
        if LoggingLevel > 1:
            print(f"Attempting to load configuration from {self.config_path}")
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
            
            if LoggingLevel > 2:
                print(f"Loaded config: {config}")
            elif LoggingLevel > 0:
                print("Config loaded from file")
            
            # Clean up temporary offline peers that are now friends
            removed_peers = [peer_id for peer_id in list(self.temp_offline_peers.keys()) 
                           if peer_id in self.friends]
            if removed_peers and LoggingLevel > 1:
                print(f"Removing {len(removed_peers)} peers from temp_offline_peers as they are now friends")
            
            for peer_id in removed_peers:
                del self.temp_offline_peers[peer_id]
            
            # Validate peer health
            if LoggingLevel > 1:
                print("Validating loaded peers")
            self.validate_peers()
            
        except FileNotFoundError:
            if LoggingLevel > 0:
                print("No existing config file, starting fresh")
        except json.JSONDecodeError:
            if LoggingLevel > 0:
                print("Config file corrupted, starting fresh")
        except Exception as e:
            print(f"Error loading config: {e}")
            if LoggingLevel > 0:
                print("Config load operation failed")

    def validate_peers(self):
        """Check health of stored peers and update their status"""
        if LoggingLevel > 1:
            print("Validating peer health")
        current_time = time.time()
        offline_threshold = timedelta(hours=1).total_seconds()
        
        # Check NAT peers
        if LoggingLevel > 2:
            print(f"Checking {len(self.nat_peers)} NAT peers")
        
        for peer_id, (ip, port) in list(self.nat_peers.items()):
            if peer_id in self.friends:
                if LoggingLevel > 3:
                    print(f"Skipping friend peer {peer_id} in NAT peers")
                continue  # Friends are always considered online
                
            if not self.check_peer_health(ip, port):
                if peer_id in self.temp_offline_peers:
                    self.temp_offline_peers[peer_id]['attempts'] += 1
                    self.temp_offline_peers[peer_id]['last_attempt'] = current_time
                    if LoggingLevel > 2:
                        print(f"Peer {peer_id} failed health check, incrementing attempts")
                else:
                    self.temp_offline_peers[peer_id] = {
                        'last_attempt': current_time,
                        'attempts': 1
                    }
                    if LoggingLevel > 1:
                        print(f"Peer {peer_id} failed health check, adding to temp_offline_peers")
                del self.nat_peers[peer_id]
        
        # Check holepunched peers
        if LoggingLevel > 2:
            print(f"Checking {len(self.holepunched_peers)} holepunched peers")
        
        for peer_id, (ip, port) in list(self.holepunched_peers.items()):
            if peer_id in self.friends:
                if LoggingLevel > 3:
                    print(f"Skipping friend peer {peer_id} in holepunched peers")
                continue
                
            if not self.check_peer_health(ip, port):
                if peer_id in self.temp_offline_peers:
                    self.temp_offline_peers[peer_id]['attempts'] += 1
                    self.temp_offline_peers[peer_id]['last_attempt'] = current_time
                    if LoggingLevel > 2:
                        print(f"Peer {peer_id} failed health check, incrementing attempts")
                else:
                    self.temp_offline_peers[peer_id] = {
                        'last_attempt': current_time,
                        'attempts': 1
                    }
                    if LoggingLevel > 1:
                        print(f"Peer {peer_id} failed health check, adding to temp_offline_peers")
                del self.holepunched_peers[peer_id]
        
        # Clean up old temporary offline peers
        if LoggingLevel > 2:
            print(f"Checking {len(self.temp_offline_peers)} temp offline peers")
        
        for peer_id in list(self.temp_offline_peers.keys()):
            peer_data = self.temp_offline_peers[peer_id]
            if current_time - peer_data['last_attempt'] > offline_threshold:
                if peer_data['attempts'] > 3:  # If failed multiple times
                    if LoggingLevel > 0:
                        print(f"Peer {peer_id} seems permanently offline, removing from temp list")
                    del self.temp_offline_peers[peer_id]
                else:
                    # Move back to active lists to retry
                    if peer_id in self.nat_peers:
                        if LoggingLevel > 2:
                            print(f"Peer {peer_id} still in NAT peers")
                    elif peer_id in self.holepunched_peers:
                        if LoggingLevel > 2:
                            print(f"Peer {peer_id} still in holepunched peers")
                    else:
                        if LoggingLevel > 1:
                            print(f"Peer {peer_id} not found in active lists, may need rediscovery")

    def check_peer_health(self, ip: str, port: int) -> bool:
        """Check if a peer is responsive"""
        if LoggingLevel > 2:
            print(f"Checking health of peer {ip}:{port}")
        
        try:
            # Try UDP ping first
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(2)
                s.sendto(b'PING', (ip, port))
                data, _ = s.recvfrom(4)
                if data == b'PONG':
                    if LoggingLevel > 3:
                        print(f"UDP health check successful for {ip}:{port}")
                    return True
        except socket.timeout:
            if LoggingLevel > 2:
                print(f"UDP timeout for {ip}:{port}")
        except Exception as e:
            if LoggingLevel > 1:
                print(f"UDP health check error for {ip}:{port}: {e}")
        
        # If UDP failed, try TCP
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((ip, port))
                s.sendall(b'PING')
                data = s.recv(4)
                if data == b'PONG':
                    if LoggingLevel > 3:
                        print(f"TCP health check successful for {ip}:{port}")
                    return True
        except Exception as e:
            if LoggingLevel > 1:
                print(f"TCP health check error for {ip}:{port}: {e}")
        
        if LoggingLevel > 1:
            print(f"Health check failed for {ip}:{port}")
        return False

    def add_friend(self, peer_id: str):
        """Add a peer to friends list"""
        if LoggingLevel > 0:
            print(f"Adding peer {peer_id} to friends list")
        self.friends.add(peer_id)
        # Remove from temp offline if present
        if peer_id in self.temp_offline_peers:
            if LoggingLevel > 1:
                print(f"Removing peer {peer_id} from temp_offline_peers")
            del self.temp_offline_peers[peer_id]
        self.save_config()

    def add_to_blacklist(self, peer_id: str):
        """Add a peer to blacklist"""
        if LoggingLevel > 0:
            print(f"Adding peer {peer_id} to blacklist")
        self.blacklist.add(peer_id)
        # Remove from all other lists
        if peer_id in self.nat_peers:
            if LoggingLevel > 1:
                print(f"Removing peer {peer_id} from nat_peers")
            self.nat_peers.pop(peer_id)
        if peer_id in self.holepunched_peers:
            if LoggingLevel > 1:
                print(f"Removing peer {peer_id} from holepunched_peers")
            self.holepunched_peers.pop(peer_id)
        if peer_id in self.temp_offline_peers:
            if LoggingLevel > 1:
                print(f"Removing peer {peer_id} from temp_offline_peers")
            self.temp_offline_peers.pop(peer_id)
        if peer_id in self.friends:
            if LoggingLevel > 1:
                print(f"Removing peer {peer_id} from friends")
            self.friends.discard(peer_id)
        self.save_config()

    def add_nat_peer(self, peer_id: str, ip: str, port: int):
        """Add a NAT peer"""
        if peer_id not in self.blacklist:
            if LoggingLevel > 1:
                print(f"Adding NAT peer {peer_id} at {ip}:{port}")
            self.nat_peers[peer_id] = (ip, port)
            # Remove from temp offline if present
            if peer_id in self.temp_offline_peers:
                if LoggingLevel > 2:
                    print(f"Removing peer {peer_id} from temp_offline_peers")
                del self.temp_offline_peers[peer_id]
            self.save_config()
        elif LoggingLevel > 0:
            print(f"Cannot add {peer_id} to NAT peers - blacklisted")

    def add_holepunched_peer(self, peer_id: str, ip: str, port: int):
        """Add a holepunched peer"""
        if peer_id not in self.blacklist:
            if LoggingLevel > 1:
                print(f"Adding holepunched peer {peer_id} at {ip}:{port}")
            self.holepunched_peers[peer_id] = (ip, port)
            # Remove from temp offline if present
            if peer_id in self.temp_offline_peers:
                if LoggingLevel > 2:
                    print(f"Removing peer {peer_id} from temp_offline_peers")
                del self.temp_offline_peers[peer_id]
            self.save_config()
        elif LoggingLevel > 0:
            print(f"Cannot add {peer_id} to holepunched peers - blacklisted")

    def get_nat_type(self):
        """Determine NAT type using STUN servers"""
        if LoggingLevel > 0:
            print("Determining NAT type using STUN servers")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(('0.0.0.0', 0))
            local_port = sock.getsockname()[1]
            
            for stun_server, stun_port in self.stun_servers:
                try:
                    if LoggingLevel > 1:
                        print(f"Trying STUN server {stun_server}:{stun_port}")
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
                        if LoggingLevel > 0:
                            print(f"Determined public UDP address: {ip}:{port}")
                            print("NAT type: Cone NAT")
                        return "Cone NAT"  # Simplified
                except socket.timeout:
                    if LoggingLevel > 1:
                        print(f"Timeout with STUN server {stun_server}:{stun_port}")
                    continue
            
            if LoggingLevel > 0:
                print("NAT type: Symmetric NAT")
            return "Symmetric NAT"  # Most restrictive
        except Exception as e:
            print(f"STUN error: {e}")
            if LoggingLevel > 0:
                print("NAT type: Unknown")
            return "Unknown"

    def get_nodes_from_tracker(self):
        """Request list of nodes from the tracker"""
        if LoggingLevel > 0:
            print("Requesting nodes from trackers")
        for tracker_addr in self.trackers:
            try:
                if LoggingLevel > 1:
                    print(f"Contacting tracker at {tracker_addr}")
                response = requests.get(f"{tracker_addr}/nodes", timeout=5)
                if response.status_code == 200:
                    if LoggingLevel > 1:
                        print(f"Successfully got nodes from {tracker_addr}")
                    return response.json()
                elif LoggingLevel > 0:
                    print(f"Tracker {tracker_addr} returned status {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to contact tracker at {tracker_addr}: {e}")
                if LoggingLevel > 1:
                    print(f"Exception details: {str(e)}")
        if LoggingLevel > 0:
            print("All trackers failed to respond")
        return {}

    def discover_peers(self, max_peers=50):
        """Main peer discovery function that combines multiple discovery methods"""
        if LoggingLevel > 0:
            print(f"Starting peer discovery (max {max_peers} peers)")
        discovered_peers = {}
        
        # Get peers from tracker
        if LoggingLevel > 1:
            print("Discovering peers from trackers")
        tracker_peers = self.get_nodes_from_tracker()
        discovered_peers.update(tracker_peers)
        if LoggingLevel > 1:
            print(f"Found {len(tracker_peers)} peers from trackers")
        
        # Discover local peers
        if LoggingLevel > 1:
            print("Discovering local peers")
        local_peers = self.discover_local_peers()
        discovered_peers.update(local_peers)
        if LoggingLevel > 1:
            print(f"Found {len(local_peers)} local peers")
        
        # Filter out blacklisted peers and limit to max_peers
        valid_peers = {
            peer_id: addr for peer_id, addr in discovered_peers.items()
            if peer_id not in self.blacklist and peer_id not in self.friends
        }
        if LoggingLevel > 1:
            print(f"Filtered to {len(valid_peers)} valid peers")
        
        # Prioritize peers by speed (unknown peers get medium priority)
        if LoggingLevel > 1:
            print("Prioritizing peers")
        prioritized_peers = self.prioritize_peers(valid_peers)
        
        # Add the top peers to our lists
        added_count = 0
        for peer_id, (ip, port) in list(prioritized_peers.items())[:max_peers]:
            if peer_id not in self.nat_peers and peer_id not in self.holepunched_peers:
                self.add_nat_peer(peer_id, ip, port)
                added_count += 1
        if LoggingLevel > 0:
            print(f"Added {added_count} new peers")
        
        self.save_config()
        return prioritized_peers

    def discover_local_peers(self):
        """Discover peers on the local network using UDP broadcast"""
        if LoggingLevel > 0:
            print("Starting local peer discovery")
        local_peers = {}
        broadcast_address = '<broadcast>'
        port = 47821  # Standard discovery port
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                s.settimeout(2)
                
                # Send discovery packet
                if LoggingLevel > 1:
                    print("Sending discovery broadcast")
                s.sendto(b'DISCOVER', (broadcast_address, port))
                
                # Listen for responses
                start_time = time.time()
                if LoggingLevel > 1:
                    print("Listening for peer responses")
                while time.time() - start_time < 5:  # Listen for 5 seconds
                    try:
                        data, addr = s.recvfrom(1024)
                        if data.startswith(b'PEER:'):
                            parts = data.decode().split(':')
                            if len(parts) == 3:
                                peer_id = parts[1]
                                peer_port = int(parts[2])
                                local_peers[peer_id] = (addr[0], peer_port)
                                if LoggingLevel > 2:
                                    print(f"Discovered local peer {peer_id} at {addr[0]}:{peer_port}")
                    except socket.timeout:
                        continue
        except Exception as e:
            print(f"Local discovery error: {e}")
            if LoggingLevel > 0:
                print("Local peer discovery failed")
        
        if LoggingLevel > 0:
            print(f"Found {len(local_peers)} local peers")
        return local_peers

    def peer_discovery_loop(self, interval=300):
        """Continuous peer discovery in background"""
        if LoggingLevel > 0:
            print(f"Starting peer discovery loop (interval: {interval}s)")
        while True:
            try:
                if LoggingLevel > 1:
                    print("Running peer discovery iteration")
                self.discover_peers()
                if LoggingLevel > 2:
                    print(f"Sleeping for {interval} seconds")
                time.sleep(interval)
            except Exception as e:
                print(f"Discovery loop error: {e}")
                if LoggingLevel > 0:
                    print("Discovery loop encountered error, waiting longer")
                time.sleep(60)  # Wait longer if error occurs

    def peer_health_check_loop(self, interval=60):
        """Continuous health monitoring of peers"""
        if LoggingLevel > 0:
            print(f"Starting peer health check loop (interval: {interval}s)")
        while True:
            try:
                if LoggingLevel > 1:
                    print("Running peer health check iteration")
                self.validate_peers()
                if LoggingLevel > 2:
                    print(f"Sleeping for {interval} seconds")
                time.sleep(interval)
            except Exception as e:
                print(f"Health check error: {e}")
                if LoggingLevel > 0:
                    print("Health check encountered error, waiting longer")
                time.sleep(60)  # Wait longer if error occurs

    def peer_speed_check_loop(self, interval=3600):
        """Periodically check peer speeds and update priorities"""
        if LoggingLevel > 0:
            print(f"Starting peer speed check loop (interval: {interval}s)")
        while True:
            try:
                if LoggingLevel > 1:
                    print("Running peer speed check iteration")
                self.check_peer_speeds()
                if LoggingLevel > 2:
                    print(f"Sleeping for {interval} seconds")
                time.sleep(interval)
            except Exception as e:
                print(f"Speed check error: {e}")
                if LoggingLevel > 0:
                    print("Speed check encountered error, waiting longer")
                time.sleep(600)  # Wait longer if error occurs

    def check_peer_speeds(self):
        """Measure and record peer response times"""
        if LoggingLevel > 0:
            print("Starting peer speed check")
        speed_results = {}
        
        # Check NAT peers
        if LoggingLevel > 1:
            print(f"Checking speeds of {len(self.nat_peers)} NAT peers")
        for peer_id, (ip, port) in self.nat_peers.items():
            if LoggingLevel > 2:
                print(f"Measuring latency for NAT peer {peer_id} at {ip}:{port}")
            latency = self.measure_peer_latency(ip, port)
            if latency is not None:
                speed_results[peer_id] = latency
                if LoggingLevel > 2:
                    print(f"Peer {peer_id} latency: {latency:.2f}ms")
        
        # Check holepunched peers
        if LoggingLevel > 1:
            print(f"Checking speeds of {len(self.holepunched_peers)} holepunched peers")
        for peer_id, (ip, port) in self.holepunched_peers.items():
            if LoggingLevel > 2:
                print(f"Measuring latency for holepunched peer {peer_id} at {ip}:{port}")
            latency = self.measure_peer_latency(ip, port)
            if latency is not None:
                speed_results[peer_id] = latency
                if LoggingLevel > 2:
                    print(f"Peer {peer_id} latency: {latency:.2f}ms")
        
        # Update peer priorities based on speed
        if LoggingLevel > 1:
            print("Updating peer priorities based on speed")
        self.update_peer_priorities(speed_results)
        self.save_config()
        if LoggingLevel > 0:
            print("Peer speed check completed")

    def measure_peer_latency(self, ip: str, port: int, samples=3) -> Optional[float]:
        """Measure average latency to a peer in milliseconds"""
        if LoggingLevel > 2:
            print(f"Measuring latency to {ip}:{port} with {samples} samples")
        total_time = 0
        successful_samples = 0
        
        for i in range(samples):
            try:
                # Try UDP first
                if LoggingLevel > 3:
                    print(f"Sample {i+1}: Trying UDP ping")
                start_time = time.time()
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.settimeout(1)
                    s.sendto(b'PING', (ip, port))
                    data, _ = s.recvfrom(4)
                    if data == b'PONG':
                        latency = (time.time() - start_time) * 1000
                        total_time += latency
                        successful_samples += 1
                        if LoggingLevel > 3:
                            print(f"UDP sample {i+1} latency: {latency:.2f}ms")
                        continue
            except socket.timeout:
                if LoggingLevel > 3:
                    print(f"UDP sample {i+1} timeout")
            except Exception as e:
                if LoggingLevel > 2:
                    print(f"UDP sample {i+1} error: {e}")
            
            try:
                # Fall back to TCP
                if LoggingLevel > 3:
                    print(f"Sample {i+1}: Trying TCP ping")
                start_time = time.time()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    s.connect((ip, port))
                    s.sendall(b'PING')
                    data = s.recv(4)
                    if data == b'PONG':
                        latency = (time.time() - start_time) * 1000
                        total_time += latency
                        successful_samples += 1
                        if LoggingLevel > 3:
                            print(f"TCP sample {i+1} latency: {latency:.2f}ms")
            except Exception as e:
                if LoggingLevel > 2:
                    print(f"TCP sample {i+1} error: {e}")
        
        if successful_samples > 0:
            avg_latency = total_time / successful_samples
            if LoggingLevel > 1:
                print(f"Average latency to {ip}:{port}: {avg_latency:.2f}ms (successful {successful_samples}/{samples})")
            return avg_latency
        if LoggingLevel > 1:
            print(f"Failed to measure latency to {ip}:{port} (0/{samples} successful)")
        return None

    def prioritize_peers(self, peers: dict) -> dict:
        """Sort peers by priority (speed, reliability, etc.)"""
        if LoggingLevel > 1:
            print(f"Prioritizing {len(peers)} peers")
        # Get known speeds for peers we've measured before
        prioritized = {}
        unknown_speed_peers = {}
        
        for peer_id, addr in peers.items():
            # Check if we have speed data in temp_offline_peers
            if peer_id in self.temp_offline_peers:
                peer_data = self.temp_offline_peers[peer_id]
                if 'avg_latency' in peer_data:
                    prioritized[peer_id] = (addr, peer_data['avg_latency'])
                    if LoggingLevel > 2:
                        print(f"Peer {peer_id} has known latency: {peer_data['avg_latency']:.2f}ms")
                else:
                    unknown_speed_peers[peer_id] = addr
                    if LoggingLevel > 2:
                        print(f"Peer {peer_id} has no latency data")
            else:
                unknown_speed_peers[peer_id] = addr
                if LoggingLevel > 2:
                    print(f"Peer {peer_id} not in temp_offline_peers")
        
        # Sort known peers by speed (fastest first)
        sorted_known = sorted(prioritized.items(), key=lambda x: x[1][1])
        if LoggingLevel > 2:
            print(f"Sorted known peers: {sorted_known}")
        
        # Add unknown peers at medium priority
        medium_latency = 100  # Assumed medium latency (ms)
        for peer_id, addr in unknown_speed_peers.items():
            sorted_known.append((peer_id, (addr, medium_latency)))
            if LoggingLevel > 2:
                print(f"Added unknown peer {peer_id} with assumed latency {medium_latency}ms")
        
        # Return as dict with fastest peers first
        result = {peer_id: addr for peer_id, (addr, _) in sorted_known}
        if LoggingLevel > 1:
            print(f"Prioritized {len(result)} peers")
        return result

    def update_peer_priorities(self, speed_results: dict):
        """Update peer speed information in the temp_offline_peers structure"""
        if LoggingLevel > 1:
            print(f"Updating priorities for {len(speed_results)} peers")
        for peer_id, latency in speed_results.items():
            if peer_id in self.temp_offline_peers:
                # Update existing record with exponential moving average
                old_latency = self.temp_offline_peers[peer_id].get('avg_latency', latency)
                new_latency = (old_latency * 0.7) + (latency * 0.3)
                self.temp_offline_peers[peer_id]['avg_latency'] = new_latency
                if LoggingLevel > 2:
                    print(f"Updated peer {peer_id} latency: {old_latency:.2f}ms -> {new_latency:.2f}ms")
            else:
                # Create new record
                self.temp_offline_peers[peer_id] = {
                    'avg_latency': latency,
                    'last_attempt': time.time(),
                    'attempts': 0
                }
                if LoggingLevel > 1:
                    print(f"Added new peer {peer_id} with latency {latency:.2f}ms")

    def start_background_tasks(self):
        """Start all background maintenance tasks"""
        if LoggingLevel > 0:
            print("Starting background tasks")
        # Discovery thread
        threading.Thread(
            target=self.peer_discovery_loop,
            daemon=True
        ).start()
        if LoggingLevel > 1:
            print("Started peer discovery thread")
        
        # Health check thread
        threading.Thread(
            target=self.peer_health_check_loop,
            daemon=True
        ).start()
        if LoggingLevel > 1:
            print("Started health check thread")
        
        # Speed check thread
        threading.Thread(
            target=self.peer_speed_check_loop,
            daemon=True
        ).start()
        if LoggingLevel > 1:
            print("Started speed check thread")
        if LoggingLevel > 0:
            print("All background tasks started")
