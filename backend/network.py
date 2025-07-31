from collections import defaultdict
import json
import os
import socket
import struct
import requests


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

