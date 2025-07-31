# tracker.py
import socket
import threading
import time
from collections import defaultdict
import json
import logging

# File-local logging level (0: normal, 3: excessive)
TRACKER_LOG_LEVEL = 1

class Tracker:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.peers = {}  # {peer_id: (address, port, last_seen)}
        self.lock = threading.Lock()
        self.running = False
        self.udp_socket = None
        self.tcp_socket = None
        self.start_time: float = 0.0
        
        # Initialize logging
        self._init_logging()
        
    def _init_logging(self):
        """Initialize logging for the tracker"""
        if TRACKER_LOG_LEVEL > 0:
            print("Initializing Tracker logging")
        self.log_level = TRACKER_LOG_LEVEL
        
    def _log(self, level: int, message: str):
        """Log a message if the level is <= current log level"""
        if level <= self.log_level:
            print(f"[Tracker] {message}")
        
    def start(self):
        """Start both TCP and UDP servers"""
        self._log(1, "Starting tracker server")
        self.running = True
        
        # Start UDP server
        udp_thread = threading.Thread(target=self._start_udp_server, daemon=True)
        udp_thread.start()
        
        # Start TCP server
        tcp_thread = threading.Thread(target=self._start_tcp_server, daemon=True)
        tcp_thread.start()
        
        # Start cleanup thread
        cleanup_thread = threading.Thread(target=self._cleanup_peers, daemon=True)
        cleanup_thread.start()
        
        self._log(1, f"Tracker started on {self.host}:{self.port} (TCP/UDP)")
        
    def stop(self):
        """Stop the tracker"""
        self._log(1, "Stopping tracker server")
        self.running = False
        if self.udp_socket:
            self.udp_socket.close()
        if self.tcp_socket:
            self.tcp_socket.close()
            
    def _start_udp_server(self):
        """Handle UDP peer registration and discovery"""
        self._log(2, "Starting UDP server")
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.host, self.port))
        
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                message = data.decode().strip()
                
                if message.startswith("REGISTER_UDP|"):
                    # Format: REGISTER_UDP|<peer_id>|<port>
                    parts = message.split('|')
                    if len(parts) >= 2:
                        peer_id = parts[1]
                        port = parts[2] if len(parts) >= 3 else addr[1]
                        with self.lock:
                            self.peers[peer_id] = (addr[0], port, time.time())
                            self._log(2, f"UDP peer registered: {peer_id} at {addr[0]}:{port}")
                            
                elif message == "GET_PEERS":
                    # Send back list of peers
                    with self.lock:
                        active_peers = {pid: f"{host}:{port}" 
                                      for pid, (host, port, _) in self.peers.items()}
                    response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                    self.udp_socket.sendto(response.encode(), addr)
                    self._log(3, f"Sent peer list to {addr}")
                    
                elif message == "PING":
                    # Simple ping/pong for health checks
                    self.udp_socket.sendto(b'PONG', addr)
                    self._log(3, f"Responded to PING from {addr}")
                    
            except Exception as e:
                self._log(0, f"UDP server error: {e}")
                continue
                
    def _start_tcp_server(self):
        """Handle TCP peer registration and discovery"""
        self._log(2, "Starting TCP server")
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((self.host, self.port))
        self.tcp_socket.listen(5)
        
        while self.running:
            try:
                conn, addr = self.tcp_socket.accept()
                threading.Thread(target=self._handle_tcp_connection, args=(conn, addr)).start()
                self._log(3, f"New TCP connection from {addr}")
            except Exception as e:
                self._log(0, f"TCP server error: {e}")
                continue
                
    def _handle_tcp_connection(self, conn, addr):
        """Handle an individual TCP connection"""
        try:
            data = conn.recv(1024).decode().strip()
            if not data:
                return
                
            if data.startswith("REGISTER|"):
                # Format: REGISTER|<peer_id>|<port>
                parts = data.split('|')
                if len(parts) >= 3:
                    peer_id = parts[1]
                    port = parts[2]
                    with self.lock:
                        self.peers[peer_id] = (addr[0], port, time.time())
                        self._log(2, f"TCP peer registered: {peer_id} at {addr[0]}:{port}")
                        
                    # Send back list of peers
                    with self.lock:
                        active_peers = {pid: f"{host}:{p}" 
                                        for pid, (host, p, _) in self.peers.items()
                                        if pid != peer_id}
                    response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                    conn.send(response.encode())
                    self._log(3, f"Sent peer list to {peer_id}")
                    
            elif data == "GET_PEERS":
                # Send back list of peers
                with self.lock:
                    active_peers = {pid: f"{host}:{port}" 
                                  for pid, (host, port, _) in self.peers.items()}
                response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                conn.send(response.encode())
                self._log(3, f"Sent full peer list to {addr}")
                
            elif data == "PING":
                # Simple ping/pong for health checks
                conn.send(b'PONG')
                self._log(3, f"Responded to TCP PING from {addr}")
                
            elif data.startswith("GET_PEER|"):
                # Get info for a specific peer
                peer_id = data.split('|')[1]
                with self.lock:
                    if peer_id in self.peers:
                        host, port, _ = self.peers[peer_id]
                        response = f"{host}:{port}"
                        conn.send(response.encode())
                        self._log(3, f"Sent peer info for {peer_id} to {addr}")
                    else:
                        conn.send(b'NOT_FOUND')
                        self._log(2, f"Peer {peer_id} not found for {addr}")
                        
            elif data.startswith("UNREGISTER|"):
                # Unregister a peer
                peer_id = data.split('|')[1]
                with self.lock:
                    if peer_id in self.peers:
                        del self.peers[peer_id]
                        conn.send(b'OK')
                        self._log(2, f"Unregistered peer {peer_id} from {addr}")
                    else:
                        conn.send(b'NOT_FOUND')
                        self._log(2, f"Attempt to unregister unknown peer {peer_id} from {addr}")
                        
            elif data == "STATS":
                # Get tracker statistics
                with self.lock:
                    stats = {
                        'total_peers': len(self.peers),
                        'peers': list(self.peers.keys()),
                        'uptime': time.time() - self.start_time if hasattr(self, 'start_time') else 0
                    }
                    response = json.dumps(stats)
                    conn.send(response.encode())
                    self._log(3, f"Sent stats to {addr}")
                
        except Exception as e:
            self._log(0, f"TCP connection error from {addr}: {e}")
        finally:
            conn.close()
            
    def _cleanup_peers(self):
        """Remove peers that haven't been seen in a while"""
        self._log(2, "Starting peer cleanup thread")
        while self.running:
            time.sleep(60)  # Run every minute
            cutoff = time.time() - 300  # 5 minutes timeout
            
            with self.lock:
                to_remove = [pid for pid, (_, _, last_seen) in self.peers.items()
                            if last_seen < cutoff]
                
                for pid in to_remove:
                    del self.peers[pid]
                    self._log(1, f"Removed inactive peer: {pid}")

    def _handle_http_request(self, conn, addr):
        """Handle HTTP requests for web-based trackers"""
        try:
            request = conn.recv(1024).decode()
            if not request:
                return
                
            # Simple HTTP request parsing
            lines = request.split('\r\n')
            if not lines:
                return
                
            request_line = lines[0]
            method, path, _ = request_line.split()
            
            if path == '/nodes':
                # Return list of all peers
                with self.lock:
                    peers = {pid: f"{host}:{port}" 
                            for pid, (host, port, _) in self.peers.items()}
                response = json.dumps(peers)
                http_response = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    f"Content-Length: {len(response)}\r\n"
                    "\r\n"
                    f"{response}"
                )
                conn.sendall(http_response.encode())
                self._log(3, f"Sent HTTP peer list to {addr}")
                
            elif path == '/stats':
                # Return tracker statistics
                with self.lock:
                    stats = {
                        'total_peers': len(self.peers),
                        'uptime': time.time() - self.start_time if hasattr(self, 'start_time') else 0
                    }
                    response = json.dumps(stats)
                    http_response = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: application/json\r\n"
                        f"Content-Length: {len(response)}\r\n"
                        "\r\n"
                        f"{response}"
                    )
                    conn.sendall(http_response.encode())
                    self._log(3, f"Sent HTTP stats to {addr}")
                    
            else:
                # Not found
                http_response = (
                    "HTTP/1.1 404 Not Found\r\n"
                    "Content-Type: text/plain\r\n"
                    "Content-Length: 9\r\n"
                    "\r\n"
                    "Not Found"
                )
                conn.sendall(http_response.encode())
                self._log(2, f"HTTP 404 for {path} from {addr}")
                
        except Exception as e:
            self._log(0, f"HTTP request error from {addr}: {e}")
        finally:
            conn.close()

if __name__ == '__main__':
    tracker = Tracker(port=8080)
    tracker.start_time = time.time()  # Record startup time for uptime calculation
    try:
        tracker.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        tracker.stop()
        print("Tracker stopped")