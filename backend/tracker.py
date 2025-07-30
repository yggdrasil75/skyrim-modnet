# tracker.py
import socket
import threading
import time
from collections import defaultdict

class Tracker:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.peers = {}  # {peer_id: (address, last_seen)}
        self.lock = threading.Lock()
        self.running = False
        self.udp_socket = None
        self.tcp_socket = None
        
    def start(self):
        """Start both TCP and UDP servers"""
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
        
        print(f"Tracker started on {self.host}:{self.port} (TCP/UDP)")
        
    def stop(self):
        """Stop the tracker"""
        self.running = False
        if self.udp_socket:
            self.udp_socket.close()
        if self.tcp_socket:
            self.tcp_socket.close()
            
    def _start_udp_server(self):
        """Handle UDP peer registration and discovery"""
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((self.host, self.port))
        
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                message = data.decode().strip()
                
                if message.startswith("REGISTER_UDP|"):
                    # Format: REGISTER_UDP|<peer_id>
                    parts = message.split('|')
                    if len(parts) >= 2:
                        peer_id = parts[1]
                        with self.lock:
                            self.peers[peer_id] = (addr[0], addr[1], time.time())
                            print(f"UDP peer registered: {peer_id} at {addr[0]}:{addr[1]}")
                            
                elif message == "GET_PEERS":
                    # Send back list of peers
                    with self.lock:
                        active_peers = {pid: f"{host}:{port}" 
                                       for pid, (host, port, _) in self.peers.items()}
                    response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                    self.udp_socket.sendto(response.encode(), addr)
                    
            except Exception as e:
                print(f"UDP server error: {e}")
                continue
                
    def _start_tcp_server(self):
        """Handle TCP peer registration and discovery"""
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((self.host, self.port))
        self.tcp_socket.listen(5)
        
        while self.running:
            try:
                conn, addr = self.tcp_socket.accept()
                threading.Thread(target=self._handle_tcp_connection, args=(conn, addr)).start()
            except Exception as e:
                print(f"TCP server error: {e}")
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
                        print(f"TCP peer registered: {peer_id} at {addr[0]}:{port}")
                        
                    # Send back list of peers
                    with self.lock:
                        active_peers = {pid: f"{host}:{p}" 
                                       for pid, (host, p, _) in self.peers.items()
                                       if pid != peer_id}
                    response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                    conn.send(response.encode())
                    
            elif data == "GET_PEERS":
                # Send back list of peers
                with self.lock:
                    active_peers = {pid: f"{host}:{port}" 
                                   for pid, (host, port, _) in self.peers.items()}
                response = "\n".join([f"{pid}|{addr}" for pid, addr in active_peers.items()])
                conn.send(response.encode())
                
        except Exception as e:
            print(f"TCP connection error: {e}")
        finally:
            conn.close()
            
    def _cleanup_peers(self):
        """Remove peers that haven't been seen in a while"""
        while self.running:
            time.sleep(60)  # Run every minute
            cutoff = time.time() - 300  # 5 minutes timeout
            
            with self.lock:
                to_remove = [pid for pid, (_, _, last_seen) in self.peers.items()
                            if last_seen < cutoff]
                
                for pid in to_remove:
                    del self.peers[pid]
                    print(f"Removed inactive peer: {pid}")

if __name__ == '__main__':
    tracker = Tracker(port=5000)
    try:
        tracker.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        tracker.stop()
        print("Tracker stopped")