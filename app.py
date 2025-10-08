import os
import hashlib
import json
import asyncio
import aiohttp
from aiohttp import web
import aiofiles
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime
from node_config import NODE_CONFIG

# Import your existing database models
from database import Base, User, Mod, ModFile, FileChunk, ChunkHosting, Friendship, NodeConnection, Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModNetworkNode:
    def __init__(self, db_path: str, storage_path: str, node_id: str, public_key: str, display_name: str = "Unnamed Node"):
        self.db_path = db_path
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.node_id = node_id
        self.public_key = public_key
        self.display_name = display_name
        
        # Initialize database
        self.engine = create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
        # Ensure this node exists in users table
        self._register_node()
        
        # Network configuration
        self.known_nodes = {}  # node_id -> (ip, port, public_key)
        self.max_chunk_size = 50 * 1024 * 1024  # 50MB
        
        # Statistics
        self.stats = {
            'files_served': 0,
            'chunks_stored': 0,
            'bytes_uploaded': 0,
            'bytes_downloaded': 0
        }
    
    def _register_node(self):
        """Register this node in the database"""
        with self.Session() as session:
            existing_user = session.query(User).filter_by(node_id=self.node_id).first()
            if not existing_user:
                user = User(
                    node_id=self.node_id,
                    public_key=self.public_key,
                    display_name=self.display_name
                )
                session.add(user)
                session.commit()
                logger.info(f"Registered new node: {self.display_name} ({self.node_id})")
    
    def add_known_node(self, node_id: str, ip: str, port: int, public_key: str):
        """Add a known node to the network"""
        self.known_nodes[node_id] = (ip, port, public_key)
        logger.info(f"Added known node: {node_id} at {ip}:{port}")
    
    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def calculate_chunk_hash(self, chunk_data: bytes) -> str:
        """Calculate SHA-256 hash of a chunk"""
        return hashlib.sha256(chunk_data).hexdigest()
    
    def split_file_into_chunks(self, file_path: Path) -> List[Tuple[int, bytes, str]]:
        """Split file into chunks and return list of (index, data, hash)"""
        chunks = []
        with open(file_path, 'rb') as f:
            index = 0
            while True:
                chunk_data = f.read(self.max_chunk_size)
                if not chunk_data:
                    break
                
                chunk_hash = self.calculate_chunk_hash(chunk_data)
                chunks.append((index, chunk_data, chunk_hash))
                index += 1
        
        return chunks
    
    def store_chunk_locally(self, chunk_hash: str, chunk_data: bytes) -> Path:
        """Store a chunk locally and return its path"""
        chunk_path = self.storage_path / f"chunks" / chunk_hash[:2] / chunk_hash
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)
        
        self.stats['chunks_stored'] += 1
        self.stats['bytes_uploaded'] += len(chunk_data)
        logger.debug(f"Stored chunk {chunk_hash} at {chunk_path}")
        
        return chunk_path
    
    def get_chunk_locally(self, chunk_hash: str) -> Optional[bytes]:
        """Retrieve a chunk from local storage"""
        chunk_path = self.storage_path / f"chunks" / chunk_hash[:2] / chunk_hash
        
        if not chunk_path.exists():
            return None
        
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
        
        self.stats['bytes_downloaded'] += len(chunk_data)
        return chunk_data
    
    async def upload_mod(self, mod_file_path: Path, title: str, description: str = "", 
                        game_short_name: str = "", version: str = "1.0.0", 
                        upload_password: str = "") -> Optional[str]:
        """Upload a mod file to the network"""
        
        if not mod_file_path.exists():
            logger.error(f"File not found: {mod_file_path}")
            return None
        
        # Calculate file hash
        file_hash = self.calculate_file_hash(mod_file_path)
        file_size = mod_file_path.stat().st_size
        
        with self.Session() as session:
            # Check if file already exists
            existing_file = session.query(ModFile).filter_by(file_hash=file_hash).first()
            if existing_file:
                logger.info(f"File already exists in network: {file_hash}")
                return existing_file.mod.mod_id
            
            # Get or create game
            game = session.query(Game).filter_by(short_name=game_short_name).first()
            if not game and game_short_name:
                game = Game(name=game_short_name.title(), short_name=game_short_name)
                session.add(game)
                session.commit()
            
            # Create mod entry
            mod_id = self._generate_mod_id()
            user = session.query(User).filter_by(node_id=self.node_id).first()
            
            mod = Mod(
                mod_id=mod_id,
                uploader_id=user.id,
                title=title,
                description=description,
                version=version,
                game_id=game.id if game else None,
                upload_password_hash=hashlib.sha256(upload_password.encode()).hexdigest()
            )
            session.add(mod)
            session.commit()
            
            # Create mod file entry
            mod_file = ModFile(
                mod_id=mod.id,
                filename=mod_file_path.name,
                file_path=str(mod_file_path),
                file_size=file_size,
                file_hash=file_hash,
                file_type=mod_file_path.suffix.lower()
            )
            session.add(mod_file)
            session.commit()
            
            # Split file into chunks and store
            chunks = self.split_file_into_chunks(mod_file_path)
            
            for index, chunk_data, chunk_hash in chunks:
                # Store chunk locally
                self.store_chunk_locally(chunk_hash, chunk_data)
                
                # Create file chunk entry
                file_chunk = FileChunk(
                    file_id=mod_file.id,
                    chunk_index=index,
                    chunk_hash=chunk_hash,
                    chunk_size=len(chunk_data),
                    total_chunks=len(chunks)
                )
                session.add(file_chunk)
                session.commit()
                
                # Create chunk hosting entry
                chunk_hosting = ChunkHosting(
                    chunk_id=file_chunk.id,
                    node_id=user.id,
                    is_primary=True
                )
                session.add(chunk_hosting)
            
            session.commit()
            
            logger.info(f"Successfully uploaded mod: {title} (ID: {mod_id})")
            return mod_id
    
    async def download_mod(self, mod_id: str, download_path: Path) -> bool:
        """Download a mod from the network"""
        
        with self.Session() as session:
            mod = session.query(Mod).filter_by(mod_id=mod_id, is_deleted=False).first()
            if not mod:
                logger.error(f"Mod not found: {mod_id}")
                return False
            
            mod_file = session.query(ModFile).filter_by(mod_id=mod.id).first()
            if not mod_file:
                logger.error(f"No files found for mod: {mod_id}")
                return False
            
            # Get all chunks for this file
            chunks = session.query(FileChunk).filter_by(file_id=mod_file.id).order_by(FileChunk.chunk_index).all()
            
            # Download chunks
            downloaded_chunks = []
            for chunk in chunks:
                chunk_data = await self._download_chunk(chunk.chunk_hash)
                if chunk_data:
                    downloaded_chunks.append((chunk.chunk_index, chunk_data))
                else:
                    logger.error(f"Failed to download chunk {chunk.chunk_hash}")
                    return False
            
            # Reconstruct file
            downloaded_chunks.sort(key=lambda x: x[0])
            with open(download_path, 'wb') as f:
                for index, chunk_data in downloaded_chunks:
                    f.write(chunk_data)
            
            # Verify file hash
            downloaded_hash = self.calculate_file_hash(download_path)
            if downloaded_hash != mod_file.file_hash:
                logger.error(f"File verification failed for {mod_id}")
                download_path.unlink()  # Delete corrupted file
                return False
            
            logger.info(f"Successfully downloaded mod: {mod.title} to {download_path}")
            return True
    
    async def _download_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """Download a chunk from the network, trying local storage first"""
        
        # Try local storage first
        local_chunk = self.get_chunk_locally(chunk_hash)
        if local_chunk:
            return local_chunk
        
        # If not found locally, try other nodes
        with self.Session() as session:
            hosting_nodes = session.query(ChunkHosting).join(User).filter(
                ChunkHosting.chunk_id == FileChunk.id,
                FileChunk.chunk_hash == chunk_hash
            ).all()
            
            for hosting in hosting_nodes:
                node = session.query(User).filter_by(id=hosting.node_id).first()
                if node.node_id == self.node_id:
                    continue  # Skip self
                
                if node.node_id in self.known_nodes:
                    ip, port, _ = self.known_nodes[node.node_id]
                    chunk_data = await self._request_chunk_from_node(ip, port, chunk_hash)
                    if chunk_data:
                        # Store chunk locally for future requests
                        self.store_chunk_locally(chunk_hash, chunk_data)
                        return chunk_data
        
        logger.error(f"Failed to download chunk from any node: {chunk_hash}")
        return None
    
    async def _request_chunk_from_node(self, ip: str, port: int, chunk_hash: str) -> Optional[bytes]:
        """Request a chunk from a specific node"""
        try:
            url = f"http://{ip}:{port}/chunk/{chunk_hash}"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.read()
                    else:
                        logger.warning(f"Node {ip}:{port} returned status {response.status} for chunk {chunk_hash}")
                        return None
        except Exception as e:
            logger.warning(f"Failed to request chunk from {ip}:{port}: {e}")
            return None
    
    def get_available_chunks(self) -> List[str]:
        """Get list of chunk hashes available on this node"""
        chunks_dir = self.storage_path / "chunks"
        chunk_hashes = []
        
        if chunks_dir.exists():
            for hash_dir in chunks_dir.iterdir():
                if hash_dir.is_dir():
                    for chunk_file in hash_dir.iterdir():
                        if chunk_file.is_file():
                            chunk_hashes.append(chunk_file.name)
        
        return chunk_hashes
    
    def get_storage_usage(self) -> Tuple[float, float]:
        """Get storage usage in GB (used, total)"""
        total_size = 0
        chunks_dir = self.storage_path / "chunks"
        
        if chunks_dir.exists():
            for file_path in chunks_dir.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        
        used_gb = total_size / (1024 ** 3)
        
        with self.Session() as session:
            user = session.query(User).filter_by(node_id=self.node_id).first()
            total_gb = user.storage_limit_gb if user else 50
        
        return used_gb, total_gb
    
    def _generate_mod_id(self) -> str:
        """Generate a unique mod ID"""
        import secrets
        return secrets.token_hex(32)
    
    def cleanup_old_chunks(self, max_age_days: int = 30):
        """Remove chunks that haven't been accessed recently"""
        with self.Session() as session:
            user = session.query(User).filter_by(node_id=self.node_id).first()
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            
            old_hosting = session.query(ChunkHosting).filter(
                ChunkHosting.node_id == user.id,
                ChunkHosting.last_accessed < cutoff_time,
                ChunkHosting.is_primary == False  # Don't remove primary chunks
            ).all()
            
            for hosting in old_hosting:
                chunk = session.query(FileChunk).filter_by(id=hosting.chunk_id).first()
                if chunk:
                    chunk_path = self.storage_path / "chunks" / chunk.chunk_hash[:2] / chunk.chunk_hash
                    if chunk_path.exists():
                        chunk_path.unlink()
                        logger.info(f"Removed old chunk: {chunk.chunk_hash}")
                
                session.delete(hosting)
            
            session.commit()

    
    async def add_remote_node(self, node_url: str) -> bool:
        """
        Manually add a remote node to the network
        """
        try:
            # Extract host and port from URL
            from urllib.parse import urlparse
            parsed = urlparse(node_url)
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == 'https' else 80)
            
            # Add to known nodes (you'll need to implement this in your node logic)
            # This is a simplified example - adapt to your actual node implementation
            node_info = {
                'node_id': f"manual_{host}_{port}",
                'base_url': node_url,
                'host': host,
                'port': port,
                'is_https': parsed.scheme == 'https'
            }
            
            # Store in database or node configuration
            # You'll need to implement the actual storage logic based on your architecture
            logger.info(f"Added manual node: {node_url}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding remote node {node_url}: {e}")
            return False


class NodeServer:
    """HTTP server for node communication"""
    
    def __init__(self, node: ModNetworkNode, host: str = 'localhost', port: int = 8080):
        self.node = node
        self.host = host
        self.port = port
        self.app = aiohttp.web.Application()
        self.setup_routes()
    
    def setup_routes(self):
        self.app.router.add_get('/chunk/{chunk_hash}', self.handle_chunk_request)
        self.app.router.add_get('/health', self.handle_health_check)
        self.app.router.add_post('/register', self.handle_node_registration)
    
    async def handle_chunk_request(self, request):
        chunk_hash = request.match_info['chunk_hash']
        
        chunk_data = self.node.get_chunk_locally(chunk_hash)
        if chunk_data:
            # Update last accessed time
            with self.node.Session() as session:
                user = session.query(User).filter_by(node_id=self.node.node_id).first()
                chunk = session.query(FileChunk).filter_by(chunk_hash=chunk_hash).first()
                if chunk:
                    hosting = session.query(ChunkHosting).filter_by(
                        chunk_id=chunk.id, 
                        node_id=user.id
                    ).first()
                    if hosting:
                        hosting.last_accessed = datetime.now()
                        session.commit()
            
            self.node.stats['files_served'] += 1
            return aiohttp.web.Response(body=chunk_data, content_type='application/octet-stream')
        else:
            return aiohttp.web.Response(status=404, text="Chunk not found")
    
    async def handle_health_check(self, request):
        used_gb, total_gb = self.node.get_storage_usage()
        health_info = {
            'node_id': self.node.node_id,
            'display_name': self.node.display_name,
            'storage_used_gb': used_gb,
            'storage_total_gb': total_gb,
            'chunks_available': len(self.node.get_available_chunks()),
            'stats': self.node.stats
        }
        return web.json_response(health_info)
    
    async def handle_node_registration(self, request):
        data = await request.json()
        node_id = data['node_id']
        ip = data['ip']
        port = data['port']
        public_key = data['public_key']
        
        self.node.add_known_node(node_id, ip, port, public_key)
        return aiohttp.web.json_response({'status': 'registered'})
    
    async def start(self):
        runner = aiohttp.web.AppRunner(self.app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info(f"Node server started at http://{self.host}:{self.port}")
    
    async def stop(self):
        await self.app.shutdown()
        await self.app.cleanup()

# Example usage
async def main():
    # Initialize a node
    node = ModNetworkNode(
        db_path=NODE_CONFIG['db_path'],
        storage_path=NODE_CONFIG['storage_path'],
        node_id=NODE_CONFIG['node_id'],
        public_key=NODE_CONFIG['public_key'],
        display_name=NODE_CONFIG['display_name']
    )
    
    # Add some known nodes
    node.add_known_node('node_abcdef', '192.168.1.100', 8080, 'public_key_other_node')
    
    # Start HTTP server
    server = NodeServer(node, '0.0.0.0', 8080)
    await server.start()
    
    try:
        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Run for 1 hour intervals
    except KeyboardInterrupt:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())