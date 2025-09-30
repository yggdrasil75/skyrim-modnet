from sqlalchemy import UniqueConstraint, create_engine, Column, Integer, String, Boolean, DateTime, Float, Text, ForeignKey, LargeBinary, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
import hashlib

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    node_id = Column(String(64), unique=True, nullable=False)  # Public key hash
    public_key = Column(Text, nullable=False)  # For verification
    display_name = Column(String(100))
    created_at = Column(DateTime, default=func.now())
    storage_limit_gb = Column(Integer, default=50)
    used_storage_gb = Column(Float, default=0.0)
    
    # Relationships with back_populates to avoid conflicts
    friendships = relationship("Friendship", foreign_keys="Friendship.user_id", back_populates="user")
    friend_of = relationship("Friendship", foreign_keys="Friendship.friend_id", back_populates="friend")
    blacklisted_users = relationship("UserBlacklist", foreign_keys="UserBlacklist.user_id", back_populates="user")
    blacklisted_by = relationship("UserBlacklist", foreign_keys="UserBlacklist.blocked_user_id", back_populates="blocked_user")
    uploaded_mods = relationship("Mod", foreign_keys="Mod.uploader_id", back_populates="uploader")
    chunks_hosted = relationship("ChunkHosting", back_populates="node")
    suggested_tags = relationship("Tag", foreign_keys="Tag.suggested_by", back_populates="suggester")
    tag_votes = relationship("TagVote", back_populates="user")
    download_history = relationship("DownloadHistory", foreign_keys="DownloadHistory.user_id", back_populates="user")
    source_downloads = relationship("DownloadHistory", foreign_keys="DownloadHistory.source_node_id", back_populates="source_node")
    outgoing_connections = relationship("NodeConnection", foreign_keys="NodeConnection.from_node_id", back_populates="from_node")
    incoming_connections = relationship("NodeConnection", foreign_keys="NodeConnection.to_node_id", back_populates="to_node")
    content_filters = relationship("ContentFilter", back_populates="user")

class Friendship(Base):
    __tablename__ = 'friendships'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    friend_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    priority = Column(Integer, default=1)  # Higher = more priority
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="friendships")
    friend = relationship("User", foreign_keys=[friend_id], back_populates="friend_of")
    
    # Ensure unique friendships
    __table_args__ = (UniqueConstraint('user_id', 'friend_id', name='unique_friendship'),)

class UserBlacklist(Base):
    __tablename__ = 'user_blacklist'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    blocked_user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    reason = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="blacklisted_users")
    blocked_user = relationship("User", foreign_keys=[blocked_user_id], back_populates="blacklisted_by")
    
    __table_args__ = (UniqueConstraint('user_id', 'blocked_user_id', name='unique_user_block'),)

class Mod(Base):
    __tablename__ = 'mods'
    
    id = Column(Integer, primary_key=True)
    mod_id = Column(String(64), unique=True, nullable=False)  # Public identifier
    uploader_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    version = Column(String(50))
    game_id = Column(Integer, ForeignKey('games.id'))
    upload_password_hash = Column(String(64), nullable=False)  # For modifications
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_deleted = Column(Boolean, default=False)
    deletion_requested_at = Column(DateTime)
    
    # Relationships
    uploader = relationship("User", foreign_keys=[uploader_id], back_populates="uploaded_mods")
    game = relationship("Game", back_populates="mods")
    files = relationship("ModFile", back_populates="mod")
    tags = relationship("ModTag", back_populates="mod")
    images = relationship("ModImage", back_populates="mod")
    blacklists = relationship("ModBlacklist", back_populates="mod")
    download_history = relationship("DownloadHistory", back_populates="mod")

class ModBlacklist(Base):
    __tablename__ = 'mod_blacklist'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    mod_id = Column(Integer, ForeignKey('mods.id'), nullable=False)
    file_hash = Column(String(64))  # Optional: block specific file
    reason = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    mod = relationship("Mod", back_populates="blacklists")
    
    __table_args__ = (UniqueConstraint('user_id', 'mod_id', 'file_hash', name='unique_mod_block'),)

class Game(Base):
    __tablename__ = 'games'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    short_name = Column(String(50), nullable=False)  # e.g., 'skyrim', 'fallout4'
    description = Column(String(1000), nullable=False)
    supported_versions = Column(JSON)  # List of supported game versions
    
    # Relationships
    mods = relationship("Mod", back_populates="game")

class ModFile(Base):
    __tablename__ = 'mod_files'
    
    id = Column(Integer, primary_key=True)
    mod_id = Column(Integer, ForeignKey('mods.id'), nullable=False)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500))  # Relative path in mod
    file_size = Column(Integer)  # In bytes
    file_hash = Column(String(64), nullable=False)  # SHA-256
    file_type = Column(String(50))  # Determined by extension/analysis
    
    # Auto-detected content flags
    has_textures = Column(Boolean, default=False)
    has_meshes = Column(Boolean, default=False)
    has_plugins = Column(Boolean, default=False)
    has_skse_plugins = Column(Boolean, default=False)
    has_scripts = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    mod = relationship("Mod", back_populates="files")
    chunks = relationship("FileChunk", back_populates="file")

class FileChunk(Base):
    __tablename__ = 'file_chunks'
    
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('mod_files.id'), nullable=False)
    chunk_index = Column(Integer, nullable=False)  # Order in file
    chunk_hash = Column(String(64), nullable=False)  # SHA-256 of chunk
    chunk_size = Column(Integer, nullable=False)  # Actual size of this chunk
    total_chunks = Column(Integer, nullable=False)  # Total chunks in file
    
    # Relationships
    file = relationship("ModFile", back_populates="chunks")
    hosting_nodes = relationship("ChunkHosting", back_populates="chunk")

class ChunkHosting(Base):
    __tablename__ = 'chunk_hosting'
    
    id = Column(Integer, primary_key=True)
    chunk_id = Column(Integer, ForeignKey('file_chunks.id'), nullable=False)
    node_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    is_primary = Column(Boolean, default=False)  # Original uploader
    replication_count = Column(Integer, default=1)
    added_at = Column(DateTime, default=func.now())
    last_accessed = Column(DateTime, default=func.now())
    
    # Relationships
    chunk = relationship("FileChunk", back_populates="hosting_nodes")
    node = relationship("User", back_populates="chunks_hosted")
    
    __table_args__ = (UniqueConstraint('chunk_id', 'node_id', name='unique_chunk_host'),)

class Tag(Base):
    __tablename__ = 'tags'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    category = Column(String(50))  # e.g., 'category', 'mechanic', 'aesthetic'
    created_at = Column(DateTime, default=func.now())
    is_approved = Column(Boolean, default=False)  # Enough votes to be official
    suggested_by = Column(Integer, ForeignKey('users.id'))
    
    # Relationships
    suggester = relationship("User", foreign_keys=[suggested_by], back_populates="suggested_tags")
    mod_tags = relationship("ModTag", back_populates="tag")

class ModTag(Base):
    __tablename__ = 'mod_tags'
    
    id = Column(Integer, primary_key=True)
    mod_id = Column(Integer, ForeignKey('mods.id'), nullable=False)
    tag_id = Column(Integer, ForeignKey('tags.id'), nullable=False)
    vote_score = Column(Integer, default=0)  # Can be negative
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    mod = relationship("Mod", back_populates="tags")
    tag = relationship("Tag", back_populates="mod_tags")
    votes = relationship("TagVote", back_populates="mod_tag")
    
    __table_args__ = (UniqueConstraint('mod_id', 'tag_id', name='unique_mod_tag'),)

class TagVote(Base):
    __tablename__ = 'tag_votes'
    
    id = Column(Integer, primary_key=True)
    mod_tag_id = Column(Integer, ForeignKey('mod_tags.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    vote_value = Column(Integer, nullable=False)  # -1, 0, 1 or similar
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    mod_tag = relationship("ModTag", back_populates="votes")
    user = relationship("User", back_populates="tag_votes")
    
    __table_args__ = (UniqueConstraint('mod_tag_id', 'user_id', name='unique_tag_vote'),)

class ModImage(Base):
    __tablename__ = 'mod_images'
    
    id = Column(Integer, primary_key=True)
    mod_id = Column(Integer, ForeignKey('mods.id'), nullable=False)
    image_hash = Column(String(64), nullable=False)
    filename = Column(String(255))
    caption = Column(String(255))
    display_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    mod = relationship("Mod", back_populates="images")

class NodeConnection(Base):
    __tablename__ = 'node_connections'
    
    id = Column(Integer, primary_key=True)
    from_node_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    to_node_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    latency_ms = Column(Float)
    last_tested = Column(DateTime, default=func.now())
    success_rate = Column(Float, default=1.0)  # Connection reliability
    
    # Relationships
    from_node = relationship("User", foreign_keys=[from_node_id], back_populates="outgoing_connections")
    to_node = relationship("User", foreign_keys=[to_node_id], back_populates="incoming_connections")
    
    __table_args__ = (UniqueConstraint('from_node_id', 'to_node_id', name='unique_connection'),)

class DownloadHistory(Base):
    __tablename__ = 'download_history'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    mod_id = Column(Integer, ForeignKey('mods.id'), nullable=False)
    source_node_id = Column(Integer, ForeignKey('users.id'))
    downloaded_at = Column(DateTime, default=func.now())
    download_speed_mbps = Column(Float)
    success = Column(Boolean, default=True)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="download_history")
    mod = relationship("Mod", back_populates="download_history")
    source_node = relationship("User", foreign_keys=[source_node_id], back_populates="source_downloads")

class ContentFilter(Base):
    __tablename__ = 'content_filters'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    filter_type = Column(String(50), nullable=False)  # 'file_type', 'file_extension', 'max_size'
    filter_value = Column(String(255), nullable=False)  # e.g., 'dll', '.exe', '100MB'
    action = Column(String(20), default='block')  # 'block', 'warn', 'allow'
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    user = relationship("User", back_populates="content_filters")

# Initialize database
def init_db(db_path='sqlite:///mod_network.db'):
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

# Utility functions
def generate_mod_id():
    import secrets
    return secrets.token_hex(32)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def chunk_file(file_path, chunk_size=50*1024*1024):  # 50MB default
    """Generator that yields file chunks"""
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk