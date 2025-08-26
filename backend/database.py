import sqlite3
from flask import g

# Database setup with node ID as salt
def get_db(nodeid):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect('./database.db')
        db.row_factory = sqlite3.Row
        
        # Apply node_id as salt for database operations
        cursor = db.cursor()
        cursor.execute(f"PRAGMA key = '{nodeid}'")
        cursor.execute("PRAGMA cipher_compatibility = 3")
        cursor.execute("PRAGMA kdf_iter = 256000")  # High iteration count for security
        cursor.execute("PRAGMA cipher_page_size = 4096")
    
    return db

def init_db(nodeid):
    db = get_db(nodeid)
    cursor = db.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        file_hash TEXT PRIMARY KEY,
        original_name TEXT NOT NULL,
        display_name TEXT NOT NULL,
        game TEXT NOT NULL,
        description TEXT,
        password_hash TEXT NOT NULL,
        origin_node TEXT NOT NULL,
        size INTEGER NOT NULL,
        owner TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chunks (
        chunk_hash TEXT PRIMARY KEY,
        file_hash TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        FOREIGN KEY (file_hash) REFERENCES files (file_hash)
    )
    ''')
    
    # Old hosts table (for backward compatibility)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hosts (
        file_hash TEXT NOT NULL,
        node_id TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (file_hash, node_id),
        FOREIGN KEY (file_hash) REFERENCES files (file_hash)
    )
    ''')
    
    # New chunk_hosts table for chunk-level hosting
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chunk_hosts (
        chunk_hash TEXT NOT NULL,
        node_id TEXT NOT NULL,
        file_hash TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (chunk_hash, node_id),
        FOREIGN KEY (chunk_hash) REFERENCES chunks (chunk_hash),
        FOREIGN KEY (file_hash) REFERENCES files (file_hash)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS peers (
        peer_address TEXT PRIMARY KEY,
        peer_type TEXT NOT NULL CHECK(peer_type IN ('friend', 'peer', 'stranger', 'enemy')),
        last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Migrate old data if needed
    try:
        # Check if chunk_hosts table is empty but hosts table has data
        cursor.execute('SELECT COUNT(*) as count FROM chunk_hosts')
        chunk_hosts_count = cursor.fetchone()['count']
        
        if chunk_hosts_count == 0:
            cursor.execute('SELECT COUNT(*) as count FROM hosts')
            hosts_count = cursor.fetchone()['count']
            
            if hosts_count > 0:
                print("Migrating old hosting data to new chunk-level format...")
                # For each file in hosts, assume the node hosts all chunks of that file
                cursor.execute('SELECT file_hash, node_id FROM hosts')
                for row in cursor.fetchall():
                    file_hash = row['file_hash']
                    node_id = row['node_id']
                    
                    # Get all chunks for this file
                    cursor.execute(
                        'SELECT chunk_hash FROM chunks WHERE file_hash = ?',
                        (file_hash,)
                    )
                    chunks = cursor.fetchall()
                    
                    for chunk_row in chunks:
                        chunk_hash = chunk_row['chunk_hash']
                        # Insert into chunk_hosts
                        cursor.execute(
                            'INSERT OR IGNORE INTO chunk_hosts (chunk_hash, node_id, file_hash) VALUES (?, ?, ?)',
                            (chunk_hash, node_id, file_hash)
                        )
                
                db.commit()
                print("Migration completed successfully")
                
    except sqlite3.Error as e:
        print(f"Error during migration: {e}")
        db.rollback()
    
    db.commit()

def close_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()
