import os
import hashlib
import sqlite3
import threading
import time
import uuid
from flask import Flask, render_template, request, send_file, redirect, url_for
from werkzeug.utils import secure_filename
from PIL import Image
import io

app = Flask(__name__, template_folder='../templates')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['CHUNK_SIZE'] = 1024 * 1024 * 5  # 5MB chunks
app.config['MAX_STORAGE'] = 1024 * 1024 * 1024  # 1GB default
app.config['NETWORK_THROTTLE'] = 1024 * 1024  # 1MB/s
app.config['DATABASE'] = 'node.db'
app.config['NODE_ID'] = str(uuid.uuid4())

# Initialize database
def init_db():
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    # Mod metadata
    c.execute('''CREATE TABLE IF NOT EXISTS mods
                 (id TEXT PRIMARY KEY, title TEXT, short_desc TEXT, 
                 long_desc TEXT, author TEXT, image_hash TEXT, 
                 timestamp REAL, chunk_count INTEGER, downloads INTEGER DEFAULT 0)''')
    
    # File chunks
    c.execute('''CREATE TABLE IF NOT EXISTS chunks
                 (hash TEXT PRIMARY KEY, data BLOB, 
                 mod_id TEXT, size INTEGER, 
                 FOREIGN KEY(mod_id) REFERENCES mods(id))''')
    
    # Network peers
    c.execute('''CREATE TABLE IF NOT EXISTS peers
                 (id TEXT PRIMARY KEY, address TEXT, last_seen REAL, 
                 status TEXT, bandwidth INTEGER, storage_used INTEGER)''')
    
    # Local configuration
    c.execute('''CREATE TABLE IF NOT EXISTS config
                 (key TEXT PRIMARY KEY, value TEXT)''')
    
    # Set default config if not exists
    c.execute("INSERT OR IGNORE INTO config VALUES ('max_storage', ?)", 
              (str(app.config['MAX_STORAGE']),))
    c.execute("INSERT OR IGNORE INTO config VALUES ('max_bandwidth', ?)", 
              (str(app.config['NETWORK_THROTTLE']),))
    
    conn.commit()
    conn.close()

init_db()

# File Processing Functions
def process_upload(file, title, short_desc, long_desc, image_file=None):
    """Process uploaded mod file and metadata"""
    # Generate mod ID
    file_hash = hashlib.sha256()
    chunk_hashes = []
    
    # Process file chunks
    chunk_count = 0
    while True:
        chunk = file.read(app.config['CHUNK_SIZE'])
        if not chunk:
            break
        
        # Generate chunk hash
        chunk_hash = hashlib.sha256(chunk).hexdigest()
        chunk_hashes.append(chunk_hash)
        
        # Store chunk in DB
        conn = sqlite3.connect(app.config['DATABASE'])
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO chunks VALUES (?, ?, ?, ?)",
                  (chunk_hash, chunk, None, len(chunk)))
        conn.commit()
        conn.close()
        
        file_hash.update(chunk)
        chunk_count += 1
    
    mod_id = file_hash.hexdigest()
    
    # Process image
    image_hash = None
    if image_file:
        try:
            # Resize and compress image
            img = Image.open(image_file)
            img.thumbnail((400, 400))
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=85)
            image_data = img_byte_arr.getvalue()
            image_hash = hashlib.sha256(image_data).hexdigest()
            
            # Store image as chunk
            conn = sqlite3.connect(app.config['DATABASE'])
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO chunks VALUES (?, ?, ?, ?)",
                      (image_hash, image_data, mod_id, len(image_data)))
            conn.commit()
            conn.close()
        except:
            image_hash = None
    
    # Store mod metadata
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute("INSERT INTO mods VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (mod_id, title, short_desc, long_desc, "User", 
               image_hash, time.time(), chunk_count, 0))
    
    # Associate chunks with mod
    for chunk_hash in chunk_hashes:
        c.execute("UPDATE chunks SET mod_id = ? WHERE hash = ?",
                  (mod_id, chunk_hash))
    
    conn.commit()
    conn.close()
    
    # Start replication in background
    threading.Thread(target=replicate_mod, args=(mod_id,)).start()
    
    return mod_id

def replicate_mod(mod_id):
    """Replicate mod to trusted peers"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    # Get mod metadata
    c.execute("SELECT * FROM mods WHERE id = ?", (mod_id,))
    mod = c.fetchone()
    
    # Get associated chunks
    c.execute("SELECT hash FROM chunks WHERE mod_id = ?", (mod_id,))
    chunks = [row[0] for row in c.fetchall()]
    
    # Get trusted peers
    c.execute("SELECT id, address FROM peers WHERE status = 'friend'")
    peers = c.fetchall()
    
    conn.close()
    
    # Distribute to peers
    for peer_id, address in peers:
        try:
            print(f"Replicating {mod_id} to {address}")
        except:
            print(f"Failed to replicate to {address}")

# Network Management
def manage_peers():
    """Periodically update peer status and replicate data"""
    while True:
        conn = sqlite3.connect(app.config['DATABASE'])
        c = conn.cursor()
        
        # Update peer status based on activity
        c.execute("UPDATE peers SET status = 'stranger' WHERE last_seen < ?",
                  (time.time() - 604800,))  # 1 week
        
        # Replicate popular mods
        c.execute("SELECT id FROM mods ORDER BY downloads DESC LIMIT 10")
        popular_mods = [row[0] for row in c.fetchall()]
        
        for mod_id in popular_mods:
            replicate_mod(mod_id)
        
        conn.commit()
        conn.close()
        time.sleep(3600)  # Run hourly

# Start background management
threading.Thread(target=manage_peers, daemon=True).start()

# Web Interface
@app.route('/')
def index():
    """Show available mods"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute("SELECT id, title, short_desc, image_hash, downloads FROM mods ORDER BY timestamp DESC LIMIT 20")
    mods = [{
        'id': row[0],
        'title': row[1],
        'desc': row[2],
        'image': row[3],
        'downloads': row[4]
    } for row in c.fetchall()]
    conn.close()
    return render_template('index.html', mods=mods)

@app.route('/search')
def search():
    """Search mods by title or description"""
    query = request.args.get('q', '')
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    if query:
        c.execute("SELECT id, title, short_desc, image_hash, downloads FROM mods WHERE title LIKE ? OR short_desc LIKE ?",
                  (f'%{query}%', f'%{query}%'))
    else:
        c.execute("SELECT id, title, short_desc, image_hash, downloads FROM mods ORDER BY downloads DESC LIMIT 50")
    
    mods = [{
        'id': row[0],
        'title': row[1],
        'desc': row[2],
        'image': row[3],
        'downloads': row[4]
    } for row in c.fetchall()]
    
    conn.close()
    return render_template('search.html', mods=mods, query=query)

@app.route('/chunk/<chunk_hash>')
def serve_chunk(chunk_hash):
    """Serve a chunk (for images)"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute("SELECT data FROM chunks WHERE hash = ?", (chunk_hash,))
    chunk = c.fetchone()
    conn.close()
    
    if not chunk:
        return "Chunk not found", 404
    
    return send_file(
        io.BytesIO(chunk[0]),
        mimetype='image/jpeg'
    )

@app.route('/mod/<mod_id>')
def mod_page(mod_id):
    """Show mod details"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    c.execute("SELECT * FROM mods WHERE id = ?", (mod_id,))
    mod = c.fetchone()
    
    if not mod:
        return "Mod not found", 404
    
    mod_data = {
        'id': mod[0],
        'title': mod[1],
        'short_desc': mod[2],
        'long_desc': mod[3],
        'author': mod[4],
        'image': mod[5],
        'date': time.ctime(mod[6]),
        'chunk_count': mod[7],
        'downloads': mod[8]
    }
    
    # Get peer availability
    c.execute("SELECT COUNT(*) FROM peers WHERE status IN ('friend', 'stranger')")
    peer_count = c.fetchone()[0]
    
    conn.close()
    return render_template('mod.html', mod=mod_data, peers=peer_count)

@app.route('/download/<mod_id>')
def download_mod(mod_id):
    """Reassemble and serve mod file"""
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    # Get mod chunks
    c.execute("SELECT data FROM chunks WHERE mod_id = ? ORDER BY hash", (mod_id,))
    chunks = [row[0] for row in c.fetchall()]
    
    if not chunks:
        return "Mod not available", 404
    
    # Update download count
    c.execute("UPDATE mods SET downloads = downloads + 1 WHERE id = ?", (mod_id,))
    conn.commit()
    
    # Reassemble file
    full_file = b''.join(chunks)
    conn.close()
    
    # Send as downloadable file
    return send_file(
        io.BytesIO(full_file),
        mimetype='application/octet-stream',
        as_attachment=True,
        download_name=f"{mod_id[:8]}.bin"
    )

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        # Process form data
        title = request.form['title']
        short_desc = request.form['short_desc']
        long_desc = request.form['long_desc']
        mod_file = request.files['mod_file']
        image_file = request.files.get('image')
        
        # Process upload
        mod_id = process_upload(
            mod_file, title, short_desc, long_desc, image_file
        )
        
        return redirect(url_for('mod_page', mod_id=mod_id))
    
    return render_template('upload.html')

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()
    
    if request.method == 'POST':
        # Update config
        max_storage = int(request.form['max_storage'])
        max_bandwidth = int(request.form['max_bandwidth'])
        
        c.execute("UPDATE config SET value = ? WHERE key = 'max_storage'", 
                  (str(max_storage),))
        c.execute("UPDATE config SET value = ? WHERE key = 'max_bandwidth'", 
                  (str(max_bandwidth),))
        
        # Update peers
        for key, value in request.form.items():
            if key.startswith('peer_status_'):
                peer_id = key[12:]
                new_status = value
                c.execute("UPDATE peers SET status = ? WHERE id = ?", 
                          (new_status, peer_id))
        
        conn.commit()
    
    # Get current config
    c.execute("SELECT * FROM config")
    config = {row[0]: row[1] for row in c.fetchall()}
    
    # Get peer list
    c.execute("SELECT id, address, last_seen, status FROM peers")
    peers = [{
        'id': row[0],
        'address': row[1],
        'last_seen': time.ctime(row[2]),
        'status': row[3]
    } for row in c.fetchall()]
    
    conn.close()
    return render_template('settings.html', config=config, peers=peers)

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(host='0.0.0.0', port=5000, threaded=True)