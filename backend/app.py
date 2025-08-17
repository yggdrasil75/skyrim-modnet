import os
import hashlib
from flask import Flask, render_template, request, redirect, url_for, send_file
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder="../templates")

# Configuration
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024  # 16GB max
app.config['CHUNK_SIZE'] = 256 * 1024  # 256KB chunks
app.config['ALLOWED_EXTENSIONS'] = {'zip', 'rar', '7z'}

# Simulated distributed storage (in reality this would be across nodes)
shared_files = {}
file_chunks = {}

# Helper functions
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def split_file(filepath):
    """Split file into chunks and return chunk hashes"""
    chunks = []
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(app.config['CHUNK_SIZE'])
            if not chunk:
                break
            chunk_hash = hashlib.sha256(chunk).hexdigest()
            chunk_filename = f"{chunk_hash}.chunk"
            chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], chunk_filename)
            
            # Save chunk (in reality this would be distributed)
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(chunk)
            
            chunks.append(chunk_hash)
    return chunks

def reassemble_file(chunk_hashes, output_path):
    """Reassemble file from chunks"""
    with open(output_path, 'wb') as outfile:
        for chunk_hash in chunk_hashes:
            chunk_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{chunk_hash}.chunk")
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as chunk_file:
                    outfile.write(chunk_file.read())

# Routes
@app.route('/')
def index():
    return render_template('index.html', files=shared_files.items())

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Split file into chunks
        chunk_hashes = split_file(filepath)
        
        # Store file metadata
        file_hash = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
        shared_files[file_hash] = {
            'name': filename,
            'size': os.path.getsize(filepath),
            'chunks': chunk_hashes
        }
        
        # Clean up original file (we only keep chunks)
        os.remove(filepath)
        
        return redirect(url_for('index'))
    
    return redirect(request.url)

@app.route('/download/<file_hash>')
def download_file(file_hash):
    if file_hash not in shared_files:
        return "File not found", 404
    
    file_info = shared_files[file_hash]
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{file_info['name']}")
    
    # Reassemble file from chunks
    reassemble_file(file_info['chunks'], temp_path)
    
    # Send file to user
    response = send_file(temp_path, as_attachment=True, download_name=file_info['name'])
    
    # Clean up temporary file
    os.remove(temp_path)
    
    return response

if __name__ == '__main__':
    # Create upload directory if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)