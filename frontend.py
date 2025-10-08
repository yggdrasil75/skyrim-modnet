from asyncio.log import logger
import tempfile
import asyncio
import threading
from pathlib import Path
import shutil
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
from sqlalchemy import create_engine, and_, func, or_, desc
from sqlalchemy.orm import sessionmaker, joinedload
import os
from datetime import datetime, timedelta
import json
from database import Base, User, Mod, ModFile, FileChunk, ChunkHosting, Friendship, NodeConnection, Game, Tag, ModTag, DownloadHistory
from app import ModNetworkNode
from node_config import NODE_CONFIG
import concurrent.futures

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB max file size


# Database setup
def get_db_session():
    engine = create_engine('sqlite:///mod_network.db')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

@app.route('/')
def index():
    """Main dashboard"""
    session = get_db_session()
    
    # Get statistics
    total_mods = session.query(Mod).filter_by(is_deleted=False).count()
    total_users = session.query(User).count()
    total_games = session.query(Game).count()
    
    # Recent mods
    recent_mods = session.query(Mod).filter_by(is_deleted=False).order_by(desc(Mod.created_at)).limit(10).all()
    
    # Popular tags
    popular_tags = session.query(Tag).join(ModTag).filter(Tag.is_approved == True).group_by(Tag.id).order_by(desc(ModTag.vote_score)).limit(10).all()
    
    session.close()
    
    return render_template('index.html', 
                         total_mods=total_mods,
                         total_users=total_users,
                         total_games=total_games,
                         recent_mods=recent_mods,
                         popular_tags=popular_tags)

@app.route('/mods')
def mods_list():
    """List all mods with filtering and search"""
    session = get_db_session()
    
    # Get filter parameters
    search = request.args.get('search', '')
    game_id = request.args.get('game_id', '')
    tag_id = request.args.get('tag_id', '')
    page = int(request.args.get('page', 1))
    per_page = 20
    
    # Base query - use joinedload to eagerly load relationships
    query = session.query(Mod).options(joinedload(Mod.game), joinedload(Mod.tags), joinedload(Mod.uploader)).filter_by(is_deleted=False)
    
    # Apply filters
    if search:
        query = query.filter(or_(
            Mod.title.ilike(f'%{search}%'),
            Mod.description.ilike(f'%{search}%')
        ))
    
    if game_id and game_id != 'all':
        query = query.filter_by(game_id=game_id)
    
    if tag_id:
        query = query.join(ModTag).filter(ModTag.tag_id == tag_id)
    
    # Pagination
    total_mods = query.count()
    mods = query.order_by(desc(Mod.created_at)).offset((page-1)*per_page).limit(per_page).all()
    
    # Get filter options
    games = session.query(Game).all()
    tags = session.query(Tag).filter_by(is_approved=True).all()
    
    session.close()
    
    return render_template('mods_list.html',
                         mods=mods,
                         games=games,
                         tags=tags,
                         search=search,
                         selected_game=game_id,
                         selected_tag=tag_id,
                         page=page,
                         per_page=per_page,
                         total_mods=total_mods)

@app.route('/mod/<mod_id>')
def mod_detail(mod_id):
    """Mod detail page"""
    session = get_db_session()
    
    # Eagerly load all relationships that will be accessed in the template
    mod = session.query(Mod).options(
        joinedload(Mod.game),
        joinedload(Mod.uploader)
    ).filter_by(mod_id=mod_id, is_deleted=False).first()
    
    if not mod:
        flash('Mod not found', 'error')
        session.close()
        return redirect(url_for('mods_list'))
    
    # Get mod files
    files = session.query(ModFile).filter_by(mod_id=mod.id).all()
    
    # Get tags with scores
    mod_tags = session.query(ModTag).filter_by(mod_id=mod.id).all()
    
    # Get download history with relationships
    downloads = session.query(DownloadHistory).options(
        joinedload(DownloadHistory.user),
        joinedload(DownloadHistory.source_node)
    ).filter_by(mod_id=mod.id).order_by(desc(DownloadHistory.downloaded_at)).limit(50).all()
    
    # Get similar mods with relationships
    similar_mods = session.query(Mod).options(
        joinedload(Mod.game)
    ).filter(
        Mod.game_id == mod.game_id,
        Mod.id != mod.id,
        Mod.is_deleted == False
    ).order_by(desc(Mod.created_at)).limit(6).all()
    
    session.close()
    
    return render_template('mod_detail.html',
                         mod=mod,
                         files=files,
                         mod_tags=mod_tags,
                         downloads=downloads,
                         similar_mods=similar_mods)

@app.route('/upload', methods=['GET', 'POST'])
def upload_mod():
    """Upload a new mod with file"""
    session = get_db_session()
    
    if request.method == 'POST':
        try:
            # Get form data
            title = request.form.get('title')
            description = request.form.get('description')
            version = request.form.get('version', '1.0.0')
            game_id = request.form.get('game_id')
            new_game_name = request.form.get('new_game_name')
            new_game_short_name = request.form.get('new_game_short_name')
            upload_password = request.form.get('upload_password')
            
            # Validate game selection
            if not game_id and not new_game_name:
                return jsonify({'success': False, 'message': 'Please select a game or create a new one'})
            
            # Check if file was uploaded
            if 'mod_file' not in request.files:
                return jsonify({'success': False, 'message': 'No file selected'})
            
            mod_file = request.files['mod_file']
            if mod_file.filename == '':
                return jsonify({'success': False, 'message': 'No file selected'})
            
            # Validate file size (max 2GB)
            mod_file.seek(0, 2)  # Seek to end
            file_size = mod_file.tell()
            mod_file.seek(0)  # Reset to beginning
            
            if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
                return jsonify({'success': False, 'message': 'File too large (max 2GB)'})
            
            # For demo purposes, use a dummy user
            user = session.query(User).first()
            if not user:
                return jsonify({'success': False, 'message': 'No users found in database'})
            
            # Handle game selection/creation
            game = None
            if game_id and game_id != 'new':
                # Use existing game
                game = session.query(Game).filter_by(id=game_id).first()
                if not game:
                    return jsonify({'success': False, 'message': 'Selected game not found'})
            elif new_game_name:
                # Create new game
                if not new_game_short_name:
                    # Generate short name from name
                    new_game_short_name = new_game_name.lower().replace(' ', '_')
                
                # Check if game already exists
                existing_game = session.query(Game).filter(
                    or_(Game.name == new_game_name, Game.short_name == new_game_short_name)
                ).first()
                
                if existing_game:
                    return jsonify({'success': False, 'message': f'Game "{new_game_name}" already exists'})
                
                game = Game(
                    name=new_game_name,
                    short_name=new_game_short_name,
                    description=request.form.get('new_game_description', '')
                )
                session.add(game)
                session.commit()
                logger.info(f"Created new game: {new_game_name} ({new_game_short_name})")
            
            # Save uploaded file to temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(mod_file.filename).suffix) as tmp_file:
                mod_file.save(tmp_file.name)
                tmp_path = Path(tmp_file.name)
            
            try:
                # Create node instance and upload mod
                node = create_node_instance()
                
                # Run the async upload in a separate thread
                def run_upload():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(
                            node.upload_mod(
                                mod_file_path=tmp_path,
                                title=title,
                                description=description,
                                game_short_name=game.short_name if game else "",
                                version=version,
                                upload_password=upload_password
                            )
                        )
                    finally:
                        loop.close()
                
                # Run upload in thread with timeout
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_upload)
                    mod_id = future.result(timeout=300)  # 5 minute timeout
                
                if mod_id:
                    flash(f'Mod "{title}" uploaded successfully!', 'success')
                    return jsonify({
                        'success': True, 
                        'message': f'Mod "{title}" uploaded successfully!',
                        'redirect_url': url_for('mod_detail', mod_id=mod_id)
                    })
                else:
                    return jsonify({'success': False, 'message': 'Failed to upload mod to network'})
                    
            except concurrent.futures.TimeoutError:
                return jsonify({'success': False, 'message': 'Upload timed out. Please try again.'})
            except Exception as e:
                return jsonify({'success': False, 'message': f'Upload error: {str(e)}'})
            finally:
                # Clean up temporary file
                if tmp_path.exists():
                    tmp_path.unlink()
            
        except Exception as e:
            session.rollback()
            return jsonify({'success': False, 'message': f'Error uploading mod: {str(e)}'})
    
    # GET request - show form
    games = session.query(Game).all()
    session.close()
    
    return render_template('upload_mod.html', games=games)

@app.route('/users')
def users_list():
    """List all users"""
    session = get_db_session()
    
    search = request.args.get('search', '')
    page = int(request.args.get('page', 1))
    per_page = 20
    
    query = session.query(User).options(joinedload(User.uploaded_mods), joinedload(User.friendships))
    
    if search:
        query = query.filter(or_(
            User.display_name.ilike(f'%{search}%'),
            User.node_id.ilike(f'%{search}%')
        ))
    
    total_users = query.count()
    users = query.order_by(desc(User.created_at)).offset((page-1)*per_page).limit(per_page).all()
    
    session.close()
    
    return render_template('users_list.html',
                         users=users,
                         search=search,
                         page=page,
                         per_page=per_page,
                         total_users=total_users)

@app.route('/user/<node_id>')
def user_detail(node_id):
    """User detail page"""
    session = get_db_session()
    
    user = session.query(User).filter_by(node_id=node_id).first()
    if not user:
        flash('User not found', 'error')
        return redirect(url_for('users_list'))
    
    # Get user's uploaded mods
    uploaded_mods = session.query(Mod).filter_by(uploader_id=user.id, is_deleted=False).order_by(desc(Mod.created_at)).all()
    
    # Get friendships
    friendships = session.query(Friendship).filter_by(user_id=user.id).all()
    
    # Get download history
    download_history = session.query(DownloadHistory).filter_by(user_id=user.id).order_by(desc(DownloadHistory.downloaded_at)).limit(50).all()
    
    # Get hosted chunks
    hosted_chunks = session.query(ChunkHosting).filter_by(node_id=user.id).count()
    
    session.close()
    
    return render_template('user_detail.html',
                         user=user,
                         uploaded_mods=uploaded_mods,
                         friendships=friendships,
                         download_history=download_history,
                         hosted_chunks=hosted_chunks)

@app.route('/games')
def games_list():
    """List all games"""
    session = get_db_session()
    
    games = session.query(Game).all()
    
    # Get mod counts for each game
    game_stats = []
    for game in games:
        mod_count = session.query(Mod).filter_by(game_id=game.id, is_deleted=False).count()
        game_stats.append({
            'game': game,
            'mod_count': mod_count
        })
    
    session.close()
    
    return render_template('games_list.html', game_stats=game_stats)

@app.route('/game/<int:game_id>')
def game_detail(game_id):
    """Game detail page"""
    session = get_db_session()
    
    game = session.query(Game).filter_by(id=game_id).first()
    if not game:
        flash('Game not found', 'error')
        return redirect(url_for('games_list'))
    
    # Get mods for this game
    mods = session.query(Mod).filter_by(game_id=game.id, is_deleted=False).order_by(desc(Mod.created_at)).all()
    
    # Get popular tags for this game
    popular_tags = session.query(Tag).join(ModTag).join(Mod).filter(
        Mod.game_id == game.id,
        Tag.is_approved == True
    ).group_by(Tag.id).order_by(desc(ModTag.vote_score)).limit(10).all()
    
    session.close()
    
    return render_template('game_detail.html',
                         game=game,
                         mods=mods,
                         popular_tags=popular_tags)

@app.route('/tags')
def tags_list():
    """List all tags"""
    session = get_db_session()
    
    category = request.args.get('category', '')
    approved_only = request.args.get('approved_only', 'true') == 'true'
    
    query = session.query(Tag)
    
    if category:
        query = query.filter_by(category=category)
    
    if approved_only:
        query = query.filter_by(is_approved=True)
    
    tags = query.order_by(Tag.name).all()
    
    # Get categories
    categories = session.query(Tag.category).distinct().all()
    categories = [cat[0] for cat in categories if cat[0]]
    
    session.close()
    
    return render_template('tags_list.html',
                         tags=tags,
                         categories=categories,
                         selected_category=category,
                         approved_only=approved_only)

@app.route('/network')
def network_status():
    """Network status and statistics"""
    session = get_db_session()
    
    # Basic stats
    total_nodes = session.query(User).count()
    total_connections = session.query(NodeConnection).count()
    total_chunks = session.query(FileChunk).count()
    total_hosting = session.query(ChunkHosting).count()
    
    # Recent activity
    recent_downloads = session.query(DownloadHistory).order_by(desc(DownloadHistory.downloaded_at)).limit(20).all()
    
    # Storage statistics
    total_storage_limit = session.query(func.sum(User.storage_limit_gb)).scalar() or 0
    total_used_storage = session.query(func.sum(User.used_storage_gb)).scalar() or 0
    
    # Node connections
    connections = session.query(NodeConnection).order_by(desc(NodeConnection.last_tested)).limit(50).all()
    
    session.close()
    
    return render_template('network_status.html',
                         total_nodes=total_nodes,
                         total_connections=total_connections,
                         total_chunks=total_chunks,
                         total_hosting=total_hosting,
                         recent_downloads=recent_downloads,
                         total_storage_limit=total_storage_limit,
                         total_used_storage=total_used_storage,
                         connections=connections)

# API endpoints for AJAX calls
@app.route('/api/stats')
def api_stats():
    """JSON API for statistics"""
    session = get_db_session()
    
    stats = {
        'total_mods': session.query(Mod).filter_by(is_deleted=False).count(),
        'total_users': session.query(User).count(),
        'total_games': session.query(Game).count(),
        'total_downloads': session.query(DownloadHistory).count(),
        'total_tags': session.query(Tag).filter_by(is_approved=True).count()
    }
    
    session.close()
    return jsonify(stats)

@app.route('/api/add_node', methods=['POST'])
def api_add_node():
    """API endpoint to manually add a remote node"""
    session = get_db_session()
    
    try:
        data = request.get_json()
        node_url = data.get('node_url', '').strip()
        
        if not node_url:
            return jsonify({'success': False, 'message': 'Node URL is required'})
        
        # Validate URL format
        if not (node_url.startswith('http://') or node_url.startswith('https://')):
            return jsonify({'success': False, 'message': 'URL must start with http:// or https://'})
        
        # Create node instance
        node = create_node_instance()
        
        # Run the async node addition
        def run_add_node():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    node.add_remote_node(node_url)
                )
            finally:
                loop.close()
        
        # Run in thread with timeout
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_add_node)
            result = future.result(timeout=30)  # 30 second timeout
        
        if result:
            flash(f'Successfully added node: {node_url}', 'success')
            return jsonify({'success': True, 'message': f'Node {node_url} added successfully'})
        else:
            return jsonify({'success': False, 'message': f'Failed to add node {node_url}'})
            
    except concurrent.futures.TimeoutError:
        return jsonify({'success': False, 'message': 'Connection timed out'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error adding node: {str(e)}'})
    finally:
        session.close()

@app.route('/api/recent_activity')
def api_recent_activity():
    """JSON API for recent activity"""
    session = get_db_session()
    
    # Recent downloads with mod info
    recent_downloads = session.query(DownloadHistory).join(Mod).join(User).order_by(
        desc(DownloadHistory.downloaded_at)
    ).limit(10).all()
    
    activity = []
    for download in recent_downloads:
        activity.append({
            'mod_title': download.mod.title,
            'user_name': download.user.display_name or download.user.node_id,
            'downloaded_at': download.downloaded_at.isoformat(),
            'success': download.success
        })
    
    session.close()
    return jsonify(activity)

@app.route('/download/<mod_id>')
def download_mod(mod_id):
    """Download a mod file"""
    session = get_db_session()
    
    try:
        # Get mod information
        mod = session.query(Mod).filter_by(mod_id=mod_id, is_deleted=False).first()
        if not mod:
            flash('Mod not found', 'error')
            return redirect(url_for('mods_list'))
        
        mod_file = session.query(ModFile).filter_by(mod_id=mod.id).first()
        if not mod_file:
            flash('No files found for this mod', 'error')
            return redirect(url_for('mod_detail', mod_id=mod_id))
        
        # Create node instance
        node = create_node_instance()
        
        # Use system temp directory instead of local downloads folder
        with tempfile.TemporaryDirectory() as temp_dir:
            download_path = Path(temp_dir)
            
            try:
                # Run the async download
                def run_download():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(
                            node.download_mod(
                                mod_id=mod_id,
                                download_path=download_path
                            )
                        )
                    finally:
                        loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_download)
                    download_result = future.result(timeout=300)
                
                if download_result and download_result.get('success'):
                    downloaded_file_path = download_result.get('file_path')
                    
                    if downloaded_file_path and Path(downloaded_file_path).exists():
                        # Record download history
                        user = session.query(User).first()
                        if user:
                            download_history = DownloadHistory(
                                user_id=user.id,
                                mod_id=mod.id,
                                source_node_id=user.id,
                                downloaded_at=datetime.now(),
                                success=True,
                                download_speed_mbps=download_result.get('download_speed', 0)
                            )
                            session.add(download_history)
                            session.commit()
                        
                        # Send file to user
                        filename = f"{mod.title.replace(' ', '_')}_{mod.version}{Path(downloaded_file_path).suffix}"
                        
                        response = send_file(
                            downloaded_file_path,
                            as_attachment=True,
                            download_name=filename,
                            mimetype='application/octet-stream'
                        )
                        
                        # No cleanup needed - tempfile.TemporaryDirectory auto-cleans
                        return response
                    else:
                        flash('Downloaded file not found', 'error')
                else:
                    flash('Download failed: ' + download_result.get('message', 'Unknown error'), 'error')
                    
            except concurrent.futures.TimeoutError:
                flash('Download timed out. Please try again.', 'error')
            except Exception as e:
                flash(f'Download error: {str(e)}', 'error')
                logger.error(f"Download error in temp directory: {e}")
                
    except Exception as e:
        session.rollback()
        flash(f'Download error: {str(e)}', 'error')
        logger.error(f"Download error for mod {mod_id}: {e}")
    finally:
        session.close()
    
    return redirect(url_for('mod_detail', mod_id=mod_id))

@app.route('/api/download_status/<mod_id>')
def download_status(mod_id):
    """Check if mod is available for download"""
    session = get_db_session()
    
    try:
        mod = session.query(Mod).filter_by(mod_id=mod_id, is_deleted=False).first()
        if not mod:
            return jsonify({'available': False, 'reason': 'Mod not found'})
        
        mod_file = session.query(ModFile).filter_by(mod_id=mod.id).first()
        if not mod_file:
            return jsonify({'available': False, 'reason': 'No files available'})
        
        # Check if chunks are available (simplified check)
        chunks = session.query(FileChunk).filter_by(file_id=mod_file.id).count()
        if chunks == 0:
            return jsonify({'available': False, 'reason': 'File chunks not available'})
        
        return jsonify({
            'available': True,
            'mod_title': mod.title,
            'file_size': mod_file.file_size,
            'file_size_mb': round(mod_file.file_size / (1024 * 1024), 2)
        })
        
    except Exception as e:
        return jsonify({'available': False, 'reason': str(e)})
    finally:
        session.close()

def create_node_instance():
    """Create a ModNetworkNode instance for file operations"""
    
    node = ModNetworkNode(
        db_path=NODE_CONFIG['db_path'],
        storage_path=NODE_CONFIG['storage_path'],
        node_id=NODE_CONFIG['node_id'],
        public_key=NODE_CONFIG['public_key'],
        display_name=NODE_CONFIG['display_name']
    )
    return node

@app.errorhandler(413)
def too_large(e):
    return jsonify({'success': False, 'message': 'File too large (max 2GB)'}), 413

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)