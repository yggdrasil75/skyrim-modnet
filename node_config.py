# node_config.py
import os

import socket
import getpass
hostname = socket.gethostname()
username = getpass.getuser()
unique_suffix = f"{hostname}_{username}"
# Node configuration for web interface
NODE_CONFIG = {
    'db_path': 'sqlite:///mod_network.db',
    'storage_path': './node_storage',
    'node_id': f'web_interface_{unique_suffix}',
    'public_key': 'web_public_key_123',
    'display_name': 'Web Interface Node'
}

# Ensure storage directory exists
os.makedirs(NODE_CONFIG['storage_path'], exist_ok=True)