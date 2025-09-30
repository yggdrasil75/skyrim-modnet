# node_config.py
import os

# Node configuration for web interface
NODE_CONFIG = {
    'db_path': 'sqlite:///mod_network.db',
    'storage_path': './node_storage',
    'node_id': 'web_interface_node',
    'public_key': 'web_public_key_123',
    'display_name': 'Web Interface Node'
}

# Ensure storage directory exists
os.makedirs(NODE_CONFIG['storage_path'], exist_ok=True)