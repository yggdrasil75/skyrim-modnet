import sys
import requests
import os

def upload_file_to_node(file_path, node_address):
    """
    Uploads a file's content to a specified node in the P2P network
    using its /data API endpoint.

    Args:
        file_path (str): The path to the file to upload.
        node_address (str): The address of the target node (e.g., 'http://127.0.0.1:5000').
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    # The API endpoint for raw data submission is /data
    api_url = f"{node_address}/data"
    
    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()

            print(f"Uploading '{file_path}' content to {api_url}...")
            response = requests.post(api_url, data=file_content, headers={'Content-Type': 'text/plain'}, timeout=10)

            if response.status_code == 201:
                response_data = response.json()
                print("✅ Upload successful!")
                print(f"   Message: {response_data.get('message')}")
                print(f"   Data Key: {response_data.get('key')}")
            else:
                print(f"❌ Upload failed. Status Code: {response.status_code}")
                print(f"   Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: Could not connect to node at {node_address}.")
        print(f"   Details: {e}")
    except IOError as e:
        print(f"❌ File error: Could not read file '{file_path}'.")
        print(f"   Details: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python upload.py <path_to_file> <node_address>")
        print("Example: python upload.py ./my_notes.txt http://127.0.0.1:5000")
        sys.exit(1)

    file_path_arg = sys.argv[1]
    node_address_arg = sys.argv[2]
    
    # Ensure address has a protocol prefix for requests library
    if not node_address_arg.startswith(('http://', 'https://')):
        node_address_arg = 'http://' + node_address_arg

    upload_file_to_node(file_path_arg, node_address_arg)