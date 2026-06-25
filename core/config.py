"""Node configuration.

Every node generates a stable NODE_ID on first boot and keeps it in
node_data/. The node's own signing key (Ed25519) lets it sign gossip so
peers can detect a peer that has been taken over / spoofed.
"""
from __future__ import annotations

import json
import os
import secrets

from nacl import signing


BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _node_identity(data_dir: str) -> dict:
    path = os.path.join(data_dir, "node_identity.json")
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    sk = signing.SigningKey.generate()
    verify_key = sk.verify_key.encode().hex()
    ident = {
        # Identity is BOUND to the key: node_id = first 16 bytes of verify key
        # hex. This must match core.gossip.derive_node_id so that a signed
        # message's derived sender id always equals the node's own NODE_ID.
        "node_id": verify_key[:32],
        "signing_key": sk.encode().hex(),
        "verify_key": verify_key,
    }
    os.makedirs(data_dir, exist_ok=True)
    with open(path, "w") as fh:
        json.dump(ident, fh)
    return ident


def load_config() -> dict:
    data_dir = os.environ.get("NODE_DATA_DIR", os.path.join(BASE, "node_data"))
    ident = _node_identity(data_dir)
    return {
        "NODE_DATA_DIR": data_dir,
        "BLOB_DIR": os.path.join(data_dir, "blobs"),
        "DB_PATH": os.path.join(data_dir, "node.sqlite"),
        "NODE_ID": ident["node_id"],
        "SIGNING_KEY": ident["signing_key"],
        "VERIFY_KEY": ident["verify_key"],
        # Operator-tunable policy. These are *local* choices — the whole
        # point is that no central authority dictates them.
        "PUBLIC_URL": os.environ.get("PUBLIC_URL", "http://localhost:5000"),
        # Role this node declares to the network. 'gateway' nodes store ZERO
        # files — they only bootstrap peers and answer network-info queries,
        # giving them nothing worth seizing. Everything else is 'common'.
        "DECLARED_ROLE": os.environ.get("DECLARED_ROLE", "common"),
        "MAX_BLOB_SHARD_BYTES": 100 * 1024 * 1024,  # 100 MB split target
        "SECURITY_SCAN_ENABLED": os.environ.get("SECURITY_SCAN", "1") == "1",
        # Replication policy: aim to keep each shard on this many distinct
        # nodes, biased toward geographically/topologically distant ones.
        "TARGET_REPLICAS": int(os.environ.get("TARGET_REPLICAS", "4")),
        "MAX_DISK_BYTES": int(os.environ.get("MAX_DISK_BYTES", str(50 * 1024**3))),
        "ENABLE_BACKGROUND_JOBS": True,
        "SECRET_KEY": secrets.token_hex(32),
    }
