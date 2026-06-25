"""Proof-of-storage, provenance, and sampling — verifiable possession.

The unifying idea: a node may never be *taken at its word* about what it
stores. Every possession claim must be backed by evidence another node can
check cheaply.

Three primitives live here:

1. CHALLENGE / RESPONSE (corruption + liveness check)
   A challenger sends a random nonce. The holder must return
   H(nonce || shard_bytes). Only a node holding the exact, uncorrupted bytes
   can produce the right digest. Cheap (one hash, one small message), and it
   simultaneously detects silent on-disk corruption and "I claim to hold this
   but don't". The challenger must itself know the correct shard_id (== sha256
   of the bytes) to know the expected answer — which it does, because shard
   ids are content addresses and it can compute the expected digest from any
   correct copy it can reach, or store a precomputed table.

   To let a challenger verify WITHOUT already holding the bytes, we use a
   commit form: expected = H(nonce || shard_id) is NOT enough (shard_id is
   public). Instead the holder returns H(nonce || bytes); the challenger
   compares against H(nonce || bytes_from_a_reference_copy). So verification
   needs one reference copy in the quorum — which the voting layer samples.

2. PROVENANCE (anti-Sybil possession proof)
   Every locally-held shard carries a signed provenance record: either
   - ORIGIN: signed by the mod author at publish, or
   - RECEIPT: signed by the source node that served the bytes ("I, node S,
     sent shard X to node R at time T").
   A node cannot fabricate "I have file X" without a receipt it cannot forge,
   so it cannot cheaply claim to hold files it never received.

3. SAMPLING (voter-side verification)
   When forming a vote, a voter fetches the actual bytes from a random subset
   of claimed holders (bounded by size) and verifies them, so attestations are
   anchored to physical possession, not mere assertion.
"""
from __future__ import annotations

import hashlib
import os
import secrets

from nacl import signing, encoding


# --- 1. proof-of-storage challenge/response ---------------------------------

def make_nonce() -> str:
    return secrets.token_hex(16)


def storage_proof(nonce_hex: str, shard_path: str) -> str | None:
    """Holder side: H(nonce || file_bytes). None if we can't read the file."""
    if not os.path.exists(shard_path):
        return None
    h = hashlib.sha256()
    h.update(bytes.fromhex(nonce_hex))
    try:
        with open(shard_path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except OSError:
        return None
    return h.hexdigest()


def expected_proof(nonce_hex: str, reference_path: str) -> str | None:
    """Challenger side: compute the answer from a known-good reference copy."""
    return storage_proof(nonce_hex, reference_path)


def verify_proof_against_bytes(nonce_hex: str, claimed_digest: str,
                               reference_bytes: bytes) -> bool:
    h = hashlib.sha256()
    h.update(bytes.fromhex(nonce_hex))
    h.update(reference_bytes)
    return h.hexdigest() == claimed_digest


# --- 2. provenance ----------------------------------------------------------

def make_receipt(signing_key_hex: str, shard_id: str, recipient_node_id: str,
                 ts: int) -> dict:
    """Source signs: 'I sent shard_id to recipient at ts'. The recipient keeps
    this as proof it legitimately received the bytes (not fabricated)."""
    sk = signing.SigningKey(signing_key_hex, encoder=encoding.HexEncoder)
    msg = f"receipt|{shard_id}|{recipient_node_id}|{ts}".encode()
    return {
        "kind": "receipt",
        "shard_id": shard_id,
        "recipient": recipient_node_id,
        "ts": ts,
        "source_verify_key": sk.verify_key.encode(encoder=encoding.HexEncoder).decode(),
        "sig": sk.sign(msg).signature.hex(),
    }


def verify_receipt(receipt: dict, recipient_node_id: str) -> bool:
    try:
        if receipt.get("kind") != "receipt":
            return False
        if receipt["recipient"] != recipient_node_id:
            return False
        msg = (f"receipt|{receipt['shard_id']}|{receipt['recipient']}"
               f"|{receipt['ts']}").encode()
        vk = signing.VerifyKey(receipt["source_verify_key"],
                               encoder=encoding.HexEncoder)
        vk.verify(msg, bytes.fromhex(receipt["sig"]))
        return True
    except Exception:  # noqa: BLE001
        return False


def make_origin(author_sk_hex: str, shard_id: str, mod_id: str, ts: int) -> dict:
    """Author signs that this shard is part of a mod they published — the root
    of every provenance chain."""
    sk = signing.SigningKey(author_sk_hex, encoder=encoding.HexEncoder)
    msg = f"origin|{shard_id}|{mod_id}|{ts}".encode()
    return {
        "kind": "origin",
        "shard_id": shard_id,
        "mod_id": mod_id,
        "ts": ts,
        "author_verify_key": sk.verify_key.encode(encoder=encoding.HexEncoder).decode(),
        "sig": sk.sign(msg).signature.hex(),
    }


def verify_origin(origin: dict, expected_author_pubkey: str | None = None) -> bool:
    try:
        if origin.get("kind") != "origin":
            return False
        if expected_author_pubkey and origin["author_verify_key"] != expected_author_pubkey:
            return False
        msg = f"origin|{origin['shard_id']}|{origin['mod_id']}|{origin['ts']}".encode()
        vk = signing.VerifyKey(origin["author_verify_key"],
                               encoder=encoding.HexEncoder)
        vk.verify(msg, bytes.fromhex(origin["sig"]))
        return True
    except Exception:  # noqa: BLE001
        return False


# --- 3. sampling helpers ----------------------------------------------------

def deterministic_sample(items: list, k: int, seed: str) -> list:
    """Pick k items pseudo-randomly but reproducibly from a per-voter seed, so
    each voter samples a DIFFERENT subset and the choice can be audited."""
    if k >= len(items):
        return list(items)
    scored = sorted(
        items,
        key=lambda it: hashlib.sha256((seed + str(it)).encode()).hexdigest(),
    )
    return scored[:k]
