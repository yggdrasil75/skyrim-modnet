"""Content voting / consensus.

Your rules, made precise:
  - A shard is content-addressed (its id == sha256 of its intended bytes).
  - Each holder publishes a SIGNED attestation: "I serve shard S and the bytes
    I serve hash to H". Honest holders report H == S. A corrupt/malicious
    holder reports H != S (or serves bytes that don't match their own claim,
    which the verifier catches on download).
  - We group attestations by the hash they claim and sum voter WEIGHTS
    (from trust.vote_weight), not raw counts, so 2 trusted agreers outweigh a
    swarm of newcomers/sybils.

Resolution (mapping your spec onto weights, with a count floor):
  - need at least MIN_AGREE_VOTERS distinct voters agreeing on one hash, and
    that hash must equal the shard_id (bytes match their address), and the
    agreeing weight must exceed disagreeing weight  -> AVAILABLE.
  - if two+ sides have meaningful weight and neither dominates -> CONTESTED
    (treated as withheld: "either corrupt or malicious").
  - a lone third dissenter against an established majority is overridden
    (it just adds to disagree_weight but doesn't flip a clear majority).
  - too few voters -> INSUFFICIENT (not yet downloadable).

"2 copies that agree" is the floor; "a third that disagrees can be overridden"
falls out of weight-comparison; "2 available that disagree -> unavailable" is
the CONTESTED branch.
"""
from __future__ import annotations

import time

from .trust import vote_weight, classify_state

MIN_AGREE_VOTERS = 2          # your "needs 2 copies that agree"
DOMINANCE_RATIO = 2.0         # winning weight must be >= 2x the runner-up


def _peer_view(db, node_id: str) -> dict:
    row = db.execute(
        "SELECT node_id, declared_role, first_seen, failures, bad_file_count, "
        "node_state, relation FROM peers WHERE node_id=?", (node_id,)
    ).fetchone()
    if row is None:
        # Unknown attester (e.g. ourselves or a brand-new node): treat as
        # newcomer with no relation.
        return {"node_id": node_id, "declared_role": "common",
                "first_seen": int(time.time()), "failures": 0,
                "bad_file_count": 0, "node_state": "newcomer",
                "relation": "neutral"}
    return dict(row)


def resolve_shard(db, shard_id: str, self_node: dict | None = None,
                  now: int | None = None) -> dict:
    """Compute consensus for one shard from its stored attestations."""
    now = now or int(time.time())
    rows = db.execute(
        "SELECT node_id, content_hash, proven FROM attestations WHERE shard_id=?",
        (shard_id,)
    ).fetchall()

    self_id = self_node.get("node_id") if self_node else None

    # tally weight per claimed content hash, and count distinct voters per hash
    by_hash: dict[str, float] = {}
    voters_by_hash: dict[str, set] = {}
    for r in rows:
        # Anti-Sybil gate: only attestations whose possession we verified
        # (proven=1) — or our own — count toward consensus. Unproven claims are
        # ignored so a node can't vote on files it can't actually answer for.
        if not r["proven"] and r["node_id"] != self_id:
            continue
        peer = _peer_view(db, r["node_id"])
        if self_node and r["node_id"] == self_id:
            peer = self_node
        w = vote_weight(peer, now)
        if w <= 0:
            continue
        by_hash.setdefault(r["content_hash"], 0.0)
        by_hash[r["content_hash"]] += w
        voters_by_hash.setdefault(r["content_hash"], set()).add(r["node_id"])

    if not by_hash:
        return _store(db, shard_id, "insufficient", None, 0, 0, 0, now)

    # winner = highest-weight hash; it must be the correct (address-matching) one
    ranked = sorted(by_hash.items(), key=lambda kv: -kv[1])
    top_hash, top_w = ranked[0]
    runner_w = ranked[1][1] if len(ranked) > 1 else 0.0
    disagree_w = sum(w for h, w in by_hash.items() if h != top_hash)
    top_voters = len(voters_by_hash[top_hash])
    total_voters = sum(len(v) for v in voters_by_hash.values())

    # The winning content must actually match the shard's address. If the
    # majority claims a hash != shard_id, the shard is poisoned at the source.
    if top_hash != shard_id:
        return _store(db, shard_id, "contested", None,
                      top_w, disagree_w, total_voters, now)

    # Genuine disagreement: multiple distinct hashes each carry weight and the
    # winner doesn't dominate. Per spec, two copies that disagree are withheld
    # ("either corrupt or malicious") even if each side has only one voter.
    if disagree_w > 0 and top_w < disagree_w * DOMINANCE_RATIO:
        return _store(db, shard_id, "contested", None,
                      top_w, disagree_w, total_voters, now)

    # Not enough independent agreement yet (and no real dispute).
    if top_voters < MIN_AGREE_VOTERS:
        return _store(db, shard_id, "insufficient", None,
                      top_w, disagree_w, total_voters, now)

    # Clear, address-matching majority -> available (a lone dissenter here is
    # simply outvoted / overridden).
    return _store(db, shard_id, "available", top_hash,
                  top_w, disagree_w, total_voters, now)


def _store(db, shard_id, status, agreed_hash, agree_w, disagree_w, voters, now):
    db.execute(
        "INSERT INTO shard_consensus(shard_id,status,agreed_hash,agree_weight,"
        "disagree_weight,voters,updated_at) VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(shard_id) DO UPDATE SET status=excluded.status, "
        "agreed_hash=excluded.agreed_hash, agree_weight=excluded.agree_weight, "
        "disagree_weight=excluded.disagree_weight, voters=excluded.voters, "
        "updated_at=excluded.updated_at",
        (shard_id, status, agreed_hash, agree_w, disagree_w, voters, now),
    )
    db.commit()
    return {"shard_id": shard_id, "status": status, "agreed_hash": agreed_hash,
            "agree_weight": round(agree_w, 4), "disagree_weight": round(disagree_w, 4),
            "voters": voters}


def mod_is_downloadable(db, mod_id: str) -> tuple[bool, list[dict]]:
    """A mod is downloadable only if EVERY shard reaches 'available'."""
    shards = db.execute("SELECT id FROM shards WHERE mod_id=? ORDER BY seq",
                        (mod_id,)).fetchall()
    statuses = []
    ok = bool(shards)
    for s in shards:
        c = db.execute("SELECT status FROM shard_consensus WHERE shard_id=?",
                       (s["id"],)).fetchone()
        st = c["status"] if c else "insufficient"
        statuses.append({"shard_id": s["id"], "status": st})
        if st != "available":
            ok = False
    return ok, statuses
