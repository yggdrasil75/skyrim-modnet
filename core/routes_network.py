"""Inter-node networking.

Nodes form an unstructured overlay. Each node:
  - announces itself to known peers (/api/net/hello)
  - exchanges shard-location digests so the network can compute, for every
    shard, how many distinct nodes hold it (rarity) and how far apart they
    are (fault-isolation).
  - measures RTT to peers as a cheap proxy for "network distance", used by
    the rebalancer to push replicas to *distant* nodes rather than nearby
    ones that likely share a failure domain.

There is no master list and no special node. Bootstrapping is via any seed
URL the operator supplies; from there gossip discovers the rest.
"""
from __future__ import annotations

import os
import time

import requests
from flask import Blueprint, request, jsonify, current_app

from .db import get_db

bp = Blueprint("network", __name__, url_prefix="/api/net")


@bp.route("/hello", methods=["POST"])
def hello():
    """A peer introduces itself; we record it and reply with our identity.

    We bind node_id -> verify_key on first contact. If a later hello presents
    a *different* verify_key for an existing node_id, we reject it: that's
    either a key rotation (must come as a signed succession, not handled here)
    or an impersonation attempt.
    """
    data = request.get_json(force=True)
    node_id = data["node_id"]
    url = data["url"]
    vk = data.get("verify_key")
    declared_role = data.get("declared_role", "common")
    if declared_role not in ("common", "gateway"):
        declared_role = "common"

    db = get_db()
    now = int(time.time())
    existing = db.execute(
        "SELECT verify_key FROM peers WHERE node_id=?", (node_id,)
    ).fetchone()
    if existing and existing["verify_key"] and vk and existing["verify_key"] != vk:
        return jsonify(error="verify_key_mismatch_for_known_node"), 409

    if existing:
        db.execute(
            "UPDATE peers SET url=?, verify_key=COALESCE(verify_key,?), "
            "last_seen=?, failures=0, declared_role=? WHERE node_id=?",
            (url, vk, now, declared_role, node_id),
        )
    else:
        db.execute(
            "INSERT INTO peers(node_id,url,verify_key,last_seen,first_seen,"
            "failures,declared_role,node_state,relation) "
            "VALUES(?,?,?,?,?,0,?, 'newcomer','neutral')",
            (node_id, url, vk, now, now, declared_role),
        )
    db.commit()
    return jsonify(
        node_id=current_app.config["NODE_ID"],
        url=current_app.config["PUBLIC_URL"],
        verify_key=current_app.config["VERIFY_KEY"],
        declared_role=current_app.config.get("DECLARED_ROLE", "common"),
        ts=now,  # lets the caller measure RTT
    )


@bp.route("/peers", methods=["GET"])
def list_peers():
    db = get_db()
    rows = db.execute(
        "SELECT node_id,url,distance_ms,last_seen,failures,node_state,"
        "relation,first_seen,bad_file_count,declared_role FROM peers "
        "ORDER BY distance_ms IS NULL, distance_ms"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@bp.route("/shard-digest", methods=["GET"])
def shard_digest():
    """Report which shards this node holds, so peers can track replication."""
    db = get_db()
    rows = db.execute(
        "SELECT id FROM shards WHERE have_local=1"
    ).fetchall()
    return jsonify(node_id=current_app.config["NODE_ID"],
                   shards=[r["id"] for r in rows])


@bp.route("/announce-locations", methods=["POST"])
def announce_locations():
    """A peer tells us where shards live; we merge into our location table."""
    data = request.get_json(force=True)
    node_id = data["node_id"]
    shard_ids = data.get("shards", [])
    db = get_db()
    now = int(time.time())
    for sid in shard_ids:
        db.execute(
            "INSERT OR REPLACE INTO shard_locations(shard_id,node_id,last_seen) "
            "VALUES(?,?,?)", (sid, node_id, now),
        )
    db.commit()
    return jsonify(ok=True, merged=len(shard_ids))


@bp.route("/rarity", methods=["GET"])
def rarity():
    """Replication factor per shard: how rare/common each file is.

    Drives two safety goals at once:
      - rare shards (replicas < target) get pushed out before they vanish.
      - over-replicated shards can be shed so one file can't hog the network.
    """
    db = get_db()
    target = current_app.config["TARGET_REPLICAS"]
    rows = db.execute(
        "SELECT shard_id, COUNT(DISTINCT node_id) reps "
        "FROM shard_locations GROUP BY shard_id"
    ).fetchall()
    rare, over = [], []
    for r in rows:
        if r["reps"] < target:
            rare.append({"shard_id": r["shard_id"], "reps": r["reps"]})
        elif r["reps"] > target * 2:
            over.append({"shard_id": r["shard_id"], "reps": r["reps"]})
    return jsonify(target=target, under_replicated=rare, over_replicated=over)


# ---------------------------------------------------------------------------
# Signed gossip endpoints: attestations, votes, moderation, relations.
# All accept a signed envelope (see core/gossip.py). We verify the signature
# AND bind it to the node_id's known verify_key before trusting anything.
# ---------------------------------------------------------------------------

from .gossip import verify_message, derive_node_id          # noqa: E402
from .voting import resolve_shard                            # noqa: E402


def _verify_envelope(db, env: dict) -> tuple[bool, str, str]:
    """Verify a signed envelope and return (ok, reason, node_id).

    node_id is derived from the verify_key, so a signature implicitly proves
    node identity. If we already know this node under a different key, reject.
    """
    vk = env.get("verify_key")
    if not vk:
        return False, "no_verify_key", ""
    node_id = derive_node_id(vk)
    known = db.execute(
        "SELECT verify_key FROM peers WHERE node_id=?", (node_id,)
    ).fetchone()
    expected = known["verify_key"] if known and known["verify_key"] else None
    ok, reason = verify_message(env, expected_verify_key=expected)
    return ok, reason, node_id


@bp.route("/attest", methods=["POST"])
def receive_attestation():
    """Accept a signed attestation: 'I (node) hold shard S; my bytes hash H'.

    payload: {shard_id, content_hash}

    Anti-Sybil: a signature proves identity but NOT possession. So before this
    attestation counts toward consensus, we challenge the attester with a
    proof-of-storage nonce. If we hold a reference copy (or can reach one) we
    verify the answer; only a verified attester is marked `proven=1` and
    weighted in voting. This stops a node from cheaply claiming 'I hold every
    file' — it must actually answer for the bytes.
    """
    from .proofs import make_nonce, storage_proof, expected_proof
    env = request.get_json(force=True)
    db = get_db()
    ok, reason, node_id = _verify_envelope(db, env)
    if not ok:
        return jsonify(error="unverified", reason=reason), 403
    p = env["p"]
    shard_id = p["shard_id"]
    content_hash = p["content_hash"]

    # Determine if we can verify possession. Reference copy = our own bytes, if
    # we hold them. (If we don't, we accept as unproven; another node that does
    # hold a copy will verify when it receives the same attestation.)
    proven = 0
    ref = db.execute("SELECT mod_id FROM shards WHERE id=? AND have_local=1",
                     (shard_id,)).fetchone()
    peer = db.execute("SELECT url FROM peers WHERE node_id=?", (node_id,)).fetchone()
    if ref and peer:
        ref_path = os.path.join(current_app.config["BLOB_DIR"],
                                ref["mod_id"], shard_id)
        nonce = make_nonce()
        try:
            r = requests.post(f"{peer['url']}/api/net/prove/{shard_id}",
                              json={"nonce": nonce}, timeout=8)
            if (r.status_code == 200
                    and r.json().get("digest") == expected_proof(nonce, ref_path)):
                proven = 1
        except requests.RequestException:
            proven = 0

    db.execute(
        "INSERT INTO attestations(shard_id,node_id,content_hash,ts,sig,verify_key,proven) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(shard_id,node_id) DO UPDATE SET "
        "content_hash=excluded.content_hash, ts=excluded.ts, sig=excluded.sig, "
        "proven=excluded.proven",
        (shard_id, node_id, content_hash, env["ts"], env["sig"],
         env["verify_key"], proven),
    )
    db.execute(
        "INSERT OR REPLACE INTO shard_locations(shard_id,node_id,last_seen) "
        "VALUES(?,?,?)", (shard_id, node_id, int(time.time())),
    )
    db.commit()
    result = resolve_shard(db, shard_id)
    return jsonify(ok=True, proven=bool(proven), consensus=result)


@bp.route("/consensus/<shard_id>", methods=["GET"])
def consensus(shard_id):
    db = get_db()
    row = db.execute("SELECT * FROM shard_consensus WHERE shard_id=?",
                     (shard_id,)).fetchone()
    if not row:
        # compute on demand
        return jsonify(resolve_shard(db, shard_id))
    return jsonify(dict(row))


@bp.route("/flag", methods=["POST"])
def receive_flag():
    """Accept a signed moderation flag (e.g. executable found in a shard).

    payload: {target_type, target_id, reason}

    IMPORTANT: a flag NEVER bans anyone. If it concerns a file, it is recorded
    as a security verdict feeding the 5% warning threshold. If it implicates a
    node, it adds ONE bad reputation vote (deduped per reporter->subject pair).
    Reputation is surfaced to users; only users mark enemies.
    """
    import hashlib
    env = request.get_json(force=True)
    db = get_db()
    ok, reason, node_id = _verify_envelope(db, env)
    if not ok:
        return jsonify(error="unverified", reason=reason), 403
    p = env["p"]
    fid = hashlib.sha256(
        f"{p['target_type']}|{p['target_id']}|{p['reason']}|{node_id}".encode()
    ).hexdigest()
    db.execute(
        "INSERT OR IGNORE INTO moderation_flags(id,target_type,target_id,reason,"
        "node_id,ts,sig,verify_key) VALUES(?,?,?,?,?,?,?,?)",
        (fid, p["target_type"], p["target_id"], p["reason"], node_id,
         env["ts"], env["sig"], env["verify_key"]),
    )
    now = int(time.time())

    if p["target_type"] == "mod":
        # record a security verdict for the file (feeds 5% warning threshold)
        db.execute(
            "INSERT OR REPLACE INTO file_security_votes(mod_id,reporter_id,"
            "verdict,ts,sig,verify_key) VALUES(?,?,?,?,?,?)",
            (p["target_id"], node_id, "malicious", now, env["sig"], env["verify_key"]),
        )
        # the node that ORIGINATED the file gets ONE bad reputation vote from
        # this reporter — accumulated, never auto-acted-upon.
        origin = db.execute(
            "SELECT node_id FROM shard_locations sl JOIN shards s "
            "ON s.id=sl.shard_id WHERE s.mod_id=? LIMIT 1", (p["target_id"],)
        ).fetchone()
        if origin:
            _record_rep_vote(db, subject=origin["node_id"], reporter=node_id,
                             good=0, bad=1, now=now,
                             sig=env["sig"], vk=env["verify_key"])
    elif p["target_type"] == "node":
        _record_rep_vote(db, subject=p["target_id"], reporter=node_id,
                         good=0, bad=1, now=now,
                         sig=env["sig"], vk=env["verify_key"])
    db.commit()
    return jsonify(ok=True, flag_id=fid)


@bp.route("/relation/<node_id>", methods=["POST"])
def set_relation(node_id):
    """Operator sets THIS node's private relation to a peer.

    body: {relation: friend|neutral|distant|neighbor|enemy}
    This is a purely local opinion; it is never gossiped as fact.
    """
    from .trust import (REL_FRIEND, REL_NEUTRAL, REL_DISTANT,
                        REL_NEIGHBOR, REL_ENEMY)
    valid = {REL_FRIEND, REL_NEUTRAL, REL_DISTANT, REL_NEIGHBOR, REL_ENEMY}
    rel = request.get_json(force=True).get("relation")
    if rel not in valid:
        return jsonify(error="invalid_relation", valid=sorted(valid)), 400
    db = get_db()
    db.execute("UPDATE peers SET relation=? WHERE node_id=?", (rel, node_id))
    db.commit()
    return jsonify(ok=True, node_id=node_id, relation=rel)


# ---------------------------------------------------------------------------
# Proof-of-storage: prove you still hold uncorrupted bytes for a shard.
# ---------------------------------------------------------------------------

@bp.route("/prove/<shard_id>", methods=["POST"])
def prove_storage(shard_id):
    """Holder side. Body {nonce}. Return H(nonce || our_bytes).

    Anyone can ask. A correct answer proves we hold the exact, uncorrupted
    bytes right now (catches silent disk corruption and false hold-claims).
    """
    from .proofs import storage_proof
    nonce = request.get_json(force=True).get("nonce")
    if not nonce:
        return jsonify(error="no_nonce"), 400
    db = get_db()
    row = db.execute(
        "SELECT mod_id FROM shards WHERE id=? AND have_local=1", (shard_id,)
    ).fetchone()
    if not row:
        return jsonify(have=False), 404
    path = os.path.join(current_app.config["BLOB_DIR"], row["mod_id"], shard_id)
    digest = storage_proof(nonce, path)
    if digest is None:
        return jsonify(have=False, error="bytes_unreadable"), 410
    return jsonify(have=True, shard_id=shard_id, digest=digest)


@bp.route("/provenance/<shard_id>", methods=["GET"])
def get_provenance(shard_id):
    """Return our signed provenance for a shard we hold (origin or receipt).

    This is how we answer 'prove you legitimately have this, not just claim
    it'. A node with no provenance record for a shard it claims is suspect.
    """
    import json as _json
    db = get_db()
    row = db.execute("SELECT kind, source_node_id, ts, record FROM provenance "
                     "WHERE shard_id=?", (shard_id,)).fetchone()
    if not row:
        return jsonify(error="no_provenance"), 404
    return jsonify(shard_id=shard_id, kind=row["kind"],
                   source_node_id=row["source_node_id"], ts=row["ts"],
                   record=_json.loads(row["record"]))


@bp.route("/request-receipt", methods=["POST"])
def request_receipt():
    """A node that just downloaded a shard FROM us asks us to sign a receipt.

    Body (signed envelope) payload: {shard_id}. We only sign if we actually
    hold the shard (so we don't vouch for transfers we didn't make).
    """
    from .proofs import make_receipt
    env = request.get_json(force=True)
    db = get_db()
    ok, reason, recipient_id = _verify_envelope(db, env)
    if not ok:
        return jsonify(error="unverified", reason=reason), 403
    shard_id = env["p"]["shard_id"]
    have = db.execute("SELECT 1 FROM shards WHERE id=? AND have_local=1",
                      (shard_id,)).fetchone()
    if not have:
        return jsonify(error="we_dont_hold_it"), 404
    receipt = make_receipt(current_app.config["SIGNING_KEY"], shard_id,
                           recipient_id, int(time.time()))
    return jsonify(receipt=receipt)


# ---------------------------------------------------------------------------
# Reputation: good/bad votes accumulate but NEVER auto-ban. Surfaced to users.
# ---------------------------------------------------------------------------

def _record_rep_vote(db, subject, reporter, good, bad, now, sig=None, vk=None):
    """Upsert one reporter->subject reputation pair and refresh the subject's
    aggregate good/bad tallies on the peers row (for fast listing)."""
    db.execute(
        "INSERT INTO node_rep_votes(subject_id,reporter_id,good,bad,last_ts,sig,verify_key) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(subject_id,reporter_id) DO UPDATE SET "
        "good=good+excluded.good, bad=bad+excluded.bad, last_ts=excluded.last_ts",
        (subject, reporter, good, bad, now, sig, vk),
    )
    agg = db.execute(
        "SELECT COALESCE(SUM(good),0) g, COALESCE(SUM(bad),0) b "
        "FROM node_rep_votes WHERE subject_id=?", (subject,)
    ).fetchone()
    db.execute(
        "UPDATE peers SET good_votes=?, bad_votes=? WHERE node_id=?",
        (agg["g"], agg["b"], subject),
    )


@bp.route("/rep-vote", methods=["POST"])
def receive_rep_vote():
    """Signed node-reputation vote: 'subject succeeded/failed a transfer'.

    payload: {subject_id, good, bad}
    good = successful replication of a known-good file; bad = failed transfer.
    Accumulates only; never bans.
    """
    env = request.get_json(force=True)
    db = get_db()
    ok, reason, reporter = _verify_envelope(db, env)
    if not ok:
        return jsonify(error="unverified", reason=reason), 403
    p = env["p"]
    _record_rep_vote(db, subject=p["subject_id"], reporter=reporter,
                     good=int(p.get("good", 0)), bad=int(p.get("bad", 0)),
                     now=int(time.time()), sig=env["sig"], vk=env["verify_key"])
    db.commit()
    from .reputation import node_reputation
    row = db.execute("SELECT * FROM peers WHERE node_id=?",
                     (p["subject_id"],)).fetchone()
    return jsonify(ok=True, reputation=node_reputation(dict(row)) if row else None)


@bp.route("/storage-report", methods=["POST"])
def storage_report():
    """A node self-reports how much it stores, so consumers can contextualize
    its failure rate (big nodes fail more in absolute terms)."""
    env = request.get_json(force=True)
    db = get_db()
    ok, reason, node_id = _verify_envelope(db, env)
    if not ok:
        return jsonify(error="unverified", reason=reason), 403
    p = env["p"]
    db.execute("UPDATE peers SET stored_bytes=?, stored_files=? WHERE node_id=?",
               (float(p.get("bytes", 0)), int(p.get("files", 0)), node_id))
    db.commit()
    return jsonify(ok=True)


@bp.route("/suspect-nodes", methods=["GET"])
def suspect_nodes():
    """The advisory 'bad node list'. Informational only — users choose enemies.

    Query: ?min_samples=20  (avoid condemning nodes with tiny histories)
    Gateways and trusted nodes replicate this; others can one-time pull it as a
    default, subscribe for updates, or just browse it.
    """
    from .reputation import list_suspect_nodes
    db = get_db()
    ms = int(request.args.get("min_samples", 20))
    return jsonify(node_id=current_app.config["NODE_ID"],
                   suspects=list_suspect_nodes(db, min_samples=ms))


@bp.route("/file-security/<mod_id>", methods=["GET"])
def file_security(mod_id):
    """5% weighted-security-node warning status for a file (never removal)."""
    from .reputation import file_security_status
    return jsonify(file_security_status(get_db(), mod_id))


@bp.route("/subscribe-suspects", methods=["POST"])
def subscribe_suspects():
    """One-time pull OR merge of another node's suspect list into ours.

    body: {source_url, mode: 'merge'|'replace'}
    'merge' adds their bad votes to ours (building your own list from a default
    seed); 'replace' is a pure one-time snapshot. Either way YOU still pick
    enemies — subscribing never auto-marks anyone.
    """
    body = request.get_json(force=True)
    src = body["source_url"].rstrip("/")
    mode = body.get("mode", "merge")
    try:
        resp = requests.get(f"{src}/api/net/suspect-nodes", timeout=10)
        suspects = resp.json().get("suspects", [])
    except Exception as exc:  # noqa: BLE001
        return jsonify(error="fetch_failed", detail=str(exc)), 502

    db = get_db()
    now = int(time.time())
    src_id = resp.json().get("node_id", "subscription")
    merged = 0
    for s in suspects:
        # We import their aggregate as a single reporter ('the source node'),
        # so one subscription can't masquerade as many independent reporters.
        if mode == "replace":
            db.execute("DELETE FROM node_rep_votes WHERE subject_id=? AND reporter_id=?",
                       (s["node_id"], src_id))
        _record_rep_vote(db, subject=s["node_id"], reporter=src_id,
                         good=s.get("good_votes", 0), bad=s.get("bad_votes", 0),
                         now=now)
        merged += 1
    db.commit()
    return jsonify(ok=True, merged=merged, mode=mode, source=src_id)
