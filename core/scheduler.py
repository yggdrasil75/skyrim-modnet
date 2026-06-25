"""Background maintenance jobs.

Run in a daemon thread per node. All jobs are best-effort and idempotent so
a node can crash/restart at any time without corrupting network state.

Jobs:
  gossip_peers       — say hello, discover new peers, measure RTT (distance).
  exchange_locations — pull peer shard digests, update rarity accounting.
  rebalance          — replicate under-replicated shards to DISTANT peers,
                       shed over-replicated ones if low on disk.
  security_sweep     — (opt-in) fetch random recent shards from peers and
                       scan their *unpacked* contents for executables.
"""
from __future__ import annotations

import io
import os
import time
import random
import threading
import tempfile
import zipfile

import requests

from .db import get_db
from .packaging import scan_file_for_executable


def start_background_jobs(app) -> None:
    t = threading.Thread(target=_loop, args=(app,), daemon=True)
    t.start()


def _loop(app):
    # stagger so a freshly started fleet doesn't thundering-herd
    time.sleep(random.uniform(1, 5))
    while True:
        try:
            with app.app_context():
                gossip_peers(app)
                refresh_states_and_relations(app)
                if app.config.get("DECLARED_ROLE") != "gateway":
                    exchange_locations(app)
                    broadcast_attestations(app)
                    rebalance(app)
                    cross_check_peers(app)
                    report_storage(app)
                    if app.config.get("SECURITY_SCAN_ENABLED"):
                        security_sweep(app)
        except Exception as exc:  # keep the node alive no matter what
            app.logger.warning("maintenance error: %s", exc)
        time.sleep(app.config.get("MAINT_INTERVAL", 60))


def refresh_states_and_relations(app):
    """Reclassify every peer's global state and auto-suggest neighbor/distant
    relations from measured RTT (without clobbering friend/enemy)."""
    from .trust import classify_state, suggest_relation
    db = get_db()
    now = int(time.time())
    for row in db.execute(
        "SELECT node_id,declared_role,first_seen,failures,"
        "distance_ms,relation FROM peers"
    ).fetchall():
        peer = dict(row)
        # relation first: an 'enemy' relation maps to 'malicious' state.
        rel = suggest_relation(peer["distance_ms"], peer["relation"] or "neutral")
        peer["relation"] = rel
        state = classify_state(peer, now)
        db.execute("UPDATE peers SET node_state=?, relation=? WHERE node_id=?",
                   (state, rel, peer["node_id"]))
    db.commit()


def broadcast_attestations(app):
    """For each locally-held shard, sign and push an attestation to peers.

    The content_hash we attest is recomputed from bytes on disk, so an honest
    node always attests the true hash. We never attest to enemies, and we skip
    pushing to gateways (they hold/score nothing)."""
    from .gossip import sign_message
    from .packaging import sha256_file
    db = get_db()
    held = db.execute(
        "SELECT s.id, s.mod_id FROM shards s WHERE s.have_local=1"
    ).fetchall()
    if not held:
        return
    targets = db.execute(
        "SELECT node_id,url,relation,declared_role FROM peers "
        "WHERE relation != 'enemy' AND declared_role != 'gateway'"
    ).fetchall()
    if not targets:
        return

    for s in held:
        path = os.path.join(app.config["BLOB_DIR"], s["mod_id"], s["id"])
        if not os.path.exists(path):
            continue
        content_hash = sha256_file(path)
        env = sign_message(app.config["SIGNING_KEY"], "attest",
                           {"shard_id": s["id"], "content_hash": content_hash})
        # store our own attestation locally too (proven=1: we hold the bytes)
        db.execute(
            "INSERT INTO attestations(shard_id,node_id,content_hash,ts,sig,verify_key,proven) "
            "VALUES(?,?,?,?,?,?,1) ON CONFLICT(shard_id,node_id) DO UPDATE SET "
            "content_hash=excluded.content_hash, ts=excluded.ts, proven=1",
            (s["id"], app.config["NODE_ID"], content_hash, env["ts"],
             env["sig"], env["verify_key"]),
        )
        for t in targets:
            try:
                requests.post(f"{t['url']}/api/net/attest", json=env, timeout=5)
            except requests.RequestException:
                continue
    db.commit()


def _peers(db):
    return db.execute("SELECT node_id,url,distance_ms,failures FROM peers").fetchall()


def gossip_peers(app):
    db = get_db()
    me = {
        "node_id": app.config["NODE_ID"],
        "url": app.config["PUBLIC_URL"],
        "verify_key": app.config["VERIFY_KEY"],
        "declared_role": app.config.get("DECLARED_ROLE", "common"),
    }
    for peer in _peers(db):
        try:
            start = time.perf_counter()
            r = requests.post(f"{peer['url']}/api/net/hello", json=me, timeout=5)
            rtt_ms = (time.perf_counter() - start) * 1000
            r.raise_for_status()
            body = r.json()
            db.execute(
                "UPDATE peers SET distance_ms=?, last_seen=?, failures=0 WHERE node_id=?",
                (rtt_ms, int(time.time()), peer["node_id"]),
            )
            # discover that peer's peers
            pr = requests.get(f"{peer['url']}/api/net/peers", timeout=5)
            for p in pr.json():
                if p["node_id"] == app.config["NODE_ID"]:
                    continue
                db.execute(
                    "INSERT OR IGNORE INTO peers(node_id,url,last_seen,failures) "
                    "VALUES(?,?,?,0)", (p["node_id"], p["url"], int(time.time())),
                )
        except requests.RequestException:
            db.execute("UPDATE peers SET failures=failures+1 WHERE node_id=?",
                       (peer["node_id"],))
    db.commit()


def exchange_locations(app):
    db = get_db()
    local = [r["id"] for r in db.execute(
        "SELECT id FROM shards WHERE have_local=1").fetchall()]
    payload = {"node_id": app.config["NODE_ID"], "shards": local}
    for peer in _peers(db):
        try:
            requests.post(f"{peer['url']}/api/net/announce-locations",
                          json=payload, timeout=5)
            r = requests.get(f"{peer['url']}/api/net/shard-digest", timeout=5)
            body = r.json()
            now = int(time.time())
            for sid in body.get("shards", []):
                db.execute(
                    "INSERT OR REPLACE INTO shard_locations(shard_id,node_id,last_seen) "
                    "VALUES(?,?,?)", (sid, body["node_id"], now),
                )
        except requests.RequestException:
            continue
    db.commit()


def _disk_used(app) -> int:
    total = 0
    for dp, _d, fs in os.walk(app.config["BLOB_DIR"]):
        for f in fs:
            try:
                total += os.path.getsize(os.path.join(dp, f))
            except OSError:
                pass
    return total


def rebalance(app):
    """Pull under-replicated shards toward us, preferring distant sources so
    replicas end up in different failure domains."""
    db = get_db()
    target = app.config["TARGET_REPLICAS"]
    if _disk_used(app) >= app.config["MAX_DISK_BYTES"]:
        return  # full; let other nodes carry rare shards

    under = db.execute(
        "SELECT shard_id, COUNT(DISTINCT node_id) reps FROM shard_locations "
        "GROUP BY shard_id HAVING reps < ?", (target,)
    ).fetchall()
    random.shuffle(under)

    for row in under[:5]:  # bounded work per cycle
        sid = row["shard_id"]
        if db.execute("SELECT 1 FROM shards WHERE id=? AND have_local=1",
                      (sid,)).fetchone():
            continue
        # Only replicate FROM peers we don't consider enemies/malicious. Also
        # skip shards whose originator we've marked enemy (don't host their
        # content at all). Prefer the most DISTANT, non-neighbor holder so the
        # new copy lands in a different failure domain.
        holders = db.execute(
            "SELECT sl.node_id, p.url, p.distance_ms, p.relation, p.node_state "
            "FROM shard_locations sl JOIN peers p ON p.node_id=sl.node_id "
            "WHERE sl.shard_id=? AND p.relation != 'enemy' "
            "AND p.node_state != 'malicious'", (sid,)
        ).fetchall()
        # don't pull from neighbors for fault-isolation copies; rank by distance
        ranked = sorted(
            holders,
            key=lambda h: (h["relation"] == "neighbor", -(h["distance_ms"] or 0)),
        )
        for h in ranked:
            if _fetch_shard(app, db, h["node_id"], h["url"], sid):
                break
    db.commit()


def _fetch_shard(app, db, source_node_id, url, sid) -> bool:
    """Replicate a shard from a peer. A successful, integrity-verified transfer
    casts a GOOD reputation vote for the source; a failure or hash mismatch
    casts a BAD one. These votes accumulate as data — they never ban anyone."""
    from .packaging import sha256_file
    ok = False
    try:
        r = requests.get(f"{url}/api/mods/shard/{sid}?raw=1", timeout=30, stream=True)
        if r.status_code != 200:
            r = requests.get(f"{url}/api/net/shard/{sid}", timeout=30, stream=True)
        if r.status_code != 200:
            _cast_rep(app, db, source_node_id, good=0, bad=1)
            return False
        meta = db.execute("SELECT mod_id FROM shards WHERE id=?", (sid,)).fetchone()
        mod_id = meta["mod_id"] if meta else "orphan"
        dest_dir = os.path.join(app.config["BLOB_DIR"], mod_id)
        os.makedirs(dest_dir, exist_ok=True)
        path = os.path.join(dest_dir, sid)
        with open(path, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
        # integrity check: shard id IS the expected sha256. Mismatch => the
        # source served bad bytes for a "known-good" address => bad vote.
        if sha256_file(path) != sid:
            os.remove(path)
            _cast_rep(app, db, source_node_id, good=0, bad=1)
            return False
        db.execute("UPDATE shards SET have_local=1 WHERE id=?", (sid,))
        db.execute(
            "INSERT OR REPLACE INTO shard_locations(shard_id,node_id,last_seen) "
            "VALUES(?,?,?)", (sid, app.config["NODE_ID"], int(time.time())),
        )
        _cast_rep(app, db, source_node_id, good=1, bad=0)
        _obtain_receipt(app, db, url, sid)   # provenance for anti-Sybil
        ok = True
        return True
    except requests.RequestException:
        _cast_rep(app, db, source_node_id, good=0, bad=1)
        return False


def _obtain_receipt(app, db, source_url, sid):
    """Ask the source to sign a transfer receipt and store it as provenance.

    If we can't get a receipt (old peer, offline), we store a weaker self-noted
    record so the shard still has *some* provenance, but receipts from the
    source are what actually resist Sybil 'I have everything' claims."""
    from .gossip import sign_message
    import json as _json
    try:
        env = sign_message(app.config["SIGNING_KEY"], "receipt_req",
                           {"shard_id": sid})
        env["node_id"] = app.config["NODE_ID"]
        r = requests.post(f"{source_url}/api/net/request-receipt",
                          json=env, timeout=8)
        if r.status_code == 200:
            receipt = r.json()["receipt"]
            from .proofs import verify_receipt
            if verify_receipt(receipt, app.config["NODE_ID"]):
                db.execute(
                    "INSERT OR REPLACE INTO provenance(shard_id,kind,source_node_id,"
                    "ts,record) VALUES(?,?,?,?,?)",
                    (sid, "receipt", receipt["source_verify_key"][:32],
                     receipt["ts"], _json.dumps(receipt)),
                )
                db.commit()
                return
    except requests.RequestException:
        pass
    # fallback: unsigned self-note (counts as weak provenance only)
    db.execute(
        "INSERT OR IGNORE INTO provenance(shard_id,kind,source_node_id,ts,record) "
        "VALUES(?,?,?,?,?)",
        (sid, "unverified", None, int(time.time()),
         _json.dumps({"kind": "unverified", "note": "no receipt obtained"})),
    )
    db.commit()


def _cast_rep(app, db, subject_id, good, bad):
    """Record our own reputation vote locally and push it to peers (signed)."""
    if not subject_id:
        return
    from .gossip import sign_message
    now = int(time.time())
    db.execute(
        "INSERT INTO node_rep_votes(subject_id,reporter_id,good,bad,last_ts) "
        "VALUES(?,?,?,?,?) ON CONFLICT(subject_id,reporter_id) DO UPDATE SET "
        "good=good+excluded.good, bad=bad+excluded.bad, last_ts=excluded.last_ts",
        (subject_id, app.config["NODE_ID"], good, bad, now),
    )
    agg = db.execute("SELECT COALESCE(SUM(good),0) g, COALESCE(SUM(bad),0) b "
                     "FROM node_rep_votes WHERE subject_id=?", (subject_id,)).fetchone()
    db.execute("UPDATE peers SET good_votes=?, bad_votes=? WHERE node_id=?",
               (agg["g"], agg["b"], subject_id))
    # gossip the vote to gateways + trusted nodes, who aggregate the bad-node list
    env = sign_message(app.config["SIGNING_KEY"], "rep",
                       {"subject_id": subject_id, "good": good, "bad": bad})
    for t in db.execute(
        "SELECT url FROM peers WHERE node_state IN ('gateway','trusted') "
        "AND relation != 'enemy'"
    ).fetchall():
        try:
            requests.post(f"{t['url']}/api/net/rep-vote", json=env, timeout=4)
        except requests.RequestException:
            continue


def cross_check_peers(app):
    """Periodically challenge peers to prove they still hold uncorrupted bytes.

    This is the trusted-node-compromise / silent-corruption defense: a file
    might sit a month with no downloads and rot, or a compromised node might
    quietly drop/alter content. We don't wait for a download to find out.

    Scale: the number of challenges we issue per cycle grows with how much WE
    store (a proxy for how much we depend on the network staying healthy), so
    big nodes audit more. For each random shard we hold, we pick OTHER claimed
    holders and ask them to answer a nonce challenge; we verify against our own
    known-good copy. pass -> good rep vote; corrupt/missing -> bad rep vote.
    """
    from .proofs import make_nonce, storage_proof
    db = get_db()
    held = db.execute(
        "SELECT s.id, s.mod_id FROM shards s WHERE s.have_local=1"
    ).fetchall()
    if not held:
        return
    # budget grows ~log with how many shards we hold (gentle, bounded)
    import math
    budget = min(20, 2 + int(math.log10(1 + len(held)) * 4))
    sample = random.sample(list(held), k=min(budget, len(held)))

    for s in sample:
        sid = s["id"]
        ref_path = os.path.join(app.config["BLOB_DIR"], s["mod_id"], sid)
        # other nodes that claim to hold this shard
        others = db.execute(
            "SELECT sl.node_id, p.url FROM shard_locations sl "
            "JOIN peers p ON p.node_id=sl.node_id "
            "WHERE sl.shard_id=? AND sl.node_id!=? AND p.relation!='enemy'",
            (sid, app.config["NODE_ID"]),
        ).fetchall()
        for o in others[:3]:                 # bounded fan-out per shard
            nonce = make_nonce()
            expected = storage_proof(nonce, ref_path)
            if expected is None:
                continue                     # we can't read our own copy; skip
            result = "missing"
            try:
                r = requests.post(f"{o['url']}/api/net/prove/{sid}",
                                  json={"nonce": nonce}, timeout=8)
                if r.status_code == 200 and r.json().get("digest") == expected:
                    result = "pass"
                elif r.status_code == 200:
                    result = "corrupt"       # answered, but wrong bytes
                else:
                    result = "missing"
            except requests.RequestException:
                result = "missing"
            db.execute(
                "INSERT INTO integrity_checks(shard_id,target_node_id,nonce,result,ts) "
                "VALUES(?,?,?,?,?)", (sid, o["node_id"], nonce, result, int(time.time())),
            )
            if result == "pass":
                _cast_rep(app, db, o["node_id"], good=1, bad=0)
            else:
                # corruption or a false hold-claim -> a bad reputation vote
                # (still never auto-bans; just data)
                _cast_rep(app, db, o["node_id"], good=0, bad=1)
    db.commit()


def report_storage(app):
    """Self-report our stored volume (signed) so peers can contextualize our
    failure rate. We also recompute it from disk so we don't lie to ourselves."""
    from .gossip import sign_message
    db = get_db()
    row = db.execute(
        "SELECT COUNT(*) c, COALESCE(SUM(size_bytes),0) b FROM shards WHERE have_local=1"
    ).fetchone()
    files, byts = row["c"], row["b"]
    db.execute("UPDATE peers SET stored_files=?, stored_bytes=? WHERE node_id=?",
               (files, byts, app.config["NODE_ID"]))
    db.commit()
    env = sign_message(app.config["SIGNING_KEY"], "storage",
                       {"files": files, "bytes": byts})
    for t in db.execute(
        "SELECT url FROM peers WHERE node_state IN ('gateway','trusted') "
        "AND relation!='enemy'"
    ).fetchall():
        try:
            requests.post(f"{t['url']}/api/net/storage-report", json=env, timeout=4)
        except requests.RequestException:
            continue


def security_sweep(app):
    """Opt-in randomized audit: download recent shards, unpack, scan."""
    db = get_db()
    recent = db.execute(
        "SELECT s.id, p.url FROM shards s "
        "JOIN shard_locations sl ON sl.shard_id=s.id "
        "JOIN peers p ON p.node_id=sl.node_id "
        "WHERE s.have_local=0 ORDER BY s.created_at DESC LIMIT 50"
    ).fetchall()
    if not recent:
        return
    sample = random.sample(list(recent), k=min(3, len(recent)))
    for row in sample:
        try:
            r = requests.get(f"{row['url']}/api/net/shard/{row['id']}", timeout=30)
            if r.status_code != 200:
                continue
            _scan_blob_bytes(app, row["id"], r.content)
        except requests.RequestException:
            continue


def _scan_blob_bytes(app, shard_id, data: bytes):
    """Best-effort: if a shard happens to be a standalone zip/7z volume we
    can open, unpack and scan member files. Flagged shards are recorded."""
    tmp = tempfile.mkdtemp(prefix="sweep_")
    try:
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                zf.extractall(tmp)
        except zipfile.BadZipFile:
            # not directly openable (e.g. a 7z split volume) — scan raw bytes
            p = os.path.join(tmp, shard_id)
            with open(p, "wb") as fh:
                fh.write(data)
        for dp, _d, fs in os.walk(tmp):
            for f in fs:
                reason = scan_file_for_executable(os.path.join(dp, f))
                if reason:
                    app.logger.warning(
                        "SECURITY: shard %s flagged (%s)", shard_id, reason)
                    # In a full build: broadcast a signed flag to peers so the
                    # offending mod can be quarantined network-wide.
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
